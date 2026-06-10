//! Core queue operations backing the JSON-RPC handlers.
//!
//! All mutations operate by `entry_id`. The legacy `subject_id`-keyed
//! mutations from the in-tree code were replaced as part of the v0.5
//! plugin extraction (see `docs/architecture/v0.5-protocol-specs.md` §2).

use std::path::{Path, PathBuf};

use animus_queue_protocol::{
    completion_status, status, QueueEntry, QueueLeaseResponse, QueueListResponse,
    QueueMutationResponse, QueueReleasePendingResponse, QueueReorderResponse, QueueStats,
};
use animus_subject_protocol::SubjectDispatch;
use anyhow::Result;
use chrono::Utc;

use crate::dispatch_queue_state::{
    DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
};
use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state, save_queue_state};

/// File-locked backend wrapping a single project root's queue state.
#[derive(Debug, Clone)]
pub struct QueueBackend {
    project_root: PathBuf,
}

/// Result of a single enqueue.
#[derive(Debug, Clone)]
pub struct EnqueueOutcome {
    /// `true` if a new entry was appended; `false` for an idempotent no-op.
    pub enqueued: bool,
    /// Stable entry id (existing or newly minted).
    pub entry_id: String,
    /// Subject id from the dispatch envelope.
    pub subject_id: String,
}

impl QueueBackend {
    /// Bind the backend to a project root. State / lock files live under
    /// `<project_root>/.animus/`.
    pub fn new(project_root: PathBuf) -> Self {
        Self { project_root }
    }

    /// Bound project root.
    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    // ============================================================
    // queue/enqueue
    // ============================================================

    /// Append a dispatch to the queue. Idempotent on duplicate (same
    /// subject key + same `workflow_ref`).
    pub fn enqueue(&self, dispatch: SubjectDispatch) -> Result<EnqueueOutcome> {
        let _lock = acquire_queue_lock(&self.project_root)?;
        let mut state = load_queue_state(&self.project_root)?.unwrap_or_default();
        let subject_id = dispatch.subject_key();

        // Idempotency: an existing pending/assigned/held entry for the same
        // (subject_key, workflow_ref) is a no-op. (Mirrors the in-tree
        // semantics; see queue_service.rs `enqueue_subject_dispatch_is_idempotent_for_same_task_pipeline`.)
        if let Some(existing) = state.entries.iter().find(|entry| {
            if entry.subject_id_ref() != subject_id {
                return false;
            }
            if entry.status == DispatchQueueEntryStatus::Unknown {
                return false;
            }
            if let Some(existing_dispatch) = entry.dispatch.as_ref() {
                existing_dispatch.workflow_ref == dispatch.workflow_ref
            } else {
                match (entry.task_id_ref(), dispatch.task_id()) {
                    (Some(existing_task), Some(incoming_task)) => existing_task == incoming_task,
                    _ => false,
                }
            }
        }) {
            return Ok(EnqueueOutcome {
                enqueued: false,
                entry_id: existing.entry_id.clone(),
                subject_id,
            });
        }

        let entry = DispatchQueueEntry::from_dispatch(dispatch);
        let entry_id = entry.entry_id.clone();
        state.entries.push(entry);
        save_queue_state(&self.project_root, &state)?;
        Ok(EnqueueOutcome {
            enqueued: true,
            entry_id,
            subject_id,
        })
    }

    // ============================================================
    // queue/list + queue/stats
    // ============================================================

    /// Paginated, filtered view of the queue + stats.
    pub fn list(
        &self,
        status_filter: &[String],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<QueueListResponse> {
        // Hold the queue lock across the read so a concurrent mutation
        // cannot race with legacy-state migration inside `load_queue_state`
        // (which mints stable ids for entries that lacked them, then writes
        // the migrated state back).
        let _lock = acquire_queue_lock(&self.project_root)?;
        let state = load_queue_state(&self.project_root)?.unwrap_or_default();
        let stats = stats_from_state(&state);

        let mut filtered: Vec<&DispatchQueueEntry> = state
            .entries
            .iter()
            .filter(|entry| {
                if status_filter.is_empty() {
                    return true;
                }
                let wire = entry.status.as_wire();
                status_filter.iter().any(|allowed| allowed == wire)
            })
            .collect();

        let total = filtered.len();
        let offset = offset.unwrap_or(0);
        if offset >= filtered.len() {
            filtered.clear();
        } else {
            filtered.drain(0..offset);
        }
        if let Some(limit) = limit {
            filtered.truncate(limit);
        }

        let entries: Vec<QueueEntry> = filtered.into_iter().filter_map(entry_to_protocol).collect();
        Ok(QueueListResponse {
            entries,
            total,
            stats,
        })
    }

    /// Aggregate counts.
    pub fn stats(&self) -> Result<QueueStats> {
        // Hold the lock for the same reason `list` does.
        let _lock = acquire_queue_lock(&self.project_root)?;
        let state = load_queue_state(&self.project_root)?.unwrap_or_default();
        Ok(stats_from_state(&state))
    }

    // ============================================================
    // queue/lease (NEW atomic dispatch path)
    // ============================================================

    /// Atomic dispatch: claim up to `max` pending entries, attach
    /// workflow ids, transition each Pending → Assigned, persist, and
    /// return the leased entries.
    ///
    /// If `workflow_ids` is `Some` its length MUST equal `max` (return
    /// [`QueueLeaseError::WorkflowIdCountMismatch`]). When `None`, synthetic
    /// UUIDs are generated.
    ///
    /// If `exclude_subjects` is `Some`, pending entries whose
    /// `subject_dispatch.subject_key()` matches any id in the list are
    /// skipped over without state transition. Daemons pass the set of
    /// subjects that already have in-flight workflows so the queue can
    /// advance past a head-of-line entry instead of returning it for
    /// immediate `queue/release_pending` back to Pending. Backward-
    /// compatible: `None` matches v0.2.0 behavior.
    pub fn lease(
        &self,
        max: usize,
        workflow_ids: Option<Vec<String>>,
        exclude_subjects: Option<Vec<String>>,
    ) -> std::result::Result<QueueLeaseResponse, QueueLeaseError> {
        if let Some(ids) = workflow_ids.as_ref() {
            if ids.len() != max {
                return Err(QueueLeaseError::WorkflowIdCountMismatch {
                    expected: max,
                    actual: ids.len(),
                });
            }
        }
        if max == 0 {
            return Ok(QueueLeaseResponse { leased: Vec::new() });
        }

        let _lock = acquire_queue_lock(&self.project_root).map_err(QueueLeaseError::Backend)?;
        let mut state = load_queue_state(&self.project_root)
            .map_err(QueueLeaseError::Backend)?
            .unwrap_or_default();

        let now_rfc3339 = Utc::now().to_rfc3339();
        let mut leased: Vec<QueueEntry> = Vec::new();
        let mut assigned_index = 0usize;
        let mut exclude_set: Option<std::collections::HashSet<String>> =
            exclude_subjects.map(|ids| ids.into_iter().collect());

        // FIFO within Pending — first-eligible-wins, in current order.
        for entry in state.entries.iter_mut() {
            if leased.len() == max {
                break;
            }
            if entry.status != DispatchQueueEntryStatus::Pending {
                continue;
            }
            // Corrupt legacy state — an entry with no dispatch envelope can't
            // be returned over the wire (`QueueEntry.subject_dispatch` is
            // required). Skip it instead of poisoning the lease.
            if entry.dispatch.is_none() {
                tracing::warn!(
                    entry_id = %entry.entry_id,
                    "queue/lease: skipping pending entry with no SubjectDispatch envelope"
                );
                continue;
            }
            if let Some(set) = exclude_set.as_mut() {
                // Prefer the dispatch's canonical subject_key (matches the
                // host's active-subject tracking); fall back to the stored
                // subject_id for entries that migrated without a dispatch.
                let key_owned = entry
                    .dispatch
                    .as_ref()
                    .map(|d| d.subject_key())
                    .unwrap_or_else(|| entry.subject_id_ref().to_string());
                if set.contains(&key_owned) {
                    continue;
                }
                // Leasing this entry makes its subject in-flight for the rest
                // of the batch — otherwise two pending entries for the same
                // subject can be leased together, defeating the exclusivity
                // the caller asked for via `exclude_subjects`.
                set.insert(key_owned);
            }
            let workflow_id = match workflow_ids.as_ref() {
                Some(ids) => ids[assigned_index].clone(),
                None => uuid::Uuid::new_v4().to_string(),
            };
            assigned_index += 1;

            entry.status = DispatchQueueEntryStatus::Assigned;
            entry.workflow_id = Some(workflow_id);
            entry.assigned_at = Some(now_rfc3339.clone());
            if let Some(protocol_entry) = entry_to_protocol(entry) {
                leased.push(protocol_entry);
            }
        }

        if !leased.is_empty() {
            save_queue_state(&self.project_root, &state).map_err(QueueLeaseError::Backend)?;
        }
        Ok(QueueLeaseResponse { leased })
    }

    // ============================================================
    // queue/hold + queue/release + queue/drop
    // ============================================================

    /// Hold a Pending entry. Idempotent on already-held.
    pub fn hold(&self, entry_id: &str) -> Result<QueueMutationResponse> {
        self.mutate_entry(entry_id, |entry| {
            match entry.status {
                DispatchQueueEntryStatus::Held => Ok(false), // idempotent no-op
                DispatchQueueEntryStatus::Pending => {
                    entry.status = DispatchQueueEntryStatus::Held;
                    entry.held_at = Some(Utc::now().to_rfc3339());
                    Ok(true)
                }
                DispatchQueueEntryStatus::Assigned => Err(MutationError::NotPending),
                DispatchQueueEntryStatus::Unknown => Err(MutationError::NotPending),
            }
        })
    }

    /// Release a Held entry back to Pending. Idempotent on already-pending.
    pub fn release(&self, entry_id: &str) -> Result<QueueMutationResponse> {
        self.mutate_entry(entry_id, |entry| match entry.status {
            DispatchQueueEntryStatus::Pending => Ok(false),
            DispatchQueueEntryStatus::Held => {
                entry.status = DispatchQueueEntryStatus::Pending;
                entry.held_at = None;
                Ok(true)
            }
            DispatchQueueEntryStatus::Assigned => Err(MutationError::NotPending),
            DispatchQueueEntryStatus::Unknown => Err(MutationError::NotPending),
        })
    }

    /// Drop an entry from the queue. Returns `not_found = true` when the
    /// entry does not exist; otherwise returns `changed = true`.
    pub fn drop_entry(&self, entry_id: &str) -> Result<QueueMutationResponse> {
        let _lock = acquire_queue_lock(&self.project_root)?;
        let Some(mut state) = load_queue_state(&self.project_root)? else {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };
        let before = state.entries.len();
        state.entries.retain(|entry| entry.entry_id != entry_id);
        let removed = before.saturating_sub(state.entries.len());
        if removed == 0 {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        }
        save_queue_state(&self.project_root, &state)?;
        Ok(QueueMutationResponse {
            changed: true,
            not_found: false,
        })
    }

    // ============================================================
    // queue/reorder
    // ============================================================

    /// Reorder entries by `entry_id`. Entries not named keep their existing
    /// relative position behind the named ones. Returns the number of entries
    /// whose absolute position changed.
    pub fn reorder(&self, entry_ids: &[String]) -> Result<QueueReorderResponse> {
        let _lock = acquire_queue_lock(&self.project_root)?;
        let Some(mut state) = load_queue_state(&self.project_root)? else {
            return Ok(QueueReorderResponse { reordered_count: 0 });
        };

        let original_order: Vec<String> = state
            .entries
            .iter()
            .map(|entry| entry.entry_id.clone())
            .collect();
        let mut consumed = vec![false; state.entries.len()];
        let mut reordered: Vec<DispatchQueueEntry> = Vec::with_capacity(state.entries.len());

        // Pull named entries to the front in the requested order. Duplicates
        // in the requested list quietly no-op (each entry can only be moved
        // once).
        for entry_id in entry_ids {
            for (index, entry) in state.entries.iter().enumerate() {
                if consumed[index] {
                    continue;
                }
                if entry.entry_id != *entry_id {
                    continue;
                }
                consumed[index] = true;
                reordered.push(entry.clone());
                break;
            }
        }

        // Append the rest in their original order.
        for (index, entry) in state.entries.iter().enumerate() {
            if !consumed[index] {
                reordered.push(entry.clone());
            }
        }

        let reordered_count = reordered
            .iter()
            .zip(original_order.iter())
            .filter(|(after, before)| &after.entry_id != *before)
            .count();

        if reordered_count == 0 {
            return Ok(QueueReorderResponse { reordered_count: 0 });
        }
        state.entries = reordered;
        save_queue_state(&self.project_root, &state)?;
        Ok(QueueReorderResponse { reordered_count })
    }

    // ============================================================
    // queue/mark_assigned + queue/completion
    // ============================================================

    /// Transition a single Pending entry to Assigned. Used by callers that
    /// prefer list+mark over atomic [`Self::lease`].
    pub fn mark_assigned(
        &self,
        entry_id: &str,
        workflow_id: Option<String>,
    ) -> Result<QueueMutationResponse> {
        self.mutate_entry(entry_id, |entry| match entry.status {
            DispatchQueueEntryStatus::Assigned => Ok(false),
            DispatchQueueEntryStatus::Pending => {
                entry.status = DispatchQueueEntryStatus::Assigned;
                if let Some(wid) = workflow_id {
                    entry.workflow_id = Some(wid);
                } else if entry.workflow_id.is_none() {
                    entry.workflow_id = Some(uuid::Uuid::new_v4().to_string());
                }
                entry.assigned_at = Some(Utc::now().to_rfc3339());
                Ok(true)
            }
            DispatchQueueEntryStatus::Held => Err(MutationError::NotPending),
            DispatchQueueEntryStatus::Unknown => Err(MutationError::NotPending),
        })
    }

    /// Mark a workflow's terminal status and prune the corresponding entry
    /// when the workflow ended.
    pub fn completion(
        &self,
        entry_id: &str,
        status: &str,
        workflow_ref: Option<&str>,
        workflow_id: Option<&str>,
    ) -> Result<QueueMutationResponse> {
        if !matches!(
            status,
            completion_status::COMPLETED | completion_status::FAILED | completion_status::CANCELLED
        ) {
            return Err(anyhow::anyhow!(
                "invalid completion status: '{status}' (expected one of: completed, failed, cancelled)"
            ));
        }

        let _lock = acquire_queue_lock(&self.project_root)?;
        let Some(mut state) = load_queue_state(&self.project_root)? else {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };
        let before = state.entries.len();
        state.entries.retain(|entry| {
            if entry.entry_id != entry_id {
                return true;
            }
            // Completion only prunes Assigned entries — a stale or misrouted
            // completion frame for a Pending/Held entry must NOT delete queued
            // work that was never leased.
            if entry.status != DispatchQueueEntryStatus::Assigned {
                return true;
            }
            // Match workflow_ref / workflow_id when provided.
            if let Some(workflow_ref) = workflow_ref {
                if entry
                    .dispatch
                    .as_ref()
                    .is_some_and(|dispatch| dispatch.workflow_ref != workflow_ref)
                {
                    return true;
                }
            }
            if let Some(workflow_id) = workflow_id {
                if entry
                    .workflow_id
                    .as_deref()
                    .is_some_and(|existing| existing != workflow_id)
                {
                    return true;
                }
            }
            false
        });
        let removed = before.saturating_sub(state.entries.len());
        if removed == 0 {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        }
        save_queue_state(&self.project_root, &state)?;
        Ok(QueueMutationResponse {
            changed: true,
            not_found: false,
        })
    }

    // ============================================================
    // queue/release_pending
    // ============================================================

    /// Atomically return an Assigned entry to Pending. Clears the workflow
    /// lease fields and appends an audit entry describing why.
    ///
    /// Errors:
    /// - [`QueueReleasePendingError::NotFound`] when `entry_id` is unknown.
    /// - [`QueueReleasePendingError::NotAssigned`] when the entry exists but
    ///   is in a state other than Assigned. The error carries the entry's
    ///   actual wire status so callers can surface it as the `-32208`
    ///   `data.actual_state` payload.
    pub fn release_pending(
        &self,
        entry_id: &str,
        reason: &str,
    ) -> std::result::Result<QueueReleasePendingResponse, QueueReleasePendingError> {
        let _lock =
            acquire_queue_lock(&self.project_root).map_err(QueueReleasePendingError::Backend)?;
        let mut state = load_queue_state(&self.project_root)
            .map_err(QueueReleasePendingError::Backend)?
            .ok_or_else(|| QueueReleasePendingError::NotFound {
                entry_id: entry_id.to_string(),
            })?;

        let entry = state
            .entries
            .iter_mut()
            .find(|entry| entry.entry_id == entry_id)
            .ok_or_else(|| QueueReleasePendingError::NotFound {
                entry_id: entry_id.to_string(),
            })?;

        if entry.status != DispatchQueueEntryStatus::Assigned {
            return Err(QueueReleasePendingError::NotAssigned {
                entry_id: entry_id.to_string(),
                actual_state: entry.status.as_wire().to_string(),
            });
        }

        let now = Utc::now().to_rfc3339();
        let from_status = entry.status.as_wire().to_string();
        entry.status = DispatchQueueEntryStatus::Pending;
        entry.assigned_at = None;
        entry.workflow_id = None;
        // TODO(codex-p2): fence late completions from the released workflow so
        // they cannot prune the replacement lease on entry id reuse. Requires
        // touching the completion path (see queue_service.rs completion()).
        entry.audit_log.push(DispatchQueueAuditEntry {
            at: now,
            method: "queue/release_pending".to_string(),
            from_status,
            to_status: status::PENDING.to_string(),
            reason: reason.to_string(),
        });

        save_queue_state(&self.project_root, &state).map_err(QueueReleasePendingError::Backend)?;

        Ok(QueueReleasePendingResponse {
            entry_id: entry_id.to_string(),
            status: status::PENDING.to_string(),
        })
    }

    // ============================================================
    // Internal helpers
    // ============================================================

    fn mutate_entry<F>(&self, entry_id: &str, mutate: F) -> Result<QueueMutationResponse>
    where
        F: FnOnce(&mut DispatchQueueEntry) -> std::result::Result<bool, MutationError>,
    {
        let _lock = acquire_queue_lock(&self.project_root)?;
        let Some(mut state) = load_queue_state(&self.project_root)? else {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };

        let Some(entry) = state
            .entries
            .iter_mut()
            .find(|entry| entry.entry_id == entry_id)
        else {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };

        let changed = match mutate(entry) {
            Ok(changed) => changed,
            Err(MutationError::NotPending) => {
                return Err(anyhow::anyhow!(
                    "queue entry {entry_id} is not in the expected pre-mutation status"
                ));
            }
        };

        if changed {
            save_queue_state(&self.project_root, &state)?;
        }
        Ok(QueueMutationResponse {
            changed,
            not_found: false,
        })
    }
}

/// Internal mutation error surfaced to RPC handlers as
/// `QUEUE_ENTRY_NOT_PENDING`.
#[derive(Debug)]
enum MutationError {
    NotPending,
}

/// Typed errors specific to `queue/lease`.
#[derive(Debug, thiserror::Error)]
pub enum QueueLeaseError {
    /// Returned to clients as
    /// [`animus_queue_protocol::error_codes::QUEUE_LEASE_WORKFLOW_ID_COUNT_MISMATCH`].
    #[error("workflow_ids length {actual} did not match max {expected}")]
    WorkflowIdCountMismatch {
        /// `max` from the request.
        expected: usize,
        /// `workflow_ids.len()` from the request.
        actual: usize,
    },
    /// Wrapped backend error (I/O, lock acquisition, persistence).
    #[error(transparent)]
    Backend(anyhow::Error),
}

/// Typed errors specific to `queue/release_pending`.
#[derive(Debug, thiserror::Error)]
pub enum QueueReleasePendingError {
    /// Entry id was not found in the queue. Surfaced as JSON-RPC `-32602`
    /// invalid_params per the v0.5.1 protocol contract.
    #[error("entry_id not found: {entry_id}")]
    NotFound {
        /// Entry id from the request.
        entry_id: String,
    },
    /// Entry exists but is not in the Assigned state. Surfaced as
    /// [`animus_queue_protocol::error_codes::QUEUE_ENTRY_NOT_ASSIGNED`]
    /// with `data.actual_state` populated.
    #[error("entry {entry_id} is in state '{actual_state}', expected 'assigned'")]
    NotAssigned {
        /// Entry id from the request.
        entry_id: String,
        /// Actual wire status (`pending` / `held`).
        actual_state: String,
    },
    /// Wrapped backend error (I/O, lock acquisition, persistence).
    #[error(transparent)]
    Backend(anyhow::Error),
}

fn entry_to_protocol(entry: &DispatchQueueEntry) -> Option<QueueEntry> {
    // The wire-level `QueueEntry.subject_dispatch` is required. Entries with
    // no persisted envelope are corrupt legacy state — log + skip them
    // instead of panicking, so callers see a healthy queue minus the
    // unrecoverable rows.
    let subject_dispatch = match entry.dispatch.clone() {
        Some(dispatch) => dispatch,
        None => {
            tracing::warn!(
                entry_id = %entry.entry_id,
                subject_id = entry.subject_id_ref(),
                "skipping queue entry with no persisted SubjectDispatch envelope"
            );
            return None;
        }
    };
    let subject_id = entry.subject_id_ref().to_string();
    let task_id = entry.task_id_ref().map(ToOwned::to_owned);
    let status_wire = match entry.status {
        DispatchQueueEntryStatus::Pending => status::PENDING.to_string(),
        DispatchQueueEntryStatus::Assigned => status::ASSIGNED.to_string(),
        DispatchQueueEntryStatus::Held => status::HELD.to_string(),
        DispatchQueueEntryStatus::Unknown => "unknown".to_string(),
    };
    Some(QueueEntry {
        entry_id: entry.entry_id.clone(),
        subject_id,
        task_id,
        subject_dispatch,
        status: status_wire,
        workflow_id: entry.workflow_id.clone(),
        enqueued_at: entry
            .enqueued_at
            .clone()
            .unwrap_or_else(|| Utc::now().to_rfc3339()),
        assigned_at: entry.assigned_at.clone(),
        held_at: entry.held_at.clone(),
    })
}

fn stats_from_state(state: &DispatchQueueState) -> QueueStats {
    QueueStats {
        total: state.entries.len(),
        pending: state
            .entries
            .iter()
            .filter(|entry| entry.status == DispatchQueueEntryStatus::Pending)
            .count(),
        assigned: state
            .entries
            .iter()
            .filter(|entry| entry.status == DispatchQueueEntryStatus::Assigned)
            .count(),
        held: state
            .entries
            .iter()
            .filter(|entry| entry.status == DispatchQueueEntryStatus::Held)
            .count(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_subject_protocol::{SubjectDispatch, SubjectRef};
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    fn task_dispatch(task_id: &str, workflow_ref: &str) -> SubjectDispatch {
        SubjectDispatch::for_subject_with_metadata(
            SubjectRef::task(task_id),
            workflow_ref,
            "manual-queue-enqueue",
            Utc::now(),
        )
    }

    #[test]
    fn enqueue_subject_dispatch_is_idempotent_for_same_task_pipeline() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let dispatch = SubjectDispatch::for_subject_with_metadata(
            SubjectRef::task("TASK-1"),
            "standard",
            "manual-queue-enqueue",
            Utc.with_ymd_and_hms(2026, 3, 7, 23, 0, 0).unwrap(),
        );

        let first = backend.enqueue(dispatch.clone()).expect("enqueue");
        let second = backend.enqueue(dispatch).expect("enqueue");

        assert!(first.enqueued);
        assert!(!second.enqueued);
        // Idempotent — second enqueue returns the same entry id.
        assert_eq!(first.entry_id, second.entry_id);
        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.stats.total, 1);
        assert_eq!(listed.entries[0].subject_id, "TASK-1");
    }

    #[test]
    fn hold_release_and_reorder_use_entry_ids() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let first = backend
            .enqueue(task_dispatch("TASK-1", "standard"))
            .expect("enqueue first");
        let second = backend
            .enqueue(task_dispatch("TASK-2", "standard"))
            .expect("enqueue second");

        let hold = backend.hold(&second.entry_id).expect("hold");
        assert!(hold.changed);
        let release = backend.release(&second.entry_id).expect("release");
        assert!(release.changed);
        let reorder = backend
            .reorder(&[second.entry_id.clone(), first.entry_id.clone()])
            .expect("reorder");
        assert_eq!(reorder.reordered_count, 2);

        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.entries[0].subject_id, "TASK-2");
        assert_eq!(listed.entries[1].subject_id, "TASK-1");
    }

    #[test]
    fn enqueue_subject_dispatch_accepts_non_task_subjects() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());

        let dispatch = SubjectDispatch::for_subject_with_metadata(
            SubjectRef::requirement("REQ-39"),
            "planning",
            "manual-queue-enqueue",
            Utc::now(),
        )
        .with_input(Some(json!({"scope":"shared-ingress"})));
        let result = backend.enqueue(dispatch).expect("enqueue");

        assert!(result.enqueued);
        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.stats.total, 1);
        assert_eq!(listed.entries[0].subject_id, "REQ-39");
        assert!(listed.entries[0].task_id.is_none());
        assert_eq!(listed.entries[0].subject_dispatch.workflow_ref, "planning");
    }

    #[test]
    fn reorder_subjects_keeps_all_entries_for_same_subject() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let standard = backend
            .enqueue(task_dispatch("TASK-1", "standard"))
            .expect("enqueue standard");
        let _t2 = backend
            .enqueue(task_dispatch("TASK-2", "standard"))
            .expect("enqueue second");
        let ops = backend
            .enqueue(task_dispatch("TASK-1", "ops"))
            .expect("enqueue ops");

        // Reorder both TASK-1 entries to the front (named by entry_id), preserving their requested order.
        let reorder = backend
            .reorder(&[standard.entry_id.clone(), ops.entry_id.clone()])
            .expect("reorder");
        assert!(reorder.reordered_count > 0);

        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.stats.total, 3);
        assert_eq!(listed.entries[0].subject_id, "TASK-1");
        assert_eq!(listed.entries[0].subject_dispatch.workflow_ref, "standard");
        assert_eq!(listed.entries[1].subject_id, "TASK-1");
        assert_eq!(listed.entries[1].subject_dispatch.workflow_ref, "ops");
        assert_eq!(listed.entries[2].subject_id, "TASK-2");
    }

    #[test]
    fn generic_subjects_use_kind_qualified_queue_ids() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let dispatch = SubjectDispatch::for_subject_with_metadata(
            SubjectRef::new("pack.review", "REV-7"),
            "review",
            "manual-queue-enqueue",
            Utc.with_ymd_and_hms(2026, 3, 8, 8, 0, 0).unwrap(),
        );

        let result = backend.enqueue(dispatch).expect("enqueue");

        assert!(result.enqueued);
        assert_eq!(result.subject_id, "pack.review::REV-7");
        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.entries[0].subject_id, "pack.review::REV-7");
        assert!(listed.entries[0].task_id.is_none());
    }
}
