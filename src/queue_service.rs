//! Core queue operations backing the JSON-RPC handlers.
//!
//! All mutations operate by `entry_id`. The legacy `subject_id`-keyed
//! mutations from the in-tree code were replaced as part of the v0.5
//! plugin extraction (see `docs/architecture/v0.5-protocol-specs.md` §2).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use animus_queue_protocol::{
    status, QueueEntry, QueueLeaseResponse, QueueListResponse, QueueMutationResponse,
    QueueNextDeadlineResponse, QueueReleasePendingResponse, QueueReorderResponse, QueueStats,
};
use animus_subject_protocol::SubjectDispatch;
use anyhow::Result;
use chrono::Utc;

use crate::dispatch_queue_state::{
    DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
};
use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state, save_queue_state};
use crate::identity::{dispatch_legacy_key, MISSING_SUBJECT_IDENTITY};
use crate::lease_ttl::DEFAULT_LEASE_TTL_SECS;
use crate::queue_history::{append_history, HistoryOutcome, HistoryRecord};

/// `queue/list` page size when the caller gives none (v0.2.9).
const DEFAULT_LIST_LIMIT: usize = 500;
/// Largest `queue/list` page (v0.2.9).
const MAX_LIST_LIMIT: usize = 2000;

/// File-locked backend wrapping a single project root's queue state.
#[derive(Debug, Clone)]
pub struct QueueBackend {
    project_root: PathBuf,
    lease_ttl_secs: i64,
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
    /// Non-fatal advisory surfaced to the caller (e.g. another entry already
    /// exists for this subject). `None` when there is nothing to flag.
    pub warning: Option<String>,
}

impl QueueBackend {
    /// Bind the backend to a project root. State / lock files live under
    /// `<project_root>/.animus/`.
    pub fn new(project_root: PathBuf) -> Self {
        Self {
            project_root,
            lease_ttl_secs: DEFAULT_LEASE_TTL_SECS,
        }
    }

    /// Use `secs` as the lease (ticket) length instead of the default.
    /// Callers pass a value from [`crate::lease_ttl::parse_lease_ttl`].
    pub fn with_lease_ttl(mut self, secs: i64) -> Self {
        self.lease_ttl_secs = secs;
        self
    }

    /// Bound project root.
    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    /// Lease (ticket) length in seconds.
    pub fn lease_ttl_secs(&self) -> i64 {
        self.lease_ttl_secs
    }

    // ============================================================
    // queue/enqueue
    // ============================================================

    /// Append a dispatch to the queue.
    ///
    /// Immediate enqueues (`run_at` is `None`) stay idempotent: an existing
    /// live entry for the same `(subject_key, workflow_ref)` is a no-op.
    /// Deferred enqueues (`run_at` set) are always created — scheduling the
    /// same subject for distinct times is legitimate — and any collision
    /// with an existing entry is surfaced via [`EnqueueOutcome::warning`]
    /// rather than rejected. The caller decides what to do with the warning.
    pub fn enqueue(
        &self,
        dispatch: SubjectDispatch,
        run_at: Option<String>,
        expire_after_secs: Option<u64>,
    ) -> std::result::Result<EnqueueOutcome, QueueCallError> {
        // A subjectless entry would make the whole file unreadable for queue
        // v0.3.3 (its dispatch type requires a subject), which breaks the
        // documented rollback path. Reject it before touching state.
        let Some(subject_id) = dispatch_legacy_key(&dispatch) else {
            return Err(QueueCallError::InvalidParams(
                MISSING_SUBJECT_IDENTITY.to_string(),
            ));
        };
        // v0.2.9 `parseRunAt`: an unreadable run_at means "dispatch now".
        let run_at = run_at.filter(|raw| {
            let readable = chrono::DateTime::parse_from_rfc3339(raw).is_ok();
            if !readable {
                tracing::warn!(
                    run_at = raw.as_str(),
                    "queue/enqueue: ignoring unparseable run_at; the entry dispatches now"
                );
            }
            readable
        });

        let _lock = acquire_queue_lock(&self.project_root)?;
        let mut state = load_queue_state(&self.project_root)?.unwrap_or_default();

        // Drop any expired deferred entries before evaluating this enqueue so
        // duplicate counts reflect the live queue.
        let finished = sweep_expired_entries(&mut state, Utc::now());

        // Count live (non-Unknown) entries already targeting this subject —
        // used for the advisory warning. Enqueue is NOT idempotent in either
        // direction: immediate and deferred enqueues both always create a new
        // entry, and a subject collision is surfaced as a warning for the
        // caller (agent/operator) to act on rather than silently dropped.
        // Lease-side `exclude_subjects` still prevents two entries for the
        // same subject from running concurrently.
        let dup_count = state
            .entries
            .iter()
            .filter(|entry| {
                entry.status != DispatchQueueEntryStatus::Unknown
                    && entry.subject_id_ref() == subject_id
            })
            .count();

        let warning = (dup_count > 0).then(|| {
            format!(
                "subject {subject_id} already has {dup_count} queued entr{}; duplicate enqueued",
                if dup_count == 1 { "y" } else { "ies" }
            )
        });

        let entry = DispatchQueueEntry::from_dispatch(dispatch, run_at, expire_after_secs);
        let entry_id = entry.entry_id.clone();
        state.entries.push(entry);
        self.commit(&state, &finished)?;
        Ok(EnqueueOutcome {
            enqueued: true,
            entry_id,
            subject_id,
            warning,
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
        filtered.truncate(limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT));

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
    // queue/next_deadline
    // ============================================================

    /// Earliest future `run_at` across pending deferred entries, for the
    /// daemon's precise-wake loop. Expired entries are swept first, so any
    /// returned instant is strictly in the future. `None` when no
    /// future-dated pending entry remains.
    pub fn next_deadline(&self) -> Result<QueueNextDeadlineResponse> {
        let _lock = acquire_queue_lock(&self.project_root)?;
        let mut state = load_queue_state(&self.project_root)?.unwrap_or_default();
        let now = Utc::now();
        let finished = sweep_expired_entries(&mut state, now);
        let next_run_at = state
            .entries
            .iter()
            .filter(|entry| entry.status == DispatchQueueEntryStatus::Pending)
            .filter_map(|entry| entry.parsed_run_at())
            .filter(|run_at| *run_at > now)
            .min()
            .map(|run_at| run_at.to_rfc3339());
        if !finished.is_empty() {
            self.commit(&state, &finished)?;
        }
        Ok(QueueNextDeadlineResponse { next_run_at })
    }

    // ============================================================
    // queue/lease (NEW atomic dispatch path)
    // ============================================================

    /// Old-style atomic dispatch: claim up to `max` entries, attach workflow
    /// ids, transition each to Assigned with a lease expiry, persist, and
    /// return them.
    ///
    /// Follows animus-postgres v0.2.9's `lease`, except that ticketed entries
    /// are never touched (difference 4):
    ///
    /// - Candidates are due Pending entries and old-style Assigned entries
    ///   whose lease expired; an expired entry is handed out again.
    /// - A subject is handed out at most once per call. `exclude_subjects`
    ///   (legacy subject keys) adds subjects to skip.
    /// - An entry keeps a workflow id it already has; otherwise the i-th
    ///   chosen entry gets `workflow_ids[i]`, or a fresh UUID when the caller
    ///   sent none. If `workflow_ids` is `Some`, its length MUST equal `max`
    ///   ([`QueueLeaseError::WorkflowIdCountMismatch`]).
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

        let now = Utc::now();
        // Drop deferred entries that blew past their expiry window while the
        // daemon was unavailable, instead of dispatching them late.
        let finished = sweep_expired_entries(&mut state, now);
        let mut exclude: HashSet<String> =
            exclude_subjects.unwrap_or_default().into_iter().collect();
        let mut chosen: Vec<usize> = Vec::new();
        for (index, entry) in state.entries.iter().enumerate() {
            if chosen.len() == max {
                break;
            }
            if entry.is_ticketed() {
                continue;
            }
            let due = entry.status == DispatchQueueEntryStatus::Pending
                && !entry.is_deferred_until_future(now);
            let expired = entry.status == DispatchQueueEntryStatus::Assigned
                && entry
                    .lease_expires_at
                    .is_some_and(|expires_at| expires_at < now);
            if !due && !expired {
                continue;
            }
            // Corrupt legacy state — an entry with no dispatch envelope can't
            // be returned over the wire (`QueueEntry.subject_dispatch` is
            // required). Skip it instead of poisoning the lease.
            if entry.dispatch.is_none() {
                tracing::warn!(
                    entry_id = %entry.entry_id,
                    "queue/lease: skipping entry with no SubjectDispatch envelope"
                );
                continue;
            }
            if !exclude.insert(entry.subject_id_ref().to_string()) {
                continue;
            }
            chosen.push(index);
        }

        let expires_at = now + chrono::Duration::seconds(self.lease_ttl_secs);
        let now_rfc3339 = now.to_rfc3339();
        let mut leased: Vec<QueueEntry> = Vec::with_capacity(chosen.len());
        for (slot, index) in chosen.into_iter().enumerate() {
            let entry = &mut state.entries[index];
            // v0.2.9: keep a lease's workflow id across expiry and reclaim.
            // The daemon may already have created that workflow.
            let workflow_id = entry
                .workflow_id
                .clone()
                .filter(|id| !id.is_empty())
                .or_else(|| workflow_ids.as_ref().map(|ids| ids[slot].clone()))
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            entry.status = DispatchQueueEntryStatus::Assigned;
            entry.workflow_id = Some(workflow_id.clone());
            entry.lease_owner = Some(workflow_id);
            entry.lease_expires_at = Some(expires_at);
            entry.assigned_at = Some(now_rfc3339.clone());
            if let Some(protocol_entry) = entry_to_protocol(entry) {
                leased.push(protocol_entry);
            }
        }

        if !leased.is_empty() || !finished.is_empty() {
            self.commit(&state, &finished)
                .map_err(QueueLeaseError::Backend)?;
        }
        Ok(QueueLeaseResponse { leased })
    }

    // ============================================================
    // queue/hold + queue/release + queue/drop
    // ============================================================

    /// Hold a Pending entry. Idempotent on already-held.
    pub fn hold(
        &self,
        entry_id: &str,
    ) -> std::result::Result<QueueMutationResponse, QueueMutationError> {
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
    pub fn release(
        &self,
        entry_id: &str,
    ) -> std::result::Result<QueueMutationResponse, QueueMutationError> {
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
        let Some(index) = state
            .entries
            .iter()
            .position(|entry| entry.entry_id == entry_id)
        else {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };
        let dropped = state.entries.remove(index);
        let record = HistoryRecord::finished(&dropped, HistoryOutcome::Dropped, "queue/drop", None);
        self.commit(&state, &[record])?;
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

    /// Transition a single old-style Pending entry to Assigned (v0.2.9
    /// `markAssigned`). The entry gets `workflow_id`, or a fresh UUID, even
    /// if it had an id before, plus a lease expiry. Ticketed entries are
    /// refused with [`QueueMutationError::Fenced`] (difference 4).
    pub fn mark_assigned(
        &self,
        entry_id: &str,
        workflow_id: Option<String>,
    ) -> std::result::Result<QueueMutationResponse, QueueMutationError> {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(self.lease_ttl_secs);
        self.mutate_entry(entry_id, |entry| {
            if entry.is_ticketed() {
                return Err(MutationError::Fenced);
            }
            match entry.status {
                DispatchQueueEntryStatus::Assigned => Ok(false),
                DispatchQueueEntryStatus::Pending => {
                    let workflow_id =
                        workflow_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                    entry.status = DispatchQueueEntryStatus::Assigned;
                    entry.workflow_id = Some(workflow_id.clone());
                    entry.lease_owner = Some(workflow_id);
                    entry.lease_expires_at = Some(expires_at);
                    entry.assigned_at = Some(now.to_rfc3339());
                    Ok(true)
                }
                DispatchQueueEntryStatus::Held => Err(MutationError::NotPending),
                DispatchQueueEntryStatus::Unknown => Err(MutationError::NotPending),
            }
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
        let Some(outcome) = HistoryOutcome::from_completion_status(status) else {
            return Err(anyhow::anyhow!(
                "invalid completion status: '{status}' (expected one of: completed, failed, cancelled)"
            ));
        };

        let _lock = acquire_queue_lock(&self.project_root)?;
        let Some(mut state) = load_queue_state(&self.project_root)? else {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };
        let Some(index) = state.entries.iter().position(|entry| {
            entry.entry_id == entry_id
                // Completion only finishes Assigned entries — a stale or
                // misrouted completion frame for a Pending/Held entry must NOT
                // delete queued work that was never leased.
                && entry.status == DispatchQueueEntryStatus::Assigned
                // Match workflow_ref / workflow_id when provided.
                && workflow_ref.is_none_or(|workflow_ref| {
                    entry
                        .dispatch
                        .as_ref()
                        .is_none_or(|dispatch| dispatch.workflow_ref == workflow_ref)
                })
                && workflow_id.is_none_or(|workflow_id| {
                    entry
                        .workflow_id
                        .as_deref()
                        .is_none_or(|existing| existing == workflow_id)
                })
        }) else {
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };
        let done = state.entries.remove(index);
        let record = HistoryRecord::finished(&done, outcome, "queue/completion", None);
        self.commit(&state, &[record])?;
        Ok(QueueMutationResponse {
            changed: true,
            not_found: false,
        })
    }

    // ============================================================
    // queue/release_pending
    // ============================================================

    /// Atomically return an old-style Assigned entry to Pending. Clears the
    /// workflow and lease fields and appends an audit entry describing why.
    ///
    /// Errors:
    /// - [`QueueReleasePendingError::NotFound`] when `entry_id` is unknown.
    /// - [`QueueReleasePendingError::Fenced`] when the entry is ticketed
    ///   (difference 4; use `queue/v2/release_pending`).
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

        if entry.is_ticketed() {
            return Err(QueueReleasePendingError::Fenced {
                entry_id: entry_id.to_string(),
            });
        }
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
        entry.lease_owner = None;
        entry.lease_expires_at = None;
        // A late old-style completion from the released workflow can still
        // finish the entry's next run. That is v0.2.9's behaviour and an
        // accepted risk for old-style calls (spec §7.3); ticketed work uses
        // queue/v2/*, which is fenced.
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

    /// Persist `state`, first recording `finished` entries in the history
    /// (see [`crate::queue_history`] for why this order is crash-safe).
    pub(crate) fn commit(
        &self,
        state: &DispatchQueueState,
        finished: &[HistoryRecord],
    ) -> Result<()> {
        append_history(&self.project_root, finished)?;
        save_queue_state(&self.project_root, state)
    }

    fn mutate_entry<F>(
        &self,
        entry_id: &str,
        mutate: F,
    ) -> std::result::Result<QueueMutationResponse, QueueMutationError>
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
                return Err(QueueMutationError::NotPending {
                    entry_id: entry_id.to_string(),
                });
            }
            Err(MutationError::Fenced) => {
                return Err(QueueMutationError::Fenced {
                    entry_id: entry_id.to_string(),
                });
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

/// Internal outcome of a [`QueueBackend::mutate_entry`] closure.
#[derive(Debug)]
enum MutationError {
    NotPending,
    Fenced,
}

/// Typed errors for `queue/hold`, `queue/release` and `queue/mark_assigned`.
#[derive(Debug, thiserror::Error)]
pub enum QueueMutationError {
    /// The entry is not in the status the call expects. Surfaced as
    /// [`animus_queue_protocol::error_codes::QUEUE_ENTRY_NOT_PENDING`].
    #[error("queue entry {entry_id} is not in the expected pre-mutation status")]
    NotPending {
        /// Entry id from the request.
        entry_id: String,
    },
    /// The entry is ticketed; old-style calls may not change it (difference
    /// 4). Surfaced as [`animus_queue_protocol::error_codes::QUEUE_STALE_FENCE`].
    #[error("queue entry {entry_id} is owned by a generation-fenced lease; use queue/v2/*")]
    Fenced {
        /// Entry id from the request.
        entry_id: String,
    },
    /// Wrapped backend error (I/O, lock acquisition, persistence).
    #[error(transparent)]
    Backend(#[from] anyhow::Error),
}

/// Errors from calls that validate caller input before touching state.
#[derive(Debug, thiserror::Error)]
pub enum QueueCallError {
    /// Bad caller input. Surfaced as JSON-RPC `-32602` invalid params.
    #[error("{0}")]
    InvalidParams(String),
    /// Wrapped backend error (I/O, lock acquisition, persistence).
    #[error(transparent)]
    Backend(#[from] anyhow::Error),
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
    /// The entry is ticketed; old-style calls may not change it (difference
    /// 4). Surfaced as [`animus_queue_protocol::error_codes::QUEUE_STALE_FENCE`].
    #[error("queue entry {entry_id} is owned by a generation-fenced lease; use queue/v2/*")]
    Fenced {
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

pub(crate) fn entry_to_protocol(entry: &DispatchQueueEntry) -> Option<QueueEntry> {
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
        run_at: entry.run_at.clone(),
        expire_after_secs: entry.expire_after_secs,
    })
}

/// Remove Pending deferred entries whose expiry window has elapsed (`now`
/// is past `run_at + expire_after_secs`). Only Pending entries are swept —
/// an entry already Assigned/Held is in flight and out of scope. Returns
/// history records for the dropped entries; callers persist them with
/// [`QueueBackend::commit`].
pub(crate) fn sweep_expired_entries(
    state: &mut DispatchQueueState,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<HistoryRecord> {
    let mut finished = Vec::new();
    state.entries.retain(|entry| {
        if entry.status != DispatchQueueEntryStatus::Pending {
            return true;
        }
        match entry.expiry_deadline() {
            Some(deadline) if now > deadline => {
                tracing::info!(
                    entry_id = %entry.entry_id,
                    subject_id = entry.subject_id_ref(),
                    "queue: expiring deferred entry past its run_at + expire_after_secs window"
                );
                finished.push(HistoryRecord::finished(
                    entry,
                    HistoryOutcome::Dropped,
                    "expiry-sweep",
                    Some("run_at + expire_after_secs passed before the entry was leased"),
                ));
                false
            }
            _ => true,
        }
    });
    finished
}

fn stats_from_state(state: &DispatchQueueState) -> QueueStats {
    let now = Utc::now();
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
        deferred: state
            .entries
            .iter()
            .filter(|entry| {
                entry.status == DispatchQueueEntryStatus::Pending
                    && entry.is_deferred_until_future(now)
            })
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
    fn enqueue_same_subject_pipeline_creates_distinct_entries_with_warning() {
        // Enqueue is no longer idempotent: re-enqueuing the same subject
        // creates a second entry and surfaces a warning. Lease-side
        // exclusivity (not enqueue dedup) keeps the subject from running twice
        // concurrently.
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let dispatch = SubjectDispatch::for_subject_with_metadata(
            SubjectRef::task("TASK-1"),
            "standard",
            "manual-queue-enqueue",
            Utc.with_ymd_and_hms(2026, 3, 7, 23, 0, 0).unwrap(),
        );

        let first = backend
            .enqueue(dispatch.clone(), None, None)
            .expect("enqueue");
        let second = backend.enqueue(dispatch, None, None).expect("enqueue");

        assert!(first.enqueued);
        assert!(second.enqueued, "re-enqueue creates a new entry");
        assert!(first.warning.is_none());
        assert!(second.warning.is_some(), "collision surfaces a warning");
        assert_ne!(first.entry_id, second.entry_id);
        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.stats.total, 2);
        assert_eq!(listed.entries[0].subject_id, "TASK-1");
    }

    #[test]
    fn enqueue_rejects_subjectless_dispatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let dispatch = SubjectDispatch::subjectless("standard", "manual-queue-enqueue", Utc::now());

        let error = backend
            .enqueue(dispatch, None, None)
            .expect_err("subjectless enqueue must be rejected");

        assert!(matches!(error, QueueCallError::InvalidParams(_)));
        assert_eq!(error.to_string(), "queue subject identity is missing");
        assert!(
            !temp.path().join(".animus").exists(),
            "a rejected enqueue must not create queue files"
        );
    }

    #[test]
    fn hold_release_and_reorder_use_entry_ids() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let first = backend
            .enqueue(task_dispatch("TASK-1", "standard"), None, None)
            .expect("enqueue first");
        let second = backend
            .enqueue(task_dispatch("TASK-2", "standard"), None, None)
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
        let result = backend.enqueue(dispatch, None, None).expect("enqueue");

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
            .enqueue(task_dispatch("TASK-1", "standard"), None, None)
            .expect("enqueue standard");
        let _t2 = backend
            .enqueue(task_dispatch("TASK-2", "standard"), None, None)
            .expect("enqueue second");
        let ops = backend
            .enqueue(task_dispatch("TASK-1", "ops"), None, None)
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

        let result = backend.enqueue(dispatch, None, None).expect("enqueue");

        assert!(result.enqueued);
        assert_eq!(result.subject_id, "pack.review::REV-7");
        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.entries[0].subject_id, "pack.review::REV-7");
        assert!(listed.entries[0].task_id.is_none());
    }

    #[test]
    fn deferred_entry_in_future_is_not_leased() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let run_at = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        let outcome = backend
            .enqueue(
                task_dispatch("TASK-1", "standard"),
                Some(run_at.clone()),
                None,
            )
            .expect("enqueue");
        assert!(outcome.enqueued);

        let leased = backend.lease(10, None, None).expect("lease");
        assert!(leased.leased.is_empty(), "future entry must not be leased");

        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.stats.pending, 1);
        assert_eq!(listed.stats.deferred, 1);
        assert_eq!(listed.entries[0].run_at.as_deref(), Some(run_at.as_str()));
    }

    #[test]
    fn deferred_entry_past_run_at_is_leased() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let run_at = (Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
        backend
            .enqueue(task_dispatch("TASK-1", "standard"), Some(run_at), None)
            .expect("enqueue");

        let leased = backend.lease(10, None, None).expect("lease");
        assert_eq!(leased.leased.len(), 1, "due entry must be leased");
        assert_eq!(leased.leased[0].subject_id, "TASK-1");
    }

    #[test]
    fn expired_deferred_entry_is_swept_not_leased() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        // run_at well in the past with a tiny grace window → already expired.
        let run_at = (Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
        backend
            .enqueue(task_dispatch("TASK-1", "standard"), Some(run_at), Some(60))
            .expect("enqueue");

        let leased = backend.lease(10, None, None).expect("lease");
        assert!(leased.leased.is_empty(), "expired entry must not dispatch");

        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.stats.total, 0, "expired entry must be swept");
    }

    #[test]
    fn deferred_duplicate_subject_is_enqueued_with_warning() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let first = backend
            .enqueue(task_dispatch("TASK-1", "standard"), None, None)
            .expect("enqueue first");
        assert!(first.enqueued);
        assert!(first.warning.is_none());

        let run_at = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        let second = backend
            .enqueue(task_dispatch("TASK-1", "standard"), Some(run_at), None)
            .expect("enqueue deferred dup");
        assert!(second.enqueued, "deferred duplicate must still enqueue");
        assert_ne!(first.entry_id, second.entry_id);
        let warning = second.warning.expect("duplicate warning");
        assert!(warning.contains("TASK-1"), "warning names the subject");

        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.stats.total, 2);
    }

    #[test]
    fn immediate_duplicate_is_enqueued_with_warning() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());
        let first = backend
            .enqueue(task_dispatch("TASK-1", "standard"), None, None)
            .expect("enqueue first");
        let second = backend
            .enqueue(task_dispatch("TASK-1", "standard"), None, None)
            .expect("enqueue second");

        assert!(first.enqueued);
        assert!(
            second.enqueued,
            "immediate duplicate is now enqueued, not deduped"
        );
        assert_ne!(first.entry_id, second.entry_id);
        assert!(second.warning.is_some(), "collision surfaces a warning");

        let listed = backend.list(&[], None, None).expect("list");
        assert_eq!(listed.stats.total, 2);
    }

    #[test]
    fn next_deadline_reports_earliest_future_run_at() {
        let temp = tempfile::tempdir().expect("tempdir");
        let backend = QueueBackend::new(temp.path().to_path_buf());

        // Empty queue → no deadline.
        assert_eq!(backend.next_deadline().expect("nd").next_run_at, None);

        // Immediate entry contributes no deadline.
        backend
            .enqueue(task_dispatch("NOW", "standard"), None, None)
            .expect("immediate");
        assert_eq!(backend.next_deadline().expect("nd").next_run_at, None);

        // Two deferred entries → earliest wins.
        let later = (Utc::now() + chrono::Duration::hours(3)).to_rfc3339();
        let sooner = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        backend
            .enqueue(task_dispatch("LATER", "standard"), Some(later), None)
            .expect("later");
        backend
            .enqueue(
                task_dispatch("SOONER", "standard"),
                Some(sooner.clone()),
                None,
            )
            .expect("sooner");

        assert_eq!(
            backend.next_deadline().expect("nd").next_run_at,
            Some(sooner)
        );
    }
}
