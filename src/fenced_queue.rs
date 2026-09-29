//! Generation-fenced ("ticketed") queue calls: `queue/v2/*`.
//!
//! Behaviour follows animus-postgres v0.2.9 (`src/queue.ts`) except for the
//! seven differences listed in the design spec (§7.2). Each one is marked
//! where it applies.

use animus_execution_protocol::{
    ExecutionFence, RepositoryReservation, SubjectGeneration, EXECUTION_FENCE_SCHEMA_ID,
    EXECUTION_FENCE_VERSION,
};
use animus_queue_protocol::{
    status, FencedQueueEntry, QueueCompletionV2Request, QueueEnqueueV2Request,
    QueueEnqueueV2Response, QueueLeaseBlock, QueueLeaseBlockReason, QueueLeaseMutationOutcome,
    QueueLeaseMutationResponse, QueueLeaseRecoverRequest, QueueLeaseRenewRequest,
    QueueLeaseV2Request, QueueLeaseV2Response, QueueReleasePendingV2Request,
};
use chrono::{Duration, Utc};

use crate::dispatch_queue_state::{
    DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
    IdempotencyBinding,
};
use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state};
use crate::identity::{dispatch_canonical_id, dispatch_task_id};
use crate::queue_history::{
    find_history_by_entry_id, find_history_by_idempotency_key, HistoryOutcome, HistoryRecord,
};
use crate::queue_service::{
    entry_to_protocol, sweep_expired_entries, QueueBackend, QueueCallError,
};
use crate::request_hash::enqueue_request_hash;

/// Most entries one `queue/v2/lease` call hands out. Advertised to the host as
/// `max_lease_batch`; the 0.7 daemon requires at least 5.
pub const MAX_LEASE_BATCH: usize = 5;

/// Longest accepted idempotency key after trimming (v0.2.9).
const MAX_IDEMPOTENCY_KEY_CHARS: usize = 256;

impl QueueBackend {
    /// `queue/v2/enqueue`: add a ticketed entry with the subject's next
    /// generation.
    ///
    /// - The same idempotency key with the same content returns the original
    ///   receipt (`enqueued: false`); with different content it is an error.
    ///   The content hash leaves out `dispatch.requested_at` (difference 2).
    /// - A task that already has a waiting or held entry, or a running
    ///   ticketed one, gets that entry back with a warning instead of a second
    ///   one (difference 3). The call's idempotency key is bound to that entry,
    ///   so a retry replays the same receipt. A running old-style entry, left
    ///   from before the upgrade, doesn't block the add, as in v0.2.9.
    /// - A malformed `run_at` is an error (difference 7), and so is an
    ///   `expire_after_secs` whose deadline no timestamp can hold.
    pub fn enqueue_v2(
        &self,
        request: QueueEnqueueV2Request,
    ) -> Result<QueueEnqueueV2Response, QueueCallError> {
        request.validate().map_err(QueueCallError::InvalidParams)?;
        let QueueEnqueueV2Request {
            subject_dispatch: dispatch,
            idempotency_key,
            repository,
            run_at,
            expire_after_secs,
        } = request;
        let qualified_id =
            dispatch_canonical_id(&dispatch).map_err(QueueCallError::InvalidParams)?;
        let parsed_run_at = run_at
            .as_deref()
            .map(|raw| {
                chrono::DateTime::parse_from_rfc3339(raw).map_err(|error| {
                    QueueCallError::InvalidParams(format!(
                        "run_at must be RFC 3339 ({raw}): {error}"
                    ))
                })
            })
            .transpose()?;
        if let Some(secs) = expire_after_secs {
            let window = i64::try_from(secs)
                .ok()
                .and_then(chrono::TimeDelta::try_seconds);
            let fits = match (window, parsed_run_at) {
                (Some(window), Some(run_at)) => run_at.checked_add_signed(window).is_some(),
                (Some(_), None) => true,
                (None, _) => false,
            };
            if !fits {
                return Err(QueueCallError::InvalidParams(format!(
                    "expire_after_secs {secs} is too large: run_at + expire_after_secs must be a valid time"
                )));
            }
        }
        let idempotency_key = idempotency_key.map(|key| key.trim().to_string());
        if idempotency_key
            .as_ref()
            .is_some_and(|key| key.chars().count() > MAX_IDEMPOTENCY_KEY_CHARS)
        {
            return Err(QueueCallError::InvalidParams(
                "idempotency_key must be a non-empty string of at most 256 characters".to_string(),
            ));
        }
        let repository = repository.map(|reservation| RepositoryReservation {
            repository: reservation.repository.trim().to_string(),
            base_ref: reservation.base_ref.trim().to_string(),
            head_ref: reservation.head_ref.trim().to_string(),
        });
        let request_hash = enqueue_request_hash(
            &dispatch,
            repository.as_ref(),
            run_at.as_deref(),
            expire_after_secs,
        );

        let _lock = acquire_queue_lock(self.project_root())?;
        let mut state = load_queue_state(self.project_root())?.unwrap_or_default();
        let finished = sweep_expired_entries(&mut state, Utc::now());

        if let Some(key) = idempotency_key.as_deref() {
            if let Some(receipt) = self.idempotent_receipt(&state, &finished, key, &request_hash)? {
                if !finished.is_empty() {
                    self.commit(&state, &finished)?;
                }
                return Ok(receipt);
            }
        }

        if let Some(index) = live_entry_for_subject(&state, &qualified_id) {
            let subject =
                ensure_ticket_identity(&mut state, index).map_err(QueueCallError::InvalidParams)?;
            let entry = &mut state.entries[index];
            if let Some(key) = idempotency_key {
                entry
                    .extra_idempotency_keys
                    .push(IdempotencyBinding { key, request_hash });
            }
            let entry_id = entry.entry_id.clone();
            self.commit(&state, &finished)?;
            return Ok(QueueEnqueueV2Response {
                enqueued: false,
                entry_id,
                subject,
                warning: Some(format!(
                    "subject {qualified_id} already has an active generation; enqueue rejected"
                )),
            });
        }

        let generation = next_subject_generation(&mut state, &qualified_id);
        let entry = DispatchQueueEntry {
            entry_id: uuid::Uuid::new_v4().to_string(),
            subject_id: Some(qualified_id.clone()),
            task_id: dispatch_task_id(&dispatch).unwrap_or_default().to_string(),
            dispatch: Some(dispatch),
            status: DispatchQueueEntryStatus::Pending,
            enqueued_at: Some(Utc::now().to_rfc3339()),
            run_at,
            expire_after_secs,
            subject_generation: Some(generation),
            repository,
            idempotency_key,
            request_hash: Some(request_hash),
            ..DispatchQueueEntry::default()
        };
        let entry_id = entry.entry_id.clone();
        state.entries.push(entry);
        self.commit(&state, &finished)?;
        Ok(QueueEnqueueV2Response {
            enqueued: true,
            entry_id,
            subject: SubjectGeneration {
                qualified_id,
                generation,
            },
            warning: None,
        })
    }

    /// `queue/v2/lease`: hand out up to `max` (1 to 5) due Pending entries in
    /// queue order, each with its full execution fence.
    ///
    /// Candidates are the first `max(50, max * 10)` due Pending entries.
    /// Held, deferred and running entries are never handed out, so an entry
    /// whose ticket expired must be taken over with `queue/v2/lease/recover`.
    /// A candidate stays where it is and is reported in `blocked` when:
    ///
    /// - its subject can't be identified (`missing_execution_identity`);
    /// - a fence in `exclude` holds the same subject generation
    ///   (`subject_generation_active`);
    /// - a fence in `exclude`, or a running entry, holds the same repository
    ///   and branch (`repository_ref_collision`).
    ///
    /// Old-style entries get ticket identity here (difference 5). A handed-out
    /// entry keeps a workflow id it already has, otherwise it takes the next
    /// unused id from `workflow_ids`. Its lease generation rises by one.
    pub fn lease_v2(
        &self,
        request: QueueLeaseV2Request,
    ) -> Result<QueueLeaseV2Response, QueueCallError> {
        request.validate().map_err(QueueCallError::InvalidParams)?;
        if request.max > MAX_LEASE_BATCH {
            return Err(QueueCallError::InvalidParams(
                "queue v2 lease requires max 1..5, owner_id, and max unique workflow_ids"
                    .to_string(),
            ));
        }
        let owner_id = request.owner_id.trim().to_string();

        let _lock = acquire_queue_lock(self.project_root())?;
        let mut state = load_queue_state(self.project_root())?.unwrap_or_default();
        let now = Utc::now();
        let finished = sweep_expired_entries(&mut state, now);
        let candidates: Vec<usize> = state
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                entry.status == DispatchQueueEntryStatus::Pending
                    && !entry.is_deferred_until_future(now)
            })
            .map(|(index, _)| index)
            .take((request.max * 10).max(50))
            .collect();

        let expires_at = now + chrono::Duration::seconds(self.lease_ttl_secs());
        let mut changed = !finished.is_empty();
        let mut leased = Vec::new();
        let mut blocked = Vec::new();
        let mut unused_workflow_ids = request.workflow_ids.iter().peekable();
        for index in candidates {
            if leased.len() >= request.max {
                break;
            }
            let entry_id = state.entries[index].entry_id.clone();
            let was_ticketed = state.entries[index].is_ticketed();
            let identity = if state.entries[index].dispatch.is_some() {
                ensure_ticket_identity(&mut state, index)
            } else {
                Err(format!("queue entry {entry_id} has no dispatch envelope"))
            };
            let subject = match identity {
                Ok(subject) => subject,
                Err(error) => {
                    tracing::warn!(
                        entry_id = %entry_id,
                        %error,
                        "queue/v2/lease: entry has no usable subject identity"
                    );
                    blocked.push(lease_block(
                        entry_id,
                        QueueLeaseBlockReason::MissingExecutionIdentity,
                        None,
                    ));
                    continue;
                }
            };
            changed |= !was_ticketed;

            if let Some(conflict) = request
                .exclude
                .iter()
                .find(|fence| fence.subject.as_ref() == Some(&subject))
            {
                blocked.push(lease_block(
                    entry_id,
                    QueueLeaseBlockReason::SubjectGenerationActive,
                    Some(conflict.clone()),
                ));
                continue;
            }
            if let Some(repository) = state.entries[index].repository.clone() {
                let key = repository.collision_key();
                if let Some(conflict) = request.exclude.iter().find(|fence| {
                    fence
                        .repository
                        .as_ref()
                        .is_some_and(|held| held.collision_key() == key)
                }) {
                    blocked.push(lease_block(
                        entry_id,
                        QueueLeaseBlockReason::RepositoryRefCollision,
                        Some(conflict.clone()),
                    ));
                    continue;
                }
                if let Some(running) = running_entry_on_branch(&state, &key) {
                    blocked.push(lease_block(
                        entry_id,
                        QueueLeaseBlockReason::RepositoryRefCollision,
                        state.entries[running].execution_fence(),
                    ));
                    continue;
                }
            }

            let existing_workflow_id = state.entries[index]
                .workflow_id
                .clone()
                .filter(|id| !id.is_empty());
            // A fresh id is only used up once the hand-out succeeds, so a
            // damaged entry below can't take the slot's id from the next one.
            let takes_fresh_id = existing_workflow_id.is_none();
            let Some(workflow_id) =
                existing_workflow_id.or_else(|| unused_workflow_ids.peek().map(|id| (*id).clone()))
            else {
                break;
            };
            let before = state.entries[index].clone();
            let entry = &mut state.entries[index];
            entry.status = DispatchQueueEntryStatus::Assigned;
            entry.workflow_id = Some(workflow_id);
            entry.workflow_generation = Some(entry.workflow_generation.unwrap_or(1));
            entry.lease_owner = Some(owner_id.clone());
            entry.lease_generation += 1;
            entry.lease_expires_at = Some(expires_at);
            entry.assigned_at = Some(now.to_rfc3339());
            entry.held_at = None;
            // A generation of 0 or an empty id can only come from a damaged
            // queue.json. Leave that entry as it was and report it, rather
            // than failing the whole call on every hand-out.
            let Some(execution) = entry.execution_fence() else {
                tracing::warn!(
                    entry_id = %entry_id,
                    "queue/v2/lease: entry has damaged ticket identity; left unchanged"
                );
                state.entries[index] = before;
                blocked.push(lease_block(
                    entry_id,
                    QueueLeaseBlockReason::MissingExecutionIdentity,
                    None,
                ));
                continue;
            };
            if takes_fresh_id {
                unused_workflow_ids.next();
            }
            changed = true;
            leased.push(FencedQueueEntry {
                entry: entry_to_protocol(entry).expect("checked above: entry has a dispatch"),
                execution,
            });
        }

        if changed {
            self.commit(&state, &finished)?;
        }
        Ok(QueueLeaseV2Response { leased, blocked })
    }

    /// `queue/v2/lease/renew`: extend a live ticket. The expiry moves to
    /// `now + ttl` but never earlier than it already is (spec §7.1: the 0.7
    /// daemon rejects a renewal whose expiry goes backwards). The generation
    /// stays the same. An expired ticket must be taken over instead.
    pub fn renew_lease(
        &self,
        request: QueueLeaseRenewRequest,
    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
        request.validate().map_err(QueueCallError::InvalidParams)?;
        let ttl_secs = self.requested_ttl(request.ttl_secs);
        self.fenced_mutation(&request.execution, FencedOperation::Renew { ttl_secs })
    }

    /// `queue/v2/lease/recover`: hand an expired ticket to a different owner.
    /// The lease generation rises by exactly one; the workflow id, workflow
    /// generation and subject generation stay.
    pub fn recover_lease(
        &self,
        request: QueueLeaseRecoverRequest,
    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
        request.validate().map_err(QueueCallError::InvalidParams)?;
        let ttl_secs = self.requested_ttl(request.ttl_secs);
        self.fenced_mutation(
            &request.execution,
            FencedOperation::Recover {
                new_owner_id: request.new_owner_id.trim().to_string(),
                ttl_secs,
            },
        )
    }

    /// `queue/v2/completion`: finish the ticket's entry and move it to the
    /// history. Accepted with an expired ticket as long as nobody took the
    /// task over (difference 1). A repeated "done" is `already_applied` and
    /// the first outcome is kept.
    pub fn completion_v2(
        &self,
        request: QueueCompletionV2Request,
    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
        request.validate().map_err(QueueCallError::InvalidParams)?;
        let outcome = HistoryOutcome::from_completion_status(&request.status)
            .expect("validate() accepts only terminal statuses");
        self.fenced_mutation(&request.execution, FencedOperation::Complete { outcome })
    }

    /// `queue/v2/release_pending`: put the ticket's entry back to waiting. It
    /// keeps its workflow id and ticket fields, so a retry is recognised and
    /// the next hand-out raises the lease generation. Accepted with an
    /// expired ticket as long as nobody took the task over (difference 1).
    pub fn release_pending_v2(
        &self,
        request: QueueReleasePendingV2Request,
    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
        request.validate().map_err(QueueCallError::InvalidParams)?;
        self.fenced_mutation(
            &request.execution,
            FencedOperation::ReleasePending {
                reason: request.reason.trim().to_string(),
            },
        )
    }

    /// A caller's `ttl_secs`, capped at the configured ticket length (v0.2.9).
    fn requested_ttl(&self, ttl_secs: Option<u64>) -> i64 {
        let limit = self.lease_ttl_secs();
        ttl_secs.map_or(limit, |secs| {
            i64::try_from(secs).unwrap_or(i64::MAX).min(limit)
        })
    }

    /// The body shared by the four ticket calls (v0.2.9 `fencedMutation`,
    /// plus difference 1). Ticket problems are outcomes, not errors.
    fn fenced_mutation(
        &self,
        execution: &ExecutionFence,
        operation: FencedOperation,
    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
        let entry_id = execution
            .queue_lease
            .as_ref()
            .expect("validate_queue_backed() requires queue_lease")
            .entry_id
            .clone();
        let _lock = acquire_queue_lock(self.project_root())?;
        let mut state = load_queue_state(self.project_root())?.unwrap_or_default();
        let Some(index) = state
            .entries
            .iter()
            .position(|entry| entry.entry_id == entry_id)
        else {
            return self.finished_entry_outcome(&entry_id, execution, &operation);
        };

        let now = Utc::now();
        let entry = &mut state.entries[index];
        if matches!(operation, FencedOperation::ReleasePending { .. })
            && entry.status == DispatchQueueEntryStatus::Pending
            && exact_fence_matches(entry, execution)
        {
            return Ok(mutation_response(
                QueueLeaseMutationOutcome::AlreadyApplied,
                None,
                None,
            ));
        }
        if entry.status != DispatchQueueEntryStatus::Assigned {
            return Ok(mutation_response(
                QueueLeaseMutationOutcome::NotAssigned,
                None,
                Some(format!("queue entry is {}", entry.status.as_wire())),
            ));
        }
        if !exact_fence_matches(entry, execution) {
            return Ok(mutation_response(
                QueueLeaseMutationOutcome::StaleFence,
                None,
                Some("execution fence does not own this queue lease".to_string()),
            ));
        }

        match operation {
            FencedOperation::Recover {
                new_owner_id,
                ttl_secs,
            } => {
                if new_owner_id.is_empty()
                    || entry.lease_owner.as_deref() == Some(new_owner_id.as_str())
                {
                    return Err(QueueCallError::InvalidParams(
                        "lease recovery requires a different non-empty owner".to_string(),
                    ));
                }
                if entry.lease_is_live(now) {
                    return Ok(mutation_response(
                        QueueLeaseMutationOutcome::LeaseStillLive,
                        entry.execution_fence(),
                        None,
                    ));
                }
                entry.lease_owner = Some(new_owner_id);
                entry.lease_generation += 1;
                entry.lease_expires_at = Some(now + Duration::seconds(ttl_secs));
                let fence = entry.execution_fence();
                self.commit(&state, &[])?;
                Ok(mutation_response(
                    QueueLeaseMutationOutcome::Applied,
                    fence,
                    None,
                ))
            }
            FencedOperation::Renew { ttl_secs } => {
                if !entry.lease_is_live(now) {
                    return Ok(mutation_response(
                        QueueLeaseMutationOutcome::StaleFence,
                        None,
                        Some("queue lease has expired and requires recovery".to_string()),
                    ));
                }
                let renewed = now + Duration::seconds(ttl_secs);
                entry.lease_expires_at = entry.lease_expires_at.max(Some(renewed));
                let fence = entry.execution_fence();
                self.commit(&state, &[])?;
                Ok(mutation_response(
                    QueueLeaseMutationOutcome::Applied,
                    fence,
                    None,
                ))
            }
            // Difference 1: no expiry check. The exact match above already
            // proves nobody took the task over.
            FencedOperation::Complete { outcome } => {
                let done = state.entries.remove(index);
                let fence = done.execution_fence();
                let record = HistoryRecord::finished(&done, outcome, "queue/v2/completion", None);
                self.commit(&state, &[record])?;
                Ok(mutation_response(
                    QueueLeaseMutationOutcome::Applied,
                    fence,
                    None,
                ))
            }
            FencedOperation::ReleasePending { reason } => {
                entry.status = DispatchQueueEntryStatus::Pending;
                entry.assigned_at = None;
                entry.audit_log.push(DispatchQueueAuditEntry {
                    at: now.to_rfc3339(),
                    method: "queue/v2/release_pending".to_string(),
                    from_status: status::ASSIGNED.to_string(),
                    to_status: status::PENDING.to_string(),
                    reason,
                });
                self.commit(&state, &[])?;
                Ok(mutation_response(
                    QueueLeaseMutationOutcome::Applied,
                    None,
                    None,
                ))
            }
        }
    }

    /// The outcome for an entry that is no longer live. v0.2.9 keeps
    /// finished rows and answers from them; this queue answers from the
    /// history, taking the last record for the entry (the one that took
    /// effect).
    fn finished_entry_outcome(
        &self,
        entry_id: &str,
        execution: &ExecutionFence,
        operation: &FencedOperation,
    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
        let Some(record) = find_history_by_entry_id(self.project_root(), entry_id)? else {
            return Ok(mutation_response(
                QueueLeaseMutationOutcome::NotFound,
                None,
                None,
            ));
        };
        if matches!(operation, FencedOperation::Complete { .. })
            && record.outcome != HistoryOutcome::Dropped
            && exact_fence_matches(&record.entry, execution)
        {
            return Ok(mutation_response(
                QueueLeaseMutationOutcome::AlreadyApplied,
                record.entry.execution_fence(),
                None,
            ));
        }
        Ok(mutation_response(
            QueueLeaseMutationOutcome::NotAssigned,
            None,
            Some(format!("queue entry is {}", record.outcome.state_word())),
        ))
    }

    /// The original receipt for `key`, from the live file or, if the entry
    /// already finished, from the history. `swept` holds entries this call's
    /// expiry sweep just removed, which aren't in the history file yet (v0.2.9
    /// keeps them as `dropped` rows, so its lookup finds them). Errors when
    /// the key is bound to different content.
    fn idempotent_receipt(
        &self,
        state: &DispatchQueueState,
        swept: &[crate::queue_history::HistoryRecord],
        key: &str,
        request_hash: &str,
    ) -> Result<Option<QueueEnqueueV2Response>, QueueCallError> {
        let has_key = |entry: &DispatchQueueEntry| entry.bound_request_hash(key).is_some();
        let live = state
            .entries
            .iter()
            .find(|entry| has_key(entry))
            .or_else(|| {
                swept
                    .iter()
                    .map(|record| &record.entry)
                    .find(|entry| has_key(entry))
            })
            .cloned();
        let found = match live {
            Some(entry) => Some(entry),
            None => find_history_by_idempotency_key(self.project_root(), key)?
                .map(|record| record.entry),
        };
        let Some(entry) = found else {
            return Ok(None);
        };
        if entry.bound_request_hash(key) != Some(request_hash) {
            return Err(QueueCallError::InvalidParams(
                "idempotency_key is already bound to a different queue request".to_string(),
            ));
        }
        let (Some(qualified_id), Some(generation)) =
            (entry.subject_id.clone(), entry.subject_generation)
        else {
            return Err(QueueCallError::Backend(anyhow::anyhow!(
                "queue entry {} holds idempotency key {key} but no subject generation",
                entry.entry_id
            )));
        };
        Ok(Some(QueueEnqueueV2Response {
            enqueued: false,
            entry_id: entry.entry_id,
            subject: SubjectGeneration {
                qualified_id,
                generation,
            },
            warning: None,
        }))
    }
}

/// One of the four ticket calls, with its validated inputs.
enum FencedOperation {
    Renew { ttl_secs: i64 },
    Recover { new_owner_id: String, ttl_secs: i64 },
    Complete { outcome: HistoryOutcome },
    ReleasePending { reason: String },
}

fn mutation_response(
    outcome: QueueLeaseMutationOutcome,
    execution: Option<ExecutionFence>,
    reason: Option<String>,
) -> QueueLeaseMutationResponse {
    QueueLeaseMutationResponse {
        outcome,
        execution,
        reason,
    }
}

/// `true` when `execution` is the entry's current ticket (v0.2.9
/// `exactFenceMatches`). Matches the owner and every id and generation, and
/// the repository, but not the expiry time.
fn exact_fence_matches(entry: &DispatchQueueEntry, execution: &ExecutionFence) -> bool {
    let (Some(subject), Some(lease)) = (&execution.subject, &execution.queue_lease) else {
        return false;
    };
    execution.schema == EXECUTION_FENCE_SCHEMA_ID
        && execution.version == EXECUTION_FENCE_VERSION
        && entry.workflow_id.as_deref() == Some(execution.workflow_id.as_str())
        && entry.workflow_generation == Some(execution.workflow_generation)
        && entry.subject_id.as_deref() == Some(subject.qualified_id.as_str())
        && entry.subject_generation == Some(subject.generation)
        && entry.entry_id == lease.entry_id
        && entry.lease_owner.as_deref() == Some(lease.owner_id.as_str())
        && entry.lease_generation == lease.generation
        && same_repository(execution.repository.as_ref(), entry.repository.as_ref())
}

/// v0.2.9 `sameRepository`: both absent, or the same repository (ignoring
/// case) with the same base and head refs.
fn same_repository(
    left: Option<&RepositoryReservation>,
    right: Option<&RepositoryReservation>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.repository.to_lowercase() == right.repository.to_lowercase()
                && left.base_ref == right.base_ref
                && left.head_ref == right.head_ref
        }
        _ => false,
    }
}

fn lease_block(
    entry_id: String,
    reason: QueueLeaseBlockReason,
    conflicts_with: Option<ExecutionFence>,
) -> QueueLeaseBlock {
    QueueLeaseBlock {
        entry_id,
        reason,
        conflicts_with,
    }
}

/// The running entry holding the branch with `collision_key`, earliest
/// assigned first (v0.2.9 orders by `assigned_at`).
fn running_entry_on_branch(state: &DispatchQueueState, collision_key: &str) -> Option<usize> {
    state
        .entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            entry.status == DispatchQueueEntryStatus::Assigned
                && entry
                    .repository
                    .as_ref()
                    .is_some_and(|held| held.collision_key() == collision_key)
        })
        .min_by(|(_, left), (_, right)| left.assigned_at.cmp(&right.assigned_at))
        .map(|(index, _)| index)
}

/// The live entry that stops a new ticketed add for `qualified_id`
/// (difference 3): the ticketed entry with the highest generation, as
/// animus-queue-postgres v0.2.0 picks, else the first waiting or held
/// old-style entry for the same subject in queue order. A running old-style
/// entry never blocks: v0.2.9 gives old entries ticket identity only while
/// they wait or are held, and after the upgrade the 0.6 daemon that ran it is
/// refused, so returning it would swallow the add.
fn live_entry_for_subject(state: &DispatchQueueState, qualified_id: &str) -> Option<usize> {
    let is_live = |entry: &DispatchQueueEntry| entry.status != DispatchQueueEntryStatus::Unknown;
    state
        .entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            is_live(entry)
                && entry.is_ticketed()
                && entry.subject_id.as_deref() == Some(qualified_id)
        })
        .max_by_key(|(_, entry)| entry.subject_generation)
        .map(|(index, _)| index)
        .or_else(|| {
            state.entries.iter().position(|entry| {
                matches!(
                    entry.status,
                    DispatchQueueEntryStatus::Pending | DispatchQueueEntryStatus::Held
                ) && !entry.is_ticketed()
                    && entry
                        .dispatch
                        .as_ref()
                        .and_then(|dispatch| dispatch_canonical_id(dispatch).ok())
                        .as_deref()
                        == Some(qualified_id)
            })
        })
}

/// Make sure the entry at `index` has ticket identity and return it. An
/// old-style entry gets its canonical id and the subject's next generation
/// (difference 5: this happens when a ticketed call first needs it, not at
/// plugin start as in v0.2.9). Errors when the entry's subject can't be
/// identified.
pub(crate) fn ensure_ticket_identity(
    state: &mut DispatchQueueState,
    index: usize,
) -> Result<SubjectGeneration, String> {
    let entry = &state.entries[index];
    if let (Some(generation), Some(qualified_id)) =
        (entry.subject_generation, entry.subject_id.clone())
    {
        return Ok(SubjectGeneration {
            qualified_id,
            generation,
        });
    }
    let qualified_id = match entry.dispatch.as_ref() {
        Some(dispatch) => dispatch_canonical_id(dispatch)?,
        None => {
            return Err(format!(
                "queue entry {} has no dispatch to identify its subject",
                entry.entry_id
            ))
        }
    };
    let generation = next_subject_generation(state, &qualified_id);
    let entry = &mut state.entries[index];
    entry.subject_id = Some(qualified_id.clone());
    entry.subject_generation = Some(generation);
    Ok(SubjectGeneration {
        qualified_id,
        generation,
    })
}

/// Next generation for `qualified_id`: one more than the highest recorded or
/// live generation. Recorded, so it is never handed out again while the
/// counters exist.
fn next_subject_generation(state: &mut DispatchQueueState, qualified_id: &str) -> u64 {
    let recorded = state
        .subject_generations
        .get(qualified_id)
        .copied()
        .unwrap_or(0);
    let live = state
        .entries
        .iter()
        .filter(|entry| entry.subject_id.as_deref() == Some(qualified_id))
        .filter_map(|entry| entry.subject_generation)
        .max()
        .unwrap_or(0);
    let generation = recorded.max(live) + 1;
    state
        .subject_generations
        .insert(qualified_id.to_string(), generation);
    generation
}
