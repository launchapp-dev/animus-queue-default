//! Generation-fenced ("ticketed") queue calls: `queue/v2/*`.
//!
//! Behaviour follows animus-postgres v0.2.9 (`src/queue.ts`) except for the
//! seven differences listed in the design spec (§7.2). Each one is marked
//! where it applies.

use animus_execution_protocol::{RepositoryReservation, SubjectGeneration};
use animus_queue_protocol::{QueueEnqueueV2Request, QueueEnqueueV2Response};
use chrono::Utc;

use crate::dispatch_queue_state::{
    DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState, IdempotencyBinding,
};
use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state};
use crate::identity::{dispatch_canonical_id, dispatch_task_id};
use crate::queue_history::find_history_by_idempotency_key;
use crate::queue_service::{sweep_expired_entries, QueueBackend, QueueCallError};
use crate::request_hash::enqueue_request_hash;

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
