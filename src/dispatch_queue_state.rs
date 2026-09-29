//! In-memory queue state shapes (also the on-disk persistence format).
//!
//! Compatibility rule: queue v0.3.3 must still read files written here (the
//! documented rollback path). So fields are only ever added, `task_id` stays
//! a required string, and the status words stay `pending`, `assigned` and
//! `held`. v0.3.3 ignores the fields it doesn't know.

use std::collections::BTreeMap;

use animus_execution_protocol::{
    ExecutionFence, QueueLeaseFence, RepositoryReservation, SubjectGeneration,
    EXECUTION_FENCE_SCHEMA_ID, EXECUTION_FENCE_VERSION,
};
use animus_subject_protocol::SubjectDispatch;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Format version written to `queue.json`. Files without the marker (queue
/// v0.3.3 and older) load as version 0.
pub const QUEUE_FORMAT_VERSION: u32 = 2;

/// Entry status. Wire form matches `animus-queue-protocol::status::*` strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DispatchQueueEntryStatus {
    /// Waiting to be leased.
    #[default]
    Pending,
    /// Leased; a workflow is running against it.
    Assigned,
    /// Held by operator action; non-dispatchable.
    Held,
    /// Forward-compat fallthrough for unknown wire values.
    #[serde(other)]
    Unknown,
}

impl DispatchQueueEntryStatus {
    /// Wire string value matching `animus_queue_protocol::status::*`.
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Pending => animus_queue_protocol::status::PENDING,
            Self::Assigned => animus_queue_protocol::status::ASSIGNED,
            Self::Held => animus_queue_protocol::status::HELD,
            // Unknown is wire-only — never returned to clients as a status.
            Self::Unknown => "unknown",
        }
    }
}

/// One queue entry. Keyed by `entry_id` (a UUID v4 string assigned on
/// enqueue) for all mutation calls.
///
/// An entry is *ticketed* once it has a `subject_generation`: it was added by
/// `queue/v2/enqueue`, or it is an old-style entry that got ticket identity
/// at its first `queue/v2/lease` hand-out. Ticketed entries store their
/// canonical `<kind>:<id>` in `subject_id`; old-style entries store the
/// legacy subject key there.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DispatchQueueEntry {
    /// Stable entry id (UUID v4) assigned on enqueue. Defaulted to the empty
    /// string on deserialization so legacy in-tree queue state (which did
    /// not carry `entry_id`) is detectable: `load_queue_state` walks the
    /// loaded entries, mints a fresh UUID for each empty slot, and persists
    /// the migrated file back to disk so subsequent calls see the same ids.
    #[serde(default)]
    pub entry_id: String,
    /// Cached subject id from the dispatch envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<String>,
    /// Task id (when the subject is a built-in task). Kept as a non-Option
    /// String for back-compat with the in-tree shape.
    pub task_id: String,
    /// Full dispatch envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<SubjectDispatch>,
    /// Current status.
    #[serde(default)]
    pub status: DispatchQueueEntryStatus,
    /// Attached workflow id (when Assigned).
    #[serde(default)]
    pub workflow_id: Option<String>,
    /// RFC 3339 enqueue timestamp.
    #[serde(default)]
    pub enqueued_at: Option<String>,
    /// RFC 3339 assignment timestamp.
    #[serde(default)]
    pub assigned_at: Option<String>,
    /// RFC 3339 hold timestamp.
    #[serde(default)]
    pub held_at: Option<String>,
    /// RFC 3339 earliest-dispatch time for a deferred entry. While `now` is
    /// before this instant the entry stays Pending but is excluded from
    /// `queue/lease`. `None` for ordinary (dispatch-ASAP) entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<String>,
    /// Grace window in seconds after `run_at` before a still-pending deferred
    /// entry is expired and dropped on sweep. `None` = never expire. Ignored
    /// when `run_at` is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_secs: Option<u64>,
    /// Audit log of state transitions recorded by reason-carrying mutations
    /// (currently `queue/release_pending`). Older transitions remain absent
    /// because legacy mutations did not record reasons.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_log: Vec<DispatchQueueAuditEntry>,
    /// Immutable subject generation. Set on ticketed entries only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_generation: Option<u64>,
    /// Workflow generation. Set to 1 at the first ticketed hand-out and kept
    /// through put-back and takeover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_generation: Option<u64>,
    /// Current lease holder: a daemon's owner id for ticketed leases, the
    /// workflow id for old-style leases (as v0.2.9 does).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_owner: Option<String>,
    /// Lease generation. Rises by one at every ticketed hand-out and
    /// takeover. 0 means never ticket-leased.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub lease_generation: u64,
    /// Lease expiry. Old-style leases get one too, but only old-style
    /// hand-outs by this version set it (v0.3.3 leases have none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// Repository and branch reserved by this entry (ticketed entries only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryReservation>,
    /// Producer idempotency key from `queue/v2/enqueue`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Content hash the idempotency key is bound to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_hash: Option<String>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// One row in [`DispatchQueueEntry::audit_log`]. Captures who/why a
/// reason-carrying state transition happened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DispatchQueueAuditEntry {
    /// RFC 3339 timestamp of the transition.
    pub at: String,
    /// JSON-RPC method that caused the transition (e.g. `queue/release_pending`).
    pub method: String,
    /// Status the entry held before the transition (wire form).
    pub from_status: String,
    /// Status the entry holds after the transition (wire form).
    pub to_status: String,
    /// Caller-supplied audit reason.
    pub reason: String,
}

/// On-disk top-level state shape.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct DispatchQueueState {
    /// File format version; see [`QUEUE_FORMAT_VERSION`]. 0 for files
    /// written by queue v0.3.3 and older.
    #[serde(default)]
    pub format_version: u32,
    /// Highest subject generation handed out per canonical subject id. Never
    /// decreases, and survives the queue emptying, so a finished task's next
    /// run gets a higher generation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub subject_generations: BTreeMap<String, u64>,
    /// Queue entries in priority/FIFO order.
    #[serde(default)]
    pub entries: Vec<DispatchQueueEntry>,
}

impl DispatchQueueEntry {
    /// Build a fresh Pending entry from a `SubjectDispatch`, assigning a
    /// stable `entry_id` and capturing `enqueued_at`. `run_at` /
    /// `expire_after_secs` carry deferred-dispatch metadata (both `None`
    /// for an ordinary dispatch-ASAP entry).
    pub fn from_dispatch(
        dispatch: SubjectDispatch,
        run_at: Option<String>,
        expire_after_secs: Option<u64>,
    ) -> Self {
        Self {
            entry_id: uuid::Uuid::new_v4().to_string(),
            subject_id: dispatch.subject_key(),
            task_id: dispatch.task_id().unwrap_or_default().to_string(),
            dispatch: Some(dispatch),
            status: DispatchQueueEntryStatus::Pending,
            enqueued_at: Some(chrono::Utc::now().to_rfc3339()),
            run_at,
            expire_after_secs,
            ..Self::default()
        }
    }

    /// `true` once the entry has ticket identity.
    pub fn is_ticketed(&self) -> bool {
        self.subject_generation.is_some()
    }

    /// `true` while the lease expiry is in the future.
    pub fn lease_is_live(&self, now: DateTime<Utc>) -> bool {
        self.lease_expires_at
            .is_some_and(|expires_at| expires_at > now)
    }

    /// The entry's execution fence, or `None` when any part of its ticket
    /// identity is missing (v0.2.9 `executionFromRow`).
    pub fn execution_fence(&self) -> Option<ExecutionFence> {
        let qualified_id = self.subject_id.clone().filter(|id| !id.is_empty())?;
        let subject_generation = self.subject_generation.filter(|g| *g > 0)?;
        let workflow_id = self.workflow_id.clone().filter(|id| !id.is_empty())?;
        let workflow_generation = self.workflow_generation.filter(|g| *g > 0)?;
        let owner_id = self.lease_owner.clone().filter(|id| !id.is_empty())?;
        let expires_at = self.lease_expires_at?;
        if self.lease_generation == 0 {
            return None;
        }
        Some(ExecutionFence {
            schema: EXECUTION_FENCE_SCHEMA_ID.to_string(),
            version: EXECUTION_FENCE_VERSION,
            workflow_id,
            workflow_generation,
            subject: Some(SubjectGeneration {
                qualified_id,
                generation: subject_generation,
            }),
            queue_lease: Some(QueueLeaseFence {
                entry_id: self.entry_id.clone(),
                owner_id,
                generation: self.lease_generation,
                expires_at,
            }),
            repository: self.repository.clone(),
        })
    }

    /// `true` when this entry is deferred and its `run_at` instant has not
    /// yet been reached as of `now`. Unparseable `run_at` values are treated
    /// as eligible (dispatch now) so a malformed timestamp never wedges an
    /// entry permanently.
    pub fn is_deferred_until_future(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        match self.parsed_run_at() {
            Some(run_at) => now < run_at,
            None => false,
        }
    }

    /// Parse `run_at` into a UTC instant, if present and well-formed.
    pub fn parsed_run_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let raw = self.run_at.as_deref()?;
        match chrono::DateTime::parse_from_rfc3339(raw) {
            Ok(dt) => Some(dt.with_timezone(&chrono::Utc)),
            Err(err) => {
                tracing::warn!(
                    entry_id = %self.entry_id,
                    run_at = raw,
                    error = %err,
                    "queue entry has unparseable run_at; treating as immediately eligible"
                );
                None
            }
        }
    }

    /// The instant at which a deferred entry should be expired (dropped
    /// instead of dispatched late): `run_at + expire_after_secs`. `None`
    /// when the entry is not deferred or has no expiry window.
    pub fn expiry_deadline(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let run_at = self.parsed_run_at()?;
        let secs = self.expire_after_secs?;
        Some(run_at + chrono::Duration::seconds(secs as i64))
    }

    /// Effective subject id (falls back to `dispatch.subject_id` then to
    /// `task_id`).
    pub fn subject_id_ref(&self) -> &str {
        if let Some(subject_id) = self
            .subject_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return subject_id;
        }
        if let Some(subject_id) = self.dispatch.as_ref().and_then(SubjectDispatch::subject_id) {
            return subject_id;
        }
        self.task_id.as_str()
    }

    /// Effective task id (None when this entry's subject is not a built-in
    /// task).
    pub fn task_id_ref(&self) -> Option<&str> {
        self.dispatch
            .as_ref()
            .and_then(SubjectDispatch::task_id)
            .or_else(|| (!self.task_id.trim().is_empty()).then_some(self.task_id.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_subject_protocol::SubjectRef;

    fn ticketed_entry() -> DispatchQueueEntry {
        DispatchQueueEntry {
            entry_id: "entry-1".to_string(),
            subject_id: Some("task:TASK-1".to_string()),
            task_id: "TASK-1".to_string(),
            dispatch: Some(SubjectDispatch::for_subject_with_metadata(
                SubjectRef::task("TASK-1"),
                "coding",
                "test",
                Utc::now(),
            )),
            status: DispatchQueueEntryStatus::Assigned,
            workflow_id: Some("workflow-1".to_string()),
            subject_generation: Some(3),
            workflow_generation: Some(1),
            lease_owner: Some("daemon-a".to_string()),
            lease_generation: 2,
            lease_expires_at: Some("2030-01-01T00:00:00Z".parse().unwrap()),
            repository: Some(RepositoryReservation {
                repository: "https://github.com/launchapp-dev/animus-cli.git".to_string(),
                base_ref: "refs/heads/main".to_string(),
                head_ref: "refs/heads/animus/TASK-1".to_string(),
            }),
            ..DispatchQueueEntry::default()
        }
    }

    #[test]
    fn execution_fence_carries_the_full_ticket() {
        let entry = ticketed_entry();
        let fence = entry.execution_fence().expect("complete identity");
        fence.validate_coding().expect("valid coding fence");
        assert_eq!(fence.workflow_id, "workflow-1");
        assert_eq!(fence.workflow_generation, 1);
        let subject = fence.subject.as_ref().unwrap();
        assert_eq!(subject.qualified_id, "task:TASK-1");
        assert_eq!(subject.generation, 3);
        let lease = fence.queue_lease.as_ref().unwrap();
        assert_eq!(lease.entry_id, "entry-1");
        assert_eq!(lease.owner_id, "daemon-a");
        assert_eq!(lease.generation, 2);
        assert_eq!(fence.repository, entry.repository);
    }

    #[test]
    fn execution_fence_needs_every_identity_part() {
        let mut missing_owner = ticketed_entry();
        missing_owner.lease_owner = None;
        assert!(missing_owner.execution_fence().is_none());

        let mut never_leased = ticketed_entry();
        never_leased.lease_generation = 0;
        assert!(never_leased.execution_fence().is_none());

        let mut old_style = ticketed_entry();
        old_style.subject_generation = None;
        assert!(!old_style.is_ticketed());
        assert!(old_style.execution_fence().is_none());
    }

    #[test]
    fn lease_liveness_follows_expiry() {
        let mut entry = ticketed_entry();
        let now = Utc::now();
        entry.lease_expires_at = Some(now + chrono::Duration::seconds(5));
        assert!(entry.lease_is_live(now));
        entry.lease_expires_at = Some(now - chrono::Duration::seconds(5));
        assert!(!entry.lease_is_live(now));
        entry.lease_expires_at = None;
        assert!(!entry.lease_is_live(now));
    }

    #[test]
    fn old_style_entries_serialize_without_ticket_fields() {
        let entry = DispatchQueueEntry::from_dispatch(
            SubjectDispatch::for_subject_with_metadata(
                SubjectRef::task("TASK-2"),
                "standard",
                "test",
                Utc::now(),
            ),
            None,
            None,
        );
        let value = serde_json::to_value(&entry).unwrap();
        for field in [
            "subject_generation",
            "workflow_generation",
            "lease_owner",
            "lease_generation",
            "lease_expires_at",
            "repository",
            "idempotency_key",
            "request_hash",
        ] {
            assert!(value.get(field).is_none(), "{field} must be omitted");
        }
        // v0.3.3 requires these.
        assert_eq!(value["task_id"], "TASK-2");
        assert_eq!(value["status"], "pending");
    }

    #[test]
    fn ticket_fields_round_trip() {
        let entry = ticketed_entry();
        let text = serde_json::to_string(&entry).unwrap();
        let back: DispatchQueueEntry = serde_json::from_str(&text).unwrap();
        assert_eq!(back, entry);
    }
}
