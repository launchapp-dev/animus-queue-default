//! In-memory queue state shapes (also the on-disk persistence format).

use animus_subject_protocol::SubjectDispatch;
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// One row in [`DispatchQueueEntry::audit_log`]. Captures who/why a
/// reason-carrying state transition happened.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DispatchQueueState {
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
            subject_id: Some(dispatch.subject_key()),
            task_id: dispatch.task_id().unwrap_or_default().to_string(),
            dispatch: Some(dispatch),
            status: DispatchQueueEntryStatus::Pending,
            workflow_id: None,
            enqueued_at: Some(chrono::Utc::now().to_rfc3339()),
            assigned_at: None,
            held_at: None,
            run_at,
            expire_after_secs,
            audit_log: Vec::new(),
        }
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
        if let Some(dispatch) = &self.dispatch {
            return dispatch.subject_id();
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
