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
    /// stable `entry_id` and capturing `enqueued_at`.
    pub fn from_dispatch(dispatch: SubjectDispatch) -> Self {
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
            audit_log: Vec::new(),
        }
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
