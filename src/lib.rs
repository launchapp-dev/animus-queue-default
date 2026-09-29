//! Animus default `queue` plugin: a file-backed dispatch queue with
//! generation-fenced ("ticketed") leases for the Animus 0.7 daemon, plus the
//! old-style `queue/*` methods. Behaviour follows animus-postgres v0.2.9
//! except for the seven differences in `docs/releases/v0.4.0.md`.
//!
//! Project root is bound at `initialize` time via the
//! `init_extensions.project_binding` extension; it is NOT a per-request
//! field. RPCs that imply a different project root than the bound one are
//! rejected with [`animus_queue_protocol::error_codes::PROJECT_BINDING_MISMATCH`].
//!
//! State and lock layout under the bound project root:
//!
//! ```text
//! <project_root>/.animus/queue.json            # live entries + counters (atomic replace)
//! <project_root>/.animus/queue.lock            # fs2 exclusive-lock file
//! <project_root>/.animus/queue-history.jsonl   # finished entries (append + fsync)
//! ```
//!
//! The lock is held only across read-modify-write cycles, never across
//! IPC.

#![warn(missing_docs)]

pub mod dispatch_queue_state;
pub mod dispatch_queue_store;
pub mod fenced_queue;
pub mod host_guard;
pub mod identity;
pub mod lease_ttl;
pub mod plugin;
pub mod queue_history;
pub mod queue_service;
pub mod request_hash;

pub use dispatch_queue_state::{
    DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
    IdempotencyBinding,
};
pub use dispatch_queue_store::{
    load_queue_state, queue_lock_path, queue_state_path, save_queue_state,
};
pub use queue_service::QueueBackend;
