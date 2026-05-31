//! Animus reference `queue` plugin (lift-and-shift of the in-tree dispatch
//! queue + the v0.5 atomic `queue/lease` path).
//!
//! Project root is bound at `initialize` time via the
//! `init_extensions.project_binding` extension; it is NOT a per-request
//! field. RPCs that imply a different project root than the bound one are
//! rejected with [`animus_queue_protocol::error_codes::PROJECT_BINDING_MISMATCH`].
//!
//! State and lock layout under the bound project root:
//!
//! ```text
//! <project_root>/.animus/queue.json   # JSON state (atomic-replace on write)
//! <project_root>/.animus/queue.lock   # fs2 exclusive-lock file
//! ```
//!
//! The lock is held only across read-modify-write cycles, never across
//! IPC.

#![warn(missing_docs)]

pub mod dispatch_queue_state;
pub mod dispatch_queue_store;
pub mod plugin;
pub mod queue_service;

pub use dispatch_queue_state::{DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState};
pub use dispatch_queue_store::{
    load_queue_state, queue_lock_path, queue_state_path, save_queue_state,
};
pub use queue_service::QueueBackend;
