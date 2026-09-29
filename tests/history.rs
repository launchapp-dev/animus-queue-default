//! Finished entries move from `queue.json` to `queue-history.jsonl`.

mod common;

use animus_queue_default::queue_history::{find_history_by_entry_id, HistoryOutcome};
use animus_queue_default::{queue_state_path, QueueBackend};
use chrono::Utc;
use common::task_dispatch;

#[test]
fn completion_moves_the_entry_to_history() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    backend
        .lease(1, Some(vec!["wf-1".to_string()]), None)
        .expect("lease");

    let outcome = backend
        .completion(&enqueued.entry_id, "failed", None, Some("wf-1"))
        .expect("completion");

    assert!(outcome.changed);
    let record = find_history_by_entry_id(temp.path(), &enqueued.entry_id)
        .expect("read history")
        .expect("history record");
    assert_eq!(record.outcome, HistoryOutcome::Failed);
    assert_eq!(record.finished_by, "queue/completion");
    assert_eq!(record.entry.workflow_id.as_deref(), Some("wf-1"));
    assert_eq!(backend.list(&[], None, None).expect("list").total, 0);
    assert!(
        queue_state_path(temp.path()).exists(),
        "queue.json stays even when empty"
    );
}

#[test]
fn drop_moves_the_entry_to_history() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");

    assert!(
        backend
            .drop_entry(&enqueued.entry_id)
            .expect("drop")
            .changed
    );

    let record = find_history_by_entry_id(temp.path(), &enqueued.entry_id)
        .expect("read history")
        .expect("history record");
    assert_eq!(record.outcome, HistoryOutcome::Dropped);
    assert_eq!(record.finished_by, "queue/drop");
}

#[test]
fn expired_deferred_entries_are_recorded_as_dropped() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let run_at = (Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), Some(run_at), Some(60))
        .expect("enqueue");

    assert!(backend
        .lease(5, None, None)
        .expect("lease")
        .leased
        .is_empty());

    let record = find_history_by_entry_id(temp.path(), &enqueued.entry_id)
        .expect("read history")
        .expect("history record");
    assert_eq!(record.outcome, HistoryOutcome::Dropped);
    assert_eq!(record.finished_by, "expiry-sweep");
    assert!(record.reason.is_some());
}
