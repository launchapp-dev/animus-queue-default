//! Integration tests for the new `queue/lease` atomic dispatch path.

use animus_queue_default::queue_service::{QueueBackend, QueueLeaseError};
use animus_subject_protocol::{SubjectDispatch, SubjectRef};
use chrono::Utc;

fn task_dispatch(task_id: &str, workflow_ref: &str) -> SubjectDispatch {
    SubjectDispatch::for_subject_with_metadata(
        SubjectRef::task(task_id),
        workflow_ref,
        "manual-queue-enqueue",
        Utc::now(),
    )
}

#[test]
fn lease_returns_multiple_entries_atomically() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let first = backend
        .enqueue(task_dispatch("TASK-1", "standard"))
        .expect("enqueue 1");
    let second = backend
        .enqueue(task_dispatch("TASK-2", "standard"))
        .expect("enqueue 2");
    let _third = backend
        .enqueue(task_dispatch("TASK-3", "standard"))
        .expect("enqueue 3");

    let workflow_ids = vec!["wf-aaa".to_string(), "wf-bbb".to_string()];
    let leased = backend
        .lease(2, Some(workflow_ids.clone()))
        .expect("lease 2");

    assert_eq!(leased.leased.len(), 2, "should lease exactly 2 entries");
    assert_eq!(leased.leased[0].entry_id, first.entry_id);
    assert_eq!(leased.leased[0].workflow_id.as_deref(), Some("wf-aaa"));
    assert_eq!(leased.leased[0].status, "assigned");
    assert!(leased.leased[0].assigned_at.is_some());

    assert_eq!(leased.leased[1].entry_id, second.entry_id);
    assert_eq!(leased.leased[1].workflow_id.as_deref(), Some("wf-bbb"));
    assert_eq!(leased.leased[1].status, "assigned");

    // Persisted: a re-list should reflect 2 assigned + 1 pending.
    let listing = backend.list(&[], None, None).expect("list");
    assert_eq!(listing.stats.total, 3);
    assert_eq!(listing.stats.assigned, 2);
    assert_eq!(listing.stats.pending, 1);

    // Second lease pulls TASK-3 only — earlier entries are now Assigned.
    let next = backend.lease(2, None).expect("lease 2 again");
    assert_eq!(next.leased.len(), 1);
    assert_eq!(next.leased[0].subject_id, "TASK-3");
}

#[test]
fn lease_workflow_id_count_mismatch_returns_typed_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    backend
        .enqueue(task_dispatch("TASK-1", "standard"))
        .expect("enqueue");

    let result = backend.lease(2, Some(vec!["wf-only-one".to_string()]));
    match result {
        Err(QueueLeaseError::WorkflowIdCountMismatch { expected, actual }) => {
            assert_eq!(expected, 2);
            assert_eq!(actual, 1);
        }
        other => panic!("expected WorkflowIdCountMismatch, got {other:?}"),
    }

    // Verify the queue state was NOT mutated by the failed lease.
    let listing = backend.list(&[], None, None).expect("list");
    assert_eq!(listing.stats.total, 1);
    assert_eq!(listing.stats.pending, 1);
    assert_eq!(listing.stats.assigned, 0);
}

#[test]
fn lease_synthesizes_workflow_ids_when_omitted() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    backend
        .enqueue(task_dispatch("TASK-1", "standard"))
        .expect("enqueue");

    let leased = backend.lease(1, None).expect("lease");
    assert_eq!(leased.leased.len(), 1);
    let workflow_id = leased.leased[0]
        .workflow_id
        .as_deref()
        .expect("synthetic workflow_id");
    // UUID v4 is 36 chars.
    assert_eq!(
        workflow_id.len(),
        36,
        "expected UUID v4-shaped workflow id, got {workflow_id}"
    );
}

#[test]
fn completion_does_not_drop_pending_or_held_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let pending = backend
        .enqueue(task_dispatch("TASK-PENDING", "standard"))
        .expect("enqueue pending");
    let to_hold = backend
        .enqueue(task_dispatch("TASK-HELD", "standard"))
        .expect("enqueue held");
    backend.hold(&to_hold.entry_id).expect("hold");

    // A stale completion frame against a still-Pending entry must be a no-op
    // (not_found=true, changed=false) and must not remove queued work.
    let pending_completion = backend
        .completion(&pending.entry_id, "completed", None, None)
        .expect("completion pending");
    assert!(!pending_completion.changed);
    assert!(pending_completion.not_found);

    let held_completion = backend
        .completion(&to_hold.entry_id, "failed", None, None)
        .expect("completion held");
    assert!(!held_completion.changed);
    assert!(held_completion.not_found);

    let listing = backend.list(&[], None, None).expect("list");
    assert_eq!(listing.stats.total, 2);
    assert_eq!(listing.stats.pending, 1);
    assert_eq!(listing.stats.held, 1);
}

#[test]
fn completion_prunes_assigned_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    backend
        .enqueue(task_dispatch("TASK-1", "standard"))
        .expect("enqueue");
    let leased = backend
        .lease(1, Some(vec!["wf-1".to_string()]))
        .expect("lease");
    let entry_id = leased.leased[0].entry_id.clone();

    let outcome = backend
        .completion(&entry_id, "completed", Some("standard"), Some("wf-1"))
        .expect("completion");
    assert!(outcome.changed);
    assert!(!outcome.not_found);

    let listing = backend.list(&[], None, None).expect("list");
    assert_eq!(listing.stats.total, 0);
}

#[test]
fn legacy_queue_state_without_entry_ids_migrates_to_stable_ids() {
    // Simulate the in-tree v0.4 queue state format (no `entry_id` field) on
    // disk and verify that two consecutive loads produce the same ids.
    let temp = tempfile::tempdir().expect("tempdir");
    let project_root = temp.path().to_path_buf();
    let animus_dir = project_root.join(".animus");
    std::fs::create_dir_all(&animus_dir).expect("animus dir");
    let queue_json = r#"{
      "entries": [
        {
          "subject_id": "TASK-LEGACY-1",
          "task_id": "TASK-LEGACY-1",
          "dispatch": {
            "subject": { "kind": "task", "id": "TASK-LEGACY-1" },
            "workflow_ref": "standard",
            "trigger_source": "legacy",
            "requested_at": "2026-05-30T00:00:00Z"
          },
          "status": "pending"
        }
      ]
    }"#;
    std::fs::write(animus_dir.join("queue.json"), queue_json).expect("write legacy state");

    let backend = QueueBackend::new(project_root);
    let first = backend.list(&[], None, None).expect("first list");
    assert_eq!(first.entries.len(), 1);
    let first_id = first.entries[0].entry_id.clone();
    assert!(!first_id.is_empty(), "migration should mint a non-empty id");

    let second = backend.list(&[], None, None).expect("second list");
    assert_eq!(second.entries.len(), 1);
    assert_eq!(
        second.entries[0].entry_id, first_id,
        "migrated entry id must be persisted so subsequent calls see the same id"
    );

    // And the migrated id must be addressable by a mutation.
    let mutated = backend.hold(&first_id).expect("hold migrated entry");
    assert!(mutated.changed);
}

#[test]
fn lease_skips_held_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let one = backend
        .enqueue(task_dispatch("TASK-1", "standard"))
        .expect("enqueue 1");
    let two = backend
        .enqueue(task_dispatch("TASK-2", "standard"))
        .expect("enqueue 2");

    backend.hold(&one.entry_id).expect("hold first");

    let leased = backend.lease(5, None).expect("lease 5");
    assert_eq!(leased.leased.len(), 1);
    assert_eq!(leased.leased[0].entry_id, two.entry_id);
}
