//! Old-style `queue/*` calls behave like animus-postgres v0.2.9, except that
//! they leave ticketed entries alone (difference 4).

mod common;

use animus_queue_default::queue_service::{QueueMutationError, QueueReleasePendingError};
use animus_queue_default::{DispatchQueueEntry, DispatchQueueEntryStatus, QueueBackend};
use animus_subject_protocol::SubjectDispatch;
use chrono::Utc;
use common::{edit_state, entry_mut, expire_lease, read_entry, task_dispatch, PluginProcess};
use serde_json::json;

fn backend(temp: &tempfile::TempDir) -> QueueBackend {
    QueueBackend::new(temp.path().to_path_buf())
}

/// Give an old-style entry ticket identity, as a ticketed hand-out would.
fn make_ticketed(temp: &tempfile::TempDir, entry_id: &str, task_id: &str) {
    edit_state(temp.path(), |state| {
        let entry = entry_mut(state, entry_id);
        entry.subject_id = Some(format!("task:{task_id}"));
        entry.subject_generation = Some(1);
    });
}

#[test]
fn lease_keeps_an_existing_workflow_id_across_five_slots() {
    // Port of v0.2.9 "lease keeps one daemon-selected id through five-slot
    // assignment and expired reclaim".
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let ids: Vec<String> = (1..=5)
        .map(|n| {
            backend
                .enqueue(task_dispatch(&format!("TASK-{n}"), "standard"), None, None)
                .expect("enqueue")
                .entry_id
        })
        .collect();
    edit_state(temp.path(), |state| {
        let third = entry_mut(state, &ids[2]);
        third.status = DispatchQueueEntryStatus::Assigned;
        third.workflow_id = Some("workflow-before-restart".to_string());
        third.lease_owner = Some("workflow-before-restart".to_string());
        third.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(60));
    });
    let proposed: Vec<String> = (1..=5).map(|n| format!("daemon-workflow-{n}")).collect();

    let leased = backend.lease(5, Some(proposed), None).expect("lease");

    let workflow_ids: Vec<&str> = leased
        .leased
        .iter()
        .map(|entry| entry.workflow_id.as_deref().unwrap())
        .collect();
    assert_eq!(
        workflow_ids,
        [
            "daemon-workflow-1",
            "daemon-workflow-2",
            "workflow-before-restart",
            "daemon-workflow-4",
            "daemon-workflow-5"
        ]
    );
}

#[test]
fn expired_old_style_lease_is_handed_out_again_unless_excluded() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    backend
        .lease(1, Some(vec!["wf-1".to_string()]), None)
        .expect("lease");
    expire_lease(temp.path(), &enqueued.entry_id);

    let excluded = backend
        .lease(1, None, Some(vec!["TASK-1".to_string()]))
        .expect("lease with exclude");
    assert!(excluded.leased.is_empty());

    let reclaimed = backend.lease(1, None, None).expect("lease again");
    assert_eq!(reclaimed.leased.len(), 1);
    assert_eq!(reclaimed.leased[0].entry_id, enqueued.entry_id);
    assert_eq!(reclaimed.leased[0].workflow_id.as_deref(), Some("wf-1"));
}

#[test]
fn live_old_style_lease_is_not_handed_out_again() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    backend.lease(1, None, None).expect("lease");

    assert!(backend
        .lease(1, None, None)
        .expect("lease again")
        .leased
        .is_empty());
}

#[test]
fn lease_hands_out_one_entry_per_subject_per_call() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    for _ in 0..2 {
        backend
            .enqueue(task_dispatch("TASK-1", "standard"), None, None)
            .expect("enqueue");
    }

    let leased = backend.lease(5, None, None).expect("lease");

    assert_eq!(leased.leased.len(), 1);
    let stats = backend.stats().expect("stats");
    assert_eq!((stats.pending, stats.assigned), (1, 1));
}

#[test]
fn lease_sets_the_owner_and_an_expiry_from_the_ttl() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp).with_lease_ttl(120);
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    let before = Utc::now();

    backend
        .lease(1, Some(vec!["wf-1".to_string()]), None)
        .expect("lease");

    let entry = read_entry(temp.path(), &enqueued.entry_id);
    assert_eq!(entry.lease_owner.as_deref(), Some("wf-1"));
    let expires_at = entry.lease_expires_at.expect("expiry");
    assert!(expires_at >= before + chrono::Duration::seconds(120));
    assert!(expires_at <= Utc::now() + chrono::Duration::seconds(120));
}

#[test]
fn mark_assigned_replaces_an_old_workflow_id() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    edit_state(temp.path(), |state| {
        entry_mut(state, &enqueued.entry_id).workflow_id = Some("stale-id".to_string());
    });

    assert!(
        backend
            .mark_assigned(&enqueued.entry_id, None)
            .expect("mark")
            .changed
    );

    let entry = read_entry(temp.path(), &enqueued.entry_id);
    let workflow_id = entry.workflow_id.clone().expect("workflow id");
    assert_ne!(workflow_id, "stale-id");
    assert_eq!(
        workflow_id.len(),
        36,
        "fresh UUID expected, got {workflow_id}"
    );
    assert_eq!(entry.lease_owner, entry.workflow_id);
    assert!(entry.lease_expires_at.is_some());
}

#[test]
fn release_pending_clears_the_lease() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    backend.lease(1, None, None).expect("lease");

    backend
        .release_pending(&enqueued.entry_id, "operator-cancel")
        .expect("release_pending");

    let entry = read_entry(temp.path(), &enqueued.entry_id);
    assert_eq!(entry.status, DispatchQueueEntryStatus::Pending);
    assert!(entry.workflow_id.is_none());
    assert!(entry.lease_owner.is_none());
    assert!(entry.lease_expires_at.is_none());
}

#[test]
fn unreadable_run_at_dispatches_now() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    backend
        .enqueue(
            task_dispatch("TASK-1", "standard"),
            Some("tomorrow morning".to_string()),
            None,
        )
        .expect("enqueue");

    let listed = backend.list(&[], None, None).expect("list");
    assert!(listed.entries[0].run_at.is_none());
    assert_eq!(backend.lease(1, None, None).expect("lease").leased.len(), 1);
}

#[test]
fn list_pages_default_to_500_and_cap_at_2000() {
    let temp = tempfile::tempdir().expect("tempdir");
    edit_state(temp.path(), |state| {
        for n in 0..2001 {
            state.entries.push(DispatchQueueEntry::from_dispatch(
                task_dispatch(&format!("TASK-{n}"), "standard"),
                None,
                None,
            ));
        }
    });
    let backend = backend(&temp);

    let default_page = backend.list(&[], None, None).expect("list");
    assert_eq!(default_page.entries.len(), 500);
    assert_eq!(default_page.total, 2001);
    assert_eq!(
        backend
            .list(&[], Some(5000), None)
            .expect("list")
            .entries
            .len(),
        2000
    );
    assert_eq!(
        backend
            .list(&[], Some(0), None)
            .expect("list")
            .entries
            .len(),
        1
    );
}

#[test]
fn generic_task_kind_reports_a_task_id() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let dispatch: SubjectDispatch = serde_json::from_value(json!({
        "subject": { "kind": "task", "id": "TASK-7" },
        "workflow_ref": "standard",
        "trigger_source": "test",
        "requested_at": "2026-09-28T00:00:00Z"
    }))
    .expect("dispatch");

    let enqueued = backend.enqueue(dispatch, None, None).expect("enqueue");

    assert_eq!(enqueued.subject_id, "TASK-7");
    let listed = backend.list(&[], None, None).expect("list");
    assert_eq!(listed.entries[0].subject_id, "TASK-7");
    assert_eq!(listed.entries[0].task_id.as_deref(), Some("TASK-7"));
}

#[test]
fn old_style_hand_out_mark_and_put_back_leave_ticketed_entries_alone() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let waiting = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue 1");
    let running = backend
        .enqueue(task_dispatch("TASK-2", "standard"), None, None)
        .expect("enqueue 2");
    make_ticketed(&temp, &waiting.entry_id, "TASK-1");
    make_ticketed(&temp, &running.entry_id, "TASK-2");
    edit_state(temp.path(), |state| {
        let entry = entry_mut(state, &running.entry_id);
        entry.status = DispatchQueueEntryStatus::Assigned;
        entry.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(60));
    });

    // Neither the waiting entry nor the expired running one is handed out.
    assert!(backend
        .lease(5, None, None)
        .expect("lease")
        .leased
        .is_empty());
    assert!(matches!(
        backend.mark_assigned(&waiting.entry_id, None),
        Err(QueueMutationError::Fenced { .. })
    ));
    assert!(matches!(
        backend.release_pending(&running.entry_id, "operator"),
        Err(QueueReleasePendingError::Fenced { .. })
    ));
    assert_eq!(
        read_entry(temp.path(), &running.entry_id).status,
        DispatchQueueEntryStatus::Assigned
    );
}

#[test]
fn hold_release_drop_and_completion_still_work_on_ticketed_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let waiting = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue 1");
    let running = backend
        .enqueue(task_dispatch("TASK-2", "standard"), None, None)
        .expect("enqueue 2");
    make_ticketed(&temp, &waiting.entry_id, "TASK-1");
    make_ticketed(&temp, &running.entry_id, "TASK-2");
    edit_state(temp.path(), |state| {
        let entry = entry_mut(state, &running.entry_id);
        entry.status = DispatchQueueEntryStatus::Assigned;
        entry.workflow_id = Some("wf-2".to_string());
    });

    assert!(backend.hold(&waiting.entry_id).expect("hold").changed);
    assert!(backend.release(&waiting.entry_id).expect("release").changed);
    // v0.2.9: old-style "done" is accepted for any entry (spec §7.3).
    assert!(
        backend
            .completion(&running.entry_id, "completed", None, None)
            .expect("completion")
            .changed
    );
    assert!(backend.drop_entry(&waiting.entry_id).expect("drop").changed);
    assert_eq!(backend.stats().expect("stats").total, 0);
}

#[test]
fn ticketed_refusal_uses_the_stale_fence_error_code() {
    let temp = tempfile::tempdir().expect("tempdir");
    let waiting = backend(&temp)
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    make_ticketed(&temp, &waiting.entry_id, "TASK-1");
    let mut plugin = PluginProcess::spawn(&[]);
    plugin.initialize(temp.path(), "1.2.0");

    let refused = plugin.request(
        "queue/mark_assigned",
        json!({ "entry_id": waiting.entry_id }),
    );

    assert_eq!(refused["error"]["code"], -32209);
    assert!(refused["error"]["message"]
        .as_str()
        .unwrap()
        .contains("use queue/v2/*"));
}
