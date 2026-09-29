//! Moving between queue v0.3.3 and v0.4.0 on the same `queue.json`
//! using the real v0.3.3 crate.

mod common;

use std::collections::HashMap;

use animus_queue_default::dispatch_queue_state::QUEUE_FORMAT_VERSION;
use animus_queue_default::{load_queue_state, DispatchQueueEntryStatus, QueueBackend};
use animus_queue_protocol::{QueueLeaseV2Request, QueueReleasePendingV2Request};
use common::{enqueue_request, task_dispatch};
use serde::de::DeserializeOwned;

/// A v0.3.3 dispatch (the type is inferred from the v0.3.3 call), decoded
/// from today's wire form. That this decodes at all is part of the rollback
/// guarantee.
fn v033_dispatch<T: DeserializeOwned>(task_id: &str) -> T {
    serde_json::from_value(serde_json::to_value(task_dispatch(task_id, "standard")).unwrap())
        .expect("v0.3.3 decodes today's dispatch")
}

fn lease_request(max: usize, owner: &str) -> QueueLeaseV2Request {
    QueueLeaseV2Request {
        max,
        owner_id: owner.to_string(),
        workflow_ids: (1..=max).map(|n| format!("wf-{owner}-{n}")).collect(),
        exclude: Vec::new(),
    }
}

#[test]
fn upgrades_a_file_written_by_v033() {
    let temp = tempfile::tempdir().expect("tempdir");
    let old = queue_v033::QueueBackend::new(temp.path().to_path_buf());
    let running = old
        .enqueue(v033_dispatch("TASK-3"), None, None)
        .unwrap()
        .entry_id;
    old.lease(1, Some(vec!["wf-old".to_string()]), None)
        .unwrap();
    let waiting = old
        .enqueue(v033_dispatch("TASK-1"), None, None)
        .unwrap()
        .entry_id;
    let held = old
        .enqueue(v033_dispatch("TASK-2"), None, None)
        .unwrap()
        .entry_id;
    old.hold(&held).unwrap();

    let new = QueueBackend::new(temp.path().to_path_buf());
    let before = load_queue_state(temp.path()).unwrap().unwrap();
    assert_eq!(before.format_version, 0);
    assert!(before.subject_generations.is_empty());
    let listed = new.list(&[], None, None).expect("list");
    let statuses: HashMap<&str, &str> = listed
        .entries
        .iter()
        .map(|entry| (entry.entry_id.as_str(), entry.status.as_str()))
        .collect();
    assert_eq!(statuses[running.as_str()], "assigned");
    assert_eq!(statuses[waiting.as_str()], "pending");
    assert_eq!(statuses[held.as_str()], "held");

    // Waiting entries get ticket identity at their first ticketed hand-out.
    let leased = new.lease_v2(lease_request(5, "daemon-a")).expect("lease");
    assert_eq!(leased.leased.len(), 1);
    assert_eq!(leased.leased[0].entry.entry_id, waiting);
    leased.leased[0]
        .validate()
        .expect("FencedQueueEntry::validate");
    assert_eq!(
        leased.leased[0]
            .execution
            .subject
            .as_ref()
            .unwrap()
            .qualified_id,
        "task:TASK-1"
    );

    // Running entries stay old-style; the old-style "done" finishes them.
    let done = new
        .completion(&running, "completed", None, Some("wf-old"))
        .expect("completion");
    assert!(done.changed);

    // Held entries carry over and run once released.
    new.release(&held).expect("release");
    let leased = new.lease_v2(lease_request(5, "daemon-a")).expect("lease");
    assert_eq!(leased.leased[0].entry.entry_id, held);

    let after = load_queue_state(temp.path()).unwrap().unwrap();
    assert_eq!(after.format_version, QUEUE_FORMAT_VERSION);
}

#[test]
fn v033_reads_a_file_written_by_v040_and_v040_reads_it_back() {
    let temp = tempfile::tempdir().expect("tempdir");
    let new = QueueBackend::new(temp.path().to_path_buf());
    let waiting = new.enqueue_v2(enqueue_request("TASK-1")).unwrap().entry_id;
    let held = new.enqueue_v2(enqueue_request("TASK-2")).unwrap().entry_id;
    new.hold(&held).unwrap();
    let running = new.enqueue_v2(enqueue_request("TASK-3")).unwrap().entry_id;
    // Hand out TASK-1 and TASK-3, then put TASK-1 back: it keeps its ticket
    // fields while waiting.
    let leased = new.lease_v2(lease_request(5, "daemon-a")).unwrap();
    let first = leased
        .leased
        .iter()
        .find(|fenced| fenced.entry.entry_id == waiting)
        .unwrap();
    new.release_pending_v2(QueueReleasePendingV2Request {
        execution: first.execution.clone(),
        reason: "rolling back".to_string(),
    })
    .unwrap();
    let old_style = new
        .enqueue(task_dispatch("TASK-4", "standard"), None, None)
        .unwrap()
        .entry_id;

    // Roll back: v0.3.3 lists everything, with statuses intact.
    let old = queue_v033::QueueBackend::new(temp.path().to_path_buf());
    let listed = old.list(&[], None, None).expect("v0.3.3 list");
    let statuses: HashMap<&str, &str> = listed
        .entries
        .iter()
        .map(|entry| (entry.entry_id.as_str(), entry.status.as_str()))
        .collect();
    assert_eq!(statuses.len(), 4);
    assert_eq!(statuses[waiting.as_str()], "pending");
    assert_eq!(statuses[held.as_str()], "held");
    assert_eq!(statuses[running.as_str()], "assigned");
    assert_eq!(statuses[old_style.as_str()], "pending");
    // ...and v0.3.3 can write it.
    old.release(&held).expect("v0.3.3 release");
    old.enqueue(v033_dispatch("TASK-5"), None, None)
        .expect("v0.3.3 enqueue");

    // Move forward again: v0.4.0 loads what v0.3.3 wrote.
    let state = load_queue_state(temp.path()).unwrap().unwrap();
    assert_eq!(state.entries.len(), 5);
    let leased = new.lease_v2(lease_request(5, "daemon-b")).expect("lease");
    assert_eq!(leased.leased.len(), 4);
    for fenced in &leased.leased {
        fenced.validate().expect("FencedQueueEntry::validate");
    }
    let running_entry = state
        .entries
        .iter()
        .find(|entry| entry.entry_id == running)
        .unwrap();
    assert_eq!(running_entry.status, DispatchQueueEntryStatus::Assigned);
}
