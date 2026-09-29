//! Crash safety (spec §6.2) and many queue processes on one project
//! (spec §6.3).

mod common;

use std::collections::HashSet;
use std::thread;

use animus_queue_default::queue_history::{
    append_history, find_history_by_entry_id, queue_history_path, HistoryOutcome, HistoryRecord,
};
use animus_queue_default::{load_queue_state, DispatchQueueEntryStatus, QueueBackend};
use animus_queue_protocol::{
    QueueCompletionV2Request, QueueLeaseMutationOutcome, QueueLeaseRecoverRequest,
    QueueLeaseV2Request, QueueLeaseV2Response, METHOD_QUEUE_LEASE_V2,
};
use common::{enqueue_request, expire_lease, read_entry, PluginProcess};

fn lease_request(owner: &str, round: usize) -> QueueLeaseV2Request {
    QueueLeaseV2Request {
        max: 5,
        owner_id: owner.to_string(),
        workflow_ids: (1..=5).map(|n| format!("wf-{owner}-{round}-{n}")).collect(),
        exclude: Vec::new(),
    }
}

fn history_lines_for(project_root: &std::path::Path, entry_id: &str) -> usize {
    std::fs::read_to_string(queue_history_path(project_root))
        .unwrap()
        .lines()
        .filter(|line| line.contains(entry_id))
        .count()
}

#[test]
fn crash_between_history_append_and_file_replace_loses_and_duplicates_nothing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    backend.enqueue_v2(enqueue_request("TASK-1")).unwrap();
    let fence = backend
        .lease_v2(lease_request("daemon-a", 0))
        .unwrap()
        .leased
        .remove(0)
        .execution;
    let entry_id = fence.queue_lease.as_ref().unwrap().entry_id.clone();
    // The first attempt appended its history line, then crashed before
    // replacing queue.json: the entry is still live.
    let entry = read_entry(temp.path(), &entry_id);
    append_history(
        temp.path(),
        &[HistoryRecord::finished(
            &entry,
            HistoryOutcome::Completed,
            "queue/v2/completion",
            None,
        )],
    )
    .unwrap();
    let completion = QueueCompletionV2Request {
        execution: fence.clone(),
        status: "completed".to_string(),
        workflow_ref: None,
    };

    // The daemon's retry finishes it again.
    let retry = backend.completion_v2(completion.clone()).unwrap();
    assert_eq!(retry.outcome, QueueLeaseMutationOutcome::Applied);
    assert!(load_queue_state(temp.path())
        .unwrap()
        .unwrap()
        .entries
        .is_empty());
    assert_eq!(history_lines_for(temp.path(), &entry_id), 2);

    // The last line is the committed one, and later retries are acknowledged.
    let record = find_history_by_entry_id(temp.path(), &entry_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.outcome, HistoryOutcome::Completed);
    let again = backend.completion_v2(completion).unwrap();
    assert_eq!(again.outcome, QueueLeaseMutationOutcome::AlreadyApplied);
    assert_eq!(history_lines_for(temp.path(), &entry_id), 2);
}

#[test]
fn interrupted_done_then_takeover_leaves_the_new_owner_in_charge() {
    // Daemon A's "done" appended its history line, then crashed before
    // queue.json was replaced. Nobody was told the task finished, so it is
    // still running, as after a rolled-back Postgres transaction. Its ticket
    // expires, daemon B takes it over and finishes it, and B's retried "done"
    // must be acknowledged against B's ticket, not A's leftover line.
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    backend.enqueue_v2(enqueue_request("TASK-1")).unwrap();
    let fence_a = backend
        .lease_v2(lease_request("daemon-a", 0))
        .unwrap()
        .leased
        .remove(0)
        .execution;
    let entry_id = fence_a.queue_lease.as_ref().unwrap().entry_id.clone();
    let entry = read_entry(temp.path(), &entry_id);
    append_history(
        temp.path(),
        &[HistoryRecord::finished(
            &entry,
            HistoryOutcome::Completed,
            "queue/v2/completion",
            None,
        )],
    )
    .unwrap();
    expire_lease(temp.path(), &entry_id);

    let recovered = backend
        .recover_lease(QueueLeaseRecoverRequest {
            execution: fence_a.clone(),
            new_owner_id: "daemon-b".to_string(),
            ttl_secs: None,
        })
        .unwrap();
    assert_eq!(recovered.outcome, QueueLeaseMutationOutcome::Applied);
    let fence_b = recovered.execution.unwrap();
    let done = QueueCompletionV2Request {
        execution: fence_b.clone(),
        status: "failed".to_string(),
        workflow_ref: None,
    };
    assert_eq!(
        backend.completion_v2(done.clone()).unwrap().outcome,
        QueueLeaseMutationOutcome::Applied
    );

    let retry = backend.completion_v2(done).unwrap();

    assert_eq!(retry.outcome, QueueLeaseMutationOutcome::AlreadyApplied);
    assert_eq!(retry.execution, Some(fence_b));
    let record = find_history_by_entry_id(temp.path(), &entry_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.outcome, HistoryOutcome::Failed);
    assert_eq!(record.entry.lease_owner.as_deref(), Some("daemon-b"));
    // A's leftover ticket is refused.
    let late_a = backend
        .completion_v2(QueueCompletionV2Request {
            execution: fence_a,
            status: "completed".to_string(),
            workflow_ref: None,
        })
        .unwrap();
    assert_eq!(late_a.outcome, QueueLeaseMutationOutcome::NotAssigned);
}

#[test]
fn many_threads_never_hand_out_an_entry_twice() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let backend = QueueBackend::new(root.clone());
    for n in 1..=40 {
        backend
            .enqueue_v2(enqueue_request(&format!("TASK-{n}")))
            .unwrap();
    }

    let workers: Vec<_> = (0..8)
        .map(|worker| {
            let root = root.clone();
            thread::spawn(move || {
                let backend = QueueBackend::new(root);
                let owner = format!("daemon-{worker}");
                let mut leased = Vec::new();
                for round in 0.. {
                    let response = backend.lease_v2(lease_request(&owner, round)).unwrap();
                    if response.leased.is_empty() {
                        break;
                    }
                    leased.extend(response.leased.into_iter().map(|f| f.entry.entry_id));
                }
                leased
            })
        })
        .collect();
    let all: Vec<String> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();

    let unique: HashSet<&String> = all.iter().collect();
    assert_eq!(all.len(), 40);
    assert_eq!(unique.len(), 40);
    let state = load_queue_state(temp.path()).unwrap().unwrap();
    assert!(state.entries.iter().all(|entry| {
        entry.status == DispatchQueueEntryStatus::Assigned && entry.lease_generation == 1
    }));
}

#[test]
fn many_plugin_processes_never_hand_out_an_entry_twice() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    let backend = QueueBackend::new(root.clone());
    for n in 1..=30 {
        backend
            .enqueue_v2(enqueue_request(&format!("TASK-{n}")))
            .unwrap();
    }

    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let root = root.clone();
            thread::spawn(move || {
                let mut plugin = PluginProcess::spawn(&[]);
                plugin.initialize(&root, "1.2.0");
                let owner = format!("daemon-{worker}");
                let mut leased = Vec::new();
                for round in 0.. {
                    let response = plugin.request(
                        METHOD_QUEUE_LEASE_V2,
                        serde_json::to_value(lease_request(&owner, round)).unwrap(),
                    );
                    let response: QueueLeaseV2Response =
                        serde_json::from_value(response["result"].clone())
                            .unwrap_or_else(|error| panic!("{error}: {response}"));
                    if response.leased.is_empty() {
                        break;
                    }
                    leased.extend(response.leased.into_iter().map(|f| f.entry.entry_id));
                }
                leased
            })
        })
        .collect();
    let all: Vec<String> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();

    let unique: HashSet<&String> = all.iter().collect();
    assert_eq!(all.len(), 30);
    assert_eq!(unique.len(), 30);
}

#[test]
fn concurrent_adds_of_one_task_make_one_entry() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();

    let workers: Vec<_> = (0..8)
        .map(|worker| {
            let root = root.clone();
            thread::spawn(move || {
                let mut request = enqueue_request("TASK-1");
                request.idempotency_key = Some(format!("delivery-{worker}"));
                QueueBackend::new(root).enqueue_v2(request).unwrap()
            })
        })
        .collect();
    let receipts: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();

    assert_eq!(receipts.iter().filter(|r| r.enqueued).count(), 1);
    let entry_ids: HashSet<&String> = receipts.iter().map(|r| &r.entry_id).collect();
    assert_eq!(entry_ids.len(), 1);
    let state = load_queue_state(temp.path()).unwrap().unwrap();
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.subject_generations.get("task:TASK-1"), Some(&1));
}
