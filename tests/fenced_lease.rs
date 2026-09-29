//! `queue/v2/lease`: v0.2.9 behaviour plus difference 5.

mod common;

use animus_execution_protocol::{
    ExecutionFence, QueueLeaseFence, RepositoryReservation, SubjectGeneration,
    EXECUTION_FENCE_SCHEMA_ID, EXECUTION_FENCE_VERSION,
};
use animus_queue_default::queue_service::QueueCallError;
use animus_queue_default::{
    load_queue_state, DispatchQueueEntry, DispatchQueueEntryStatus, QueueBackend,
};
use animus_queue_protocol::{
    QueueEnqueueV2Request, QueueLeaseBlockReason, QueueLeaseV2Request, QueueLeaseV2Response,
};
use animus_subject_protocol::{SubjectDispatch, SubjectRef};
use chrono::Utc;
use common::{edit_state, enqueue_request, expire_lease, read_entry, reservation, task_dispatch};

fn backend(temp: &tempfile::TempDir) -> QueueBackend {
    QueueBackend::new(temp.path().to_path_buf())
}

/// A lease call for `owner` with `max` fresh workflow ids.
fn lease_request(max: usize, owner: &str, exclude: Vec<ExecutionFence>) -> QueueLeaseV2Request {
    QueueLeaseV2Request {
        max,
        owner_id: owner.to_string(),
        workflow_ids: (1..=max).map(|n| format!("wf-{owner}-{n}")).collect(),
        exclude,
    }
}

fn lease(backend: &QueueBackend, max: usize, owner: &str) -> QueueLeaseV2Response {
    backend
        .lease_v2(lease_request(max, owner, Vec::new()))
        .expect("lease")
}

/// Ticketed add of `task_id` reserving the branch of `branch_task`.
fn enqueue_on_branch(backend: &QueueBackend, task_id: &str, branch_task: &str) -> String {
    backend
        .enqueue_v2(QueueEnqueueV2Request {
            repository: Some(reservation(branch_task)),
            ..enqueue_request(task_id)
        })
        .expect("enqueue")
        .entry_id
}

fn add(backend: &QueueBackend, task_id: &str) -> String {
    backend
        .enqueue_v2(enqueue_request(task_id))
        .expect("enqueue")
        .entry_id
}

/// A fence some other run holds, for `exclude`.
fn foreign_fence(
    subject: Option<SubjectGeneration>,
    repository: Option<RepositoryReservation>,
) -> ExecutionFence {
    ExecutionFence {
        schema: EXECUTION_FENCE_SCHEMA_ID.to_string(),
        version: EXECUTION_FENCE_VERSION,
        workflow_id: "wf-elsewhere".to_string(),
        workflow_generation: 1,
        subject,
        queue_lease: Some(QueueLeaseFence {
            entry_id: "entry-elsewhere".to_string(),
            owner_id: "daemon-x".to_string(),
            generation: 1,
            expires_at: Utc::now() + chrono::Duration::seconds(600),
        }),
        repository,
    }
}

fn invalid_params(result: Result<QueueLeaseV2Response, QueueCallError>) -> String {
    match result {
        Err(QueueCallError::InvalidParams(message)) => message,
        other => panic!("expected InvalidParams, got {other:?}"),
    }
}

#[test]
fn hands_out_an_entry_with_its_full_ticket() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let entry_id = add(&backend, "TASK-1");
    let before = Utc::now();

    let response = lease(&backend, 1, "daemon-a");

    assert!(response.blocked.is_empty());
    assert_eq!(response.leased.len(), 1);
    let leased = &response.leased[0];
    leased.validate().expect("FencedQueueEntry::validate");
    leased.execution.validate_coding().expect("coding fence");
    assert_eq!(leased.entry.entry_id, entry_id);
    assert_eq!(leased.entry.status, "assigned");
    assert_eq!(leased.entry.workflow_id.as_deref(), Some("wf-daemon-a-1"));
    let execution = &leased.execution;
    assert_eq!(execution.workflow_id, "wf-daemon-a-1");
    assert_eq!(execution.workflow_generation, 1);
    assert_eq!(
        execution.subject,
        Some(SubjectGeneration {
            qualified_id: "task:TASK-1".to_string(),
            generation: 1,
        })
    );
    let ticket = execution.queue_lease.as_ref().expect("queue lease");
    assert_eq!(ticket.entry_id, entry_id);
    assert_eq!(ticket.owner_id, "daemon-a");
    assert_eq!(ticket.generation, 1);
    let length = (ticket.expires_at - before).num_seconds();
    assert!((1799..=1801).contains(&length), "ticket length {length}");
    assert_eq!(execution.repository, Some(reservation("TASK-1")));
    assert_eq!(
        read_entry(temp.path(), &entry_id)
            .execution_fence()
            .as_ref(),
        Some(execution)
    );
}

#[test]
fn hands_out_five_in_queue_order() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let ids: Vec<String> = (1..=6)
        .map(|n| add(&backend, &format!("TASK-{n}")))
        .collect();

    let response = lease(&backend, 5, "daemon-a");

    let leased: Vec<&str> = response
        .leased
        .iter()
        .map(|fenced| fenced.entry.entry_id.as_str())
        .collect();
    assert_eq!(
        leased,
        ids[..5].iter().map(String::as_str).collect::<Vec<_>>()
    );
    let workflow_ids: Vec<&str> = response
        .leased
        .iter()
        .map(|fenced| fenced.execution.workflow_id.as_str())
        .collect();
    assert_eq!(
        workflow_ids,
        [
            "wf-daemon-a-1",
            "wf-daemon-a-2",
            "wf-daemon-a-3",
            "wf-daemon-a-4",
            "wf-daemon-a-5"
        ]
    );
    assert_eq!(
        read_entry(temp.path(), &ids[5]).status,
        DispatchQueueEntryStatus::Pending
    );
}

#[test]
fn refuses_more_than_five() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let entry_id = add(&backend, "TASK-1");

    let message = invalid_params(backend.lease_v2(lease_request(6, "daemon-a", Vec::new())));

    assert_eq!(
        message,
        "queue v2 lease requires max 1..5, owner_id, and max unique workflow_ids"
    );
    assert_eq!(
        read_entry(temp.path(), &entry_id).status,
        DispatchQueueEntryStatus::Pending
    );
}

#[test]
fn refuses_invalid_requests_without_changing_anything() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let entry_id = add(&backend, "TASK-1");
    let valid = lease_request(2, "daemon-a", Vec::new());

    let mut zero = valid.clone();
    zero.max = 0;
    zero.workflow_ids.clear();
    let mut blank_owner = valid.clone();
    blank_owner.owner_id = "   ".to_string();
    let mut too_few_ids = valid.clone();
    too_few_ids.workflow_ids.pop();
    let mut duplicate_ids = valid.clone();
    duplicate_ids.workflow_ids = vec!["wf-1".to_string(), "wf-1".to_string()];
    let mut blank_id = valid.clone();
    blank_id.workflow_ids[1] = " ".to_string();
    let mut bad_exclude = valid.clone();
    let mut fence = foreign_fence(None, None);
    fence.workflow_generation = 0;
    bad_exclude.exclude = vec![fence];

    for request in [
        zero,
        blank_owner,
        too_few_ids,
        duplicate_ids,
        blank_id,
        bad_exclude,
    ] {
        invalid_params(backend.lease_v2(request));
    }
    assert_eq!(
        read_entry(temp.path(), &entry_id).status,
        DispatchQueueEntryStatus::Pending
    );
}

#[test]
fn skips_held_and_not_yet_due_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let held = add(&backend, "TASK-1");
    backend.hold(&held).expect("hold");
    backend
        .enqueue_v2(QueueEnqueueV2Request {
            run_at: Some((Utc::now() + chrono::Duration::hours(1)).to_rfc3339()),
            ..enqueue_request("TASK-2")
        })
        .expect("deferred enqueue");
    let due = add(&backend, "TASK-3");

    let response = lease(&backend, 5, "daemon-a");

    assert_eq!(response.leased.len(), 1);
    assert_eq!(response.leased[0].entry.entry_id, due);
    assert!(response.blocked.is_empty());
}

#[test]
fn expired_ticket_is_not_handed_out_again() {
    // v0.2.9: only waiting entries are candidates, so an expired ticket is
    // neither handed out nor reported as blocked. It comes back only through
    // queue/v2/lease/recover.
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let entry_id = add(&backend, "TASK-1");
    lease(&backend, 1, "daemon-a");
    expire_lease(temp.path(), &entry_id);

    let response = lease(&backend, 5, "daemon-b");

    assert!(response.leased.is_empty());
    assert!(response.blocked.is_empty());
    let entry = read_entry(temp.path(), &entry_id);
    assert_eq!(entry.status, DispatchQueueEntryStatus::Assigned);
    assert_eq!(entry.lease_owner.as_deref(), Some("daemon-a"));
    assert_eq!(entry.lease_generation, 1);
}

#[test]
fn active_subject_generation_in_exclude_blocks_the_entry() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let entry_id = add(&backend, "TASK-1");
    let holder = foreign_fence(
        Some(SubjectGeneration {
            qualified_id: "task:TASK-1".to_string(),
            generation: 1,
        }),
        None,
    );

    let response = backend
        .lease_v2(lease_request(1, "daemon-a", vec![holder.clone()]))
        .expect("lease");

    assert!(response.leased.is_empty());
    assert_eq!(response.blocked.len(), 1);
    assert_eq!(response.blocked[0].entry_id, entry_id);
    assert_eq!(
        response.blocked[0].reason,
        QueueLeaseBlockReason::SubjectGenerationActive
    );
    assert_eq!(response.blocked[0].conflicts_with, Some(holder));
    let entry = read_entry(temp.path(), &entry_id);
    assert_eq!(entry.status, DispatchQueueEntryStatus::Pending);
    assert_eq!(entry.lease_generation, 0);
}

#[test]
fn branch_held_by_an_excluded_ticket_blocks_the_entry() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let entry_id = add(&backend, "TASK-1");
    // Same repository in a different case, same branch: a collision.
    let mut branch = reservation("TASK-1");
    branch.repository = branch.repository.to_ascii_uppercase();
    let holder = foreign_fence(None, Some(branch));
    // Same repository, other branch: no collision.
    let bystander = foreign_fence(None, Some(reservation("TASK-9")));

    let response = backend
        .lease_v2(lease_request(
            1,
            "daemon-a",
            vec![bystander, holder.clone()],
        ))
        .expect("lease");

    assert!(response.leased.is_empty());
    assert_eq!(response.blocked.len(), 1);
    assert_eq!(
        response.blocked[0].reason,
        QueueLeaseBlockReason::RepositoryRefCollision
    );
    assert_eq!(response.blocked[0].conflicts_with, Some(holder));
    assert_eq!(
        read_entry(temp.path(), &entry_id).status,
        DispatchQueueEntryStatus::Pending
    );
}

#[test]
fn branch_held_by_a_running_entry_blocks_the_entry() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    enqueue_on_branch(&backend, "TASK-1", "SHARED");
    let waiting = enqueue_on_branch(&backend, "TASK-2", "SHARED");
    let first = lease(&backend, 1, "daemon-a");

    let second = lease(&backend, 1, "daemon-a");

    assert!(second.leased.is_empty());
    assert_eq!(second.blocked.len(), 1);
    assert_eq!(second.blocked[0].entry_id, waiting);
    assert_eq!(
        second.blocked[0].reason,
        QueueLeaseBlockReason::RepositoryRefCollision
    );
    assert_eq!(
        second.blocked[0].conflicts_with.as_ref(),
        Some(&first.leased[0].execution)
    );
}

#[test]
fn branch_collision_within_one_call() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    enqueue_on_branch(&backend, "TASK-1", "SHARED");
    let second = enqueue_on_branch(&backend, "TASK-2", "SHARED");
    let other = add(&backend, "TASK-3");

    let response = lease(&backend, 5, "daemon-a");

    let leased: Vec<&str> = response
        .leased
        .iter()
        .map(|fenced| fenced.entry.entry_id.as_str())
        .collect();
    assert_eq!(leased.len(), 2);
    assert_eq!(leased[1], other);
    assert_eq!(response.blocked.len(), 1);
    assert_eq!(response.blocked[0].entry_id, second);
    assert_eq!(
        response.blocked[0].conflicts_with.as_ref(),
        Some(&response.leased[0].execution)
    );
}

#[test]
fn difference_5_old_style_entry_gets_ticket_identity_at_first_hand_out() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let old = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("old-style enqueue");
    let before = read_entry(temp.path(), &old.entry_id);
    assert!(!before.is_ticketed());
    assert_eq!(before.subject_id.as_deref(), Some("TASK-1"));

    let response = lease(&backend, 1, "daemon-a");

    assert_eq!(response.leased.len(), 1);
    let leased = &response.leased[0];
    leased.validate().expect("FencedQueueEntry::validate");
    leased
        .execution
        .validate_queue_backed()
        .expect("queue-backed fence");
    assert_eq!(
        leased.execution.subject,
        Some(SubjectGeneration {
            qualified_id: "task:TASK-1".to_string(),
            generation: 1,
        })
    );
    assert_eq!(leased.execution.repository, None);
    let after = read_entry(temp.path(), &old.entry_id);
    assert_eq!(after.subject_id.as_deref(), Some("task:TASK-1"));
    assert_eq!(after.subject_generation, Some(1));
    let state = load_queue_state(temp.path()).unwrap().unwrap();
    assert_eq!(state.subject_generations.get("task:TASK-1"), Some(&1));
}

#[test]
fn second_copy_of_a_task_is_handed_out_with_the_next_generation() {
    // Old files and old-style adds can hold two copies of one task. v0.2.9
    // numbers them in queue order and hands out both (spec §7.3).
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    for _ in 0..2 {
        backend
            .enqueue(task_dispatch("TASK-1", "standard"), None, None)
            .expect("old-style enqueue");
    }

    let response = lease(&backend, 5, "daemon-a");

    let generations: Vec<u64> = response
        .leased
        .iter()
        .map(|fenced| fenced.execution.subject.as_ref().unwrap().generation)
        .collect();
    assert_eq!(generations, [1, 2]);
}

#[test]
fn entry_without_a_usable_subject_is_blocked_and_the_rest_still_run() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    edit_state(temp.path(), |state| {
        state.entries.push(DispatchQueueEntry {
            entry_id: "no-dispatch".to_string(),
            task_id: "TASK-0".to_string(),
            ..DispatchQueueEntry::default()
        });
        state.entries.push(DispatchQueueEntry::from_dispatch(
            SubjectDispatch::for_subject_with_metadata(
                SubjectRef::task("   "),
                "standard",
                "integration-test",
                Utc::now(),
            ),
            None,
            None,
        ));
    });
    let blank_id = load_queue_state(temp.path()).unwrap().unwrap().entries[1]
        .entry_id
        .clone();
    let good = add(&backend, "TASK-1");

    let response = lease(&backend, 5, "daemon-a");

    assert_eq!(response.leased.len(), 1);
    assert_eq!(response.leased[0].entry.entry_id, good);
    let blocked: Vec<(&str, QueueLeaseBlockReason)> = response
        .blocked
        .iter()
        .map(|block| (block.entry_id.as_str(), block.reason))
        .collect();
    assert_eq!(
        blocked,
        [
            (
                "no-dispatch",
                QueueLeaseBlockReason::MissingExecutionIdentity
            ),
            (
                blank_id.as_str(),
                QueueLeaseBlockReason::MissingExecutionIdentity
            ),
        ]
    );
    assert!(response
        .blocked
        .iter()
        .all(|block| block.conflicts_with.is_none()));
    assert!(!read_entry(temp.path(), &blank_id).is_ticketed());
}

#[test]
fn entry_with_a_workflow_id_keeps_it_and_leaves_the_new_ids_unused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let kept = add(&backend, "TASK-1");
    add(&backend, "TASK-2");
    edit_state(temp.path(), |state| {
        common::entry_mut(state, &kept).workflow_id = Some("wf-existing".to_string());
    });

    let response = lease(&backend, 2, "daemon-a");

    let workflow_ids: Vec<&str> = response
        .leased
        .iter()
        .map(|fenced| fenced.execution.workflow_id.as_str())
        .collect();
    assert_eq!(workflow_ids, ["wf-existing", "wf-daemon-a-1"]);
}

#[test]
fn difference_6_ticket_length_follows_the_setting() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf()).with_lease_ttl(120);
    add(&backend, "TASK-1");
    let before = Utc::now();

    let response = lease(&backend, 1, "daemon-a");

    let expires_at = response.leased[0]
        .execution
        .queue_lease
        .as_ref()
        .unwrap()
        .expires_at;
    let length = (expires_at - before).num_seconds();
    assert!((119..=121).contains(&length), "ticket length {length}");
}

#[test]
fn looks_at_most_fifty_waiting_entries_per_call() {
    // v0.2.9 reads max(50, max * 10) waiting entries per call. A free entry
    // behind 50 blocked ones waits for a later call.
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    enqueue_on_branch(&backend, "RUNNING", "SHARED");
    lease(&backend, 1, "daemon-a");
    for n in 1..=50 {
        enqueue_on_branch(&backend, &format!("TASK-{n}"), "SHARED");
    }
    let free = add(&backend, "TASK-FREE");

    let response = lease(&backend, 5, "daemon-a");

    assert!(response.leased.is_empty());
    assert_eq!(response.blocked.len(), 50);
    assert_eq!(
        read_entry(temp.path(), &free).status,
        DispatchQueueEntryStatus::Pending
    );
}
