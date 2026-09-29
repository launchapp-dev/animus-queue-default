//! `queue/v2/lease/renew`, `queue/v2/lease/recover`, `queue/v2/completion`
//! and `queue/v2/release_pending`: v0.2.9 behaviour plus difference 1.

mod common;

use animus_execution_protocol::ExecutionFence;
use animus_queue_default::queue_history::{find_history_by_entry_id, HistoryOutcome};
use animus_queue_default::queue_service::QueueCallError;
use animus_queue_default::{DispatchQueueEntryStatus, QueueBackend};
use animus_queue_protocol::{
    QueueCompletionV2Request, QueueLeaseMutationOutcome, QueueLeaseMutationResponse,
    QueueLeaseRecoverRequest, QueueLeaseRenewRequest, QueueLeaseV2Request,
    QueueReleasePendingV2Request,
};
use chrono::Utc;
use common::{enqueue_request, expire_lease, read_entry, task_dispatch};

fn backend(temp: &tempfile::TempDir) -> QueueBackend {
    QueueBackend::new(temp.path().to_path_buf())
}

/// Add `task_id` and hand it to `owner`; returns the ticket.
fn leased(backend: &QueueBackend, task_id: &str, owner: &str) -> ExecutionFence {
    backend
        .enqueue_v2(enqueue_request(task_id))
        .expect("enqueue");
    lease_next(backend, owner)
}

/// Hand the next waiting entry to `owner`; returns the ticket.
fn lease_next(backend: &QueueBackend, owner: &str) -> ExecutionFence {
    let mut response = backend
        .lease_v2(QueueLeaseV2Request {
            max: 1,
            owner_id: owner.to_string(),
            workflow_ids: vec![format!("wf-{owner}")],
            exclude: Vec::new(),
        })
        .expect("lease");
    assert_eq!(response.leased.len(), 1, "blocked: {:?}", response.blocked);
    response.leased.remove(0).execution
}

fn entry_id(fence: &ExecutionFence) -> String {
    fence.queue_lease.as_ref().unwrap().entry_id.clone()
}

fn renew(
    backend: &QueueBackend,
    fence: &ExecutionFence,
    ttl_secs: Option<u64>,
) -> QueueLeaseMutationResponse {
    backend
        .renew_lease(QueueLeaseRenewRequest {
            execution: fence.clone(),
            ttl_secs,
        })
        .expect("renew")
}

fn recover(
    backend: &QueueBackend,
    fence: &ExecutionFence,
    owner: &str,
) -> QueueLeaseMutationResponse {
    backend
        .recover_lease(QueueLeaseRecoverRequest {
            execution: fence.clone(),
            new_owner_id: owner.to_string(),
            ttl_secs: None,
        })
        .expect("recover")
}

fn complete(
    backend: &QueueBackend,
    fence: &ExecutionFence,
    status: &str,
) -> QueueLeaseMutationResponse {
    backend
        .completion_v2(QueueCompletionV2Request {
            execution: fence.clone(),
            status: status.to_string(),
            workflow_ref: Some("coding".to_string()),
        })
        .expect("completion")
}

fn put_back(backend: &QueueBackend, fence: &ExecutionFence) -> QueueLeaseMutationResponse {
    backend
        .release_pending_v2(QueueReleasePendingV2Request {
            execution: fence.clone(),
            reason: "daemon shutting down".to_string(),
        })
        .expect("release_pending")
}

fn assert_outcome(
    response: &QueueLeaseMutationResponse,
    outcome: QueueLeaseMutationOutcome,
    reason: Option<&str>,
) {
    assert_eq!(response.outcome, outcome, "response: {response:?}");
    assert_eq!(response.reason.as_deref(), reason, "response: {response:?}");
}

fn expires_at(fence: &ExecutionFence) -> chrono::DateTime<Utc> {
    fence.queue_lease.as_ref().unwrap().expires_at
}

fn lease_generation(fence: &ExecutionFence) -> u64 {
    fence.queue_lease.as_ref().unwrap().generation
}

const NOT_OWNER: &str = "execution fence does not own this queue lease";

#[test]
fn renew_extends_a_live_ticket_and_keeps_its_generation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");

    let response = renew(&backend, &fence, None);

    assert_outcome(&response, QueueLeaseMutationOutcome::Applied, None);
    let renewed = response.execution.expect("renewed fence");
    assert!(fence.same_execution_generation(&renewed));
    assert_eq!(lease_generation(&renewed), 1);
    assert_eq!(renewed.queue_lease.as_ref().unwrap().owner_id, "daemon-a");
    assert!(expires_at(&renewed) >= expires_at(&fence));
    assert_eq!(renewed.repository, fence.repository);
}

#[test]
fn renew_never_moves_the_expiry_earlier() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");

    let response = renew(&backend, &fence, Some(60));

    assert_outcome(&response, QueueLeaseMutationOutcome::Applied, None);
    assert_eq!(expires_at(&response.execution.unwrap()), expires_at(&fence));
}

#[test]
fn renew_caps_the_requested_length_at_the_setting() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf()).with_lease_ttl(120);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    let before = Utc::now();

    let response = renew(&backend, &fence, Some(86_400));

    let length = (expires_at(&response.execution.unwrap()) - before).num_seconds();
    assert!((119..=121).contains(&length), "ticket length {length}");
}

#[test]
fn tickets_match_on_owner_and_numbers_not_expiry() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let mut fence = leased(&backend, "TASK-1", "daemon-a");
    fence.queue_lease.as_mut().unwrap().expires_at = Utc::now() - chrono::Duration::days(1);

    assert_outcome(
        &renew(&backend, &fence, None),
        QueueLeaseMutationOutcome::Applied,
        None,
    );
}

#[test]
fn renew_of_an_expired_ticket_is_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    expire_lease(temp.path(), &entry_id(&fence));

    let response = renew(&backend, &fence, None);

    assert_outcome(
        &response,
        QueueLeaseMutationOutcome::StaleFence,
        Some("queue lease has expired and requires recovery"),
    );
    assert!(response.execution.is_none());
}

#[test]
fn takeover_before_expiry_reports_the_live_ticket() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");

    let response = recover(&backend, &fence, "daemon-b");

    assert_outcome(&response, QueueLeaseMutationOutcome::LeaseStillLive, None);
    assert_eq!(response.execution.as_ref(), Some(&fence));
    let entry = read_entry(temp.path(), &entry_id(&fence));
    assert_eq!(entry.lease_owner.as_deref(), Some("daemon-a"));
    assert_eq!(entry.lease_generation, 1);
}

#[test]
fn takeover_after_expiry_raises_the_lease_generation_by_one() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    expire_lease(temp.path(), &entry_id(&fence));
    let before = Utc::now();

    let response = recover(&backend, &fence, "daemon-b");

    assert_outcome(&response, QueueLeaseMutationOutcome::Applied, None);
    let recovered = response.execution.expect("recovered fence");
    assert!(fence.same_execution_generation(&recovered));
    assert_eq!(recovered.repository, fence.repository);
    let ticket = recovered.queue_lease.as_ref().unwrap();
    assert_eq!(ticket.owner_id, "daemon-b");
    assert_eq!(ticket.generation, 2);
    let length = (ticket.expires_at - before).num_seconds();
    assert!((1799..=1801).contains(&length), "ticket length {length}");
    assert_eq!(
        read_entry(temp.path(), &entry_id(&fence)).status,
        DispatchQueueEntryStatus::Assigned
    );
}

#[test]
fn takeover_by_the_same_owner_is_invalid() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    expire_lease(temp.path(), &entry_id(&fence));

    for owner in ["daemon-a", " daemon-a ", "  "] {
        let result = backend.recover_lease(QueueLeaseRecoverRequest {
            execution: fence.clone(),
            new_owner_id: owner.to_string(),
            ttl_secs: None,
        });
        assert!(
            matches!(result, Err(QueueCallError::InvalidParams(_))),
            "owner {owner:?}: {result:?}"
        );
    }
    assert_eq!(
        read_entry(temp.path(), &entry_id(&fence)).lease_generation,
        1
    );
}

#[test]
fn old_ticket_is_refused_after_a_takeover() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let old = leased(&backend, "TASK-1", "daemon-a");
    expire_lease(temp.path(), &entry_id(&old));
    recover(&backend, &old, "daemon-b");

    assert_outcome(
        &renew(&backend, &old, None),
        QueueLeaseMutationOutcome::StaleFence,
        Some(NOT_OWNER),
    );
    assert_outcome(
        &complete(&backend, &old, "completed"),
        QueueLeaseMutationOutcome::StaleFence,
        Some(NOT_OWNER),
    );
    assert_outcome(
        &put_back(&backend, &old),
        QueueLeaseMutationOutcome::StaleFence,
        Some(NOT_OWNER),
    );
    let entry = read_entry(temp.path(), &entry_id(&old));
    assert_eq!(entry.status, DispatchQueueEntryStatus::Assigned);
    assert_eq!(entry.lease_owner.as_deref(), Some("daemon-b"));
}

#[test]
fn a_different_branch_or_generation_is_a_stale_ticket() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");

    let mut other_branch = fence.clone();
    other_branch.repository.as_mut().unwrap().head_ref = "refs/heads/animus/other".to_string();
    let mut other_workflow_generation = fence.clone();
    other_workflow_generation.workflow_generation = 2;
    let mut other_subject_generation = fence.clone();
    other_subject_generation
        .subject
        .as_mut()
        .unwrap()
        .generation = 2;
    let mut no_repository = fence.clone();
    no_repository.repository = None;
    for stale in [
        other_branch,
        other_workflow_generation,
        other_subject_generation,
        no_repository,
    ] {
        assert_outcome(
            &renew(&backend, &stale, None),
            QueueLeaseMutationOutcome::StaleFence,
            Some(NOT_OWNER),
        );
    }

    let mut repository_case = fence.clone();
    let repository = repository_case.repository.as_mut().unwrap();
    repository.repository = repository.repository.to_ascii_uppercase();
    assert_outcome(
        &renew(&backend, &repository_case, None),
        QueueLeaseMutationOutcome::Applied,
        None,
    );
}

#[test]
fn done_moves_the_entry_to_the_history() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");

    let response = complete(&backend, &fence, "failed");

    assert_outcome(&response, QueueLeaseMutationOutcome::Applied, None);
    assert_eq!(response.execution.as_ref(), Some(&fence));
    assert_eq!(backend.stats().expect("stats").total, 0);
    let record = find_history_by_entry_id(temp.path(), &entry_id(&fence))
        .unwrap()
        .expect("history record");
    assert_eq!(record.outcome, HistoryOutcome::Failed);
    assert_eq!(record.finished_by, "queue/v2/completion");
}

#[test]
fn repeated_done_is_acknowledged_and_keeps_the_first_outcome() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    complete(&backend, &fence, "completed");

    let again = complete(&backend, &fence, "failed");

    assert_outcome(&again, QueueLeaseMutationOutcome::AlreadyApplied, None);
    assert_eq!(again.execution.as_ref(), Some(&fence));
    let record = find_history_by_entry_id(temp.path(), &entry_id(&fence))
        .unwrap()
        .unwrap();
    assert_eq!(record.outcome, HistoryOutcome::Completed);
}

#[test]
fn other_calls_on_a_finished_entry_are_not_assigned() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    complete(&backend, &fence, "completed");
    let mut stale = fence.clone();
    stale.queue_lease.as_mut().unwrap().generation = 7;

    for response in [
        renew(&backend, &fence, None),
        recover(&backend, &fence, "daemon-b"),
        put_back(&backend, &fence),
        complete(&backend, &stale, "completed"),
    ] {
        assert_outcome(
            &response,
            QueueLeaseMutationOutcome::NotAssigned,
            Some("queue entry is done"),
        );
    }
}

#[test]
fn done_for_a_task_an_operator_dropped_is_not_assigned() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    backend.drop_entry(&entry_id(&fence)).expect("drop");

    assert_outcome(
        &complete(&backend, &fence, "completed"),
        QueueLeaseMutationOutcome::NotAssigned,
        Some("queue entry is dropped"),
    );
}

#[test]
fn unknown_entry_is_not_found() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let mut fence = leased(&backend, "TASK-1", "daemon-a");
    fence.queue_lease.as_mut().unwrap().entry_id = "no-such-entry".to_string();

    for response in [
        renew(&backend, &fence, None),
        complete(&backend, &fence, "completed"),
        put_back(&backend, &fence),
    ] {
        assert_outcome(&response, QueueLeaseMutationOutcome::NotFound, None);
        assert!(response.execution.is_none());
    }
}

#[test]
fn difference_1_done_with_an_expired_ticket_is_accepted() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    expire_lease(temp.path(), &entry_id(&fence));

    let response = complete(&backend, &fence, "completed");

    assert_outcome(&response, QueueLeaseMutationOutcome::Applied, None);
    assert_eq!(backend.stats().expect("stats").total, 0);
}

#[test]
fn difference_1_put_back_with_an_expired_ticket_is_accepted() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    expire_lease(temp.path(), &entry_id(&fence));

    assert_outcome(
        &put_back(&backend, &fence),
        QueueLeaseMutationOutcome::Applied,
        None,
    );
    assert_eq!(
        read_entry(temp.path(), &entry_id(&fence)).status,
        DispatchQueueEntryStatus::Pending
    );
}

#[test]
fn put_back_keeps_the_run_id_and_the_next_hand_out_raises_the_generation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");

    let response = put_back(&backend, &fence);

    assert_outcome(&response, QueueLeaseMutationOutcome::Applied, None);
    assert!(response.execution.is_none());
    let entry = read_entry(temp.path(), &entry_id(&fence));
    assert_eq!(entry.status, DispatchQueueEntryStatus::Pending);
    assert_eq!(entry.assigned_at, None);
    assert_eq!(entry.workflow_id.as_deref(), Some("wf-daemon-a"));
    let audit = entry.audit_log.last().expect("audit entry");
    assert_eq!(audit.method, "queue/v2/release_pending");
    assert_eq!(audit.from_status, "assigned");
    assert_eq!(audit.to_status, "pending");
    assert_eq!(audit.reason, "daemon shutting down");

    // A retried put-back is acknowledged.
    assert_outcome(
        &put_back(&backend, &fence),
        QueueLeaseMutationOutcome::AlreadyApplied,
        None,
    );
    // Other calls on the waiting entry are refused.
    assert_outcome(
        &renew(&backend, &fence, None),
        QueueLeaseMutationOutcome::NotAssigned,
        Some("queue entry is pending"),
    );

    let next = lease_next(&backend, "daemon-b");
    assert!(fence.same_execution_generation(&next));
    assert_eq!(next.workflow_id, "wf-daemon-a");
    assert_eq!(lease_generation(&next), 2);
    assert_eq!(next.queue_lease.as_ref().unwrap().owner_id, "daemon-b");
    assert_outcome(
        &complete(&backend, &fence, "completed"),
        QueueLeaseMutationOutcome::StaleFence,
        Some(NOT_OWNER),
    );
}

#[test]
fn held_entry_is_not_assigned() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    put_back(&backend, &fence);
    backend.hold(&entry_id(&fence)).expect("hold");

    assert_outcome(
        &complete(&backend, &fence, "completed"),
        QueueLeaseMutationOutcome::NotAssigned,
        Some("queue entry is held"),
    );
}

#[test]
fn old_style_running_entry_has_no_ticket_to_match() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let old = backend
        .enqueue(task_dispatch("TASK-2", "standard"), None, None)
        .expect("old-style enqueue");
    backend
        .lease(1, Some(vec!["wf-old".to_string()]), None)
        .expect("old-style lease");
    let mut fence = leased(&backend, "TASK-1", "daemon-a");
    fence.queue_lease.as_mut().unwrap().entry_id = old.entry_id.clone();

    assert_outcome(
        &complete(&backend, &fence, "completed"),
        QueueLeaseMutationOutcome::StaleFence,
        Some(NOT_OWNER),
    );
    assert_eq!(
        read_entry(temp.path(), &old.entry_id).status,
        DispatchQueueEntryStatus::Assigned
    );
}

#[test]
fn invalid_requests_are_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let fence = leased(&backend, "TASK-1", "daemon-a");
    let mut no_lease = fence.clone();
    no_lease.queue_lease = None;

    let results = [
        backend.completion_v2(QueueCompletionV2Request {
            execution: fence.clone(),
            status: "done".to_string(),
            workflow_ref: None,
        }),
        backend.release_pending_v2(QueueReleasePendingV2Request {
            execution: fence.clone(),
            reason: "  ".to_string(),
        }),
        backend.renew_lease(QueueLeaseRenewRequest {
            execution: fence.clone(),
            ttl_secs: Some(0),
        }),
        backend.renew_lease(QueueLeaseRenewRequest {
            execution: no_lease,
            ttl_secs: None,
        }),
    ];
    for result in results {
        assert!(
            matches!(result, Err(QueueCallError::InvalidParams(_))),
            "{result:?}"
        );
    }
    assert_eq!(
        read_entry(temp.path(), &entry_id(&fence)).status,
        DispatchQueueEntryStatus::Assigned
    );
}
