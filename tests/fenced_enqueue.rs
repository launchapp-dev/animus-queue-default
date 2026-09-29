//! `queue/v2/enqueue`: v0.2.9 behaviour plus differences 2, 3 and 7.

mod common;

use animus_queue_default::queue_service::QueueCallError;
use animus_queue_default::{queue_state_path, DispatchQueueEntryStatus, QueueBackend};
use animus_queue_protocol::QueueEnqueueV2Request;
use animus_subject_protocol::{SubjectDispatch, SubjectRef};
use chrono::Utc;
use common::{edit_state, enqueue_request, entry_mut, read_entry, reservation, task_dispatch};

fn backend(temp: &tempfile::TempDir) -> QueueBackend {
    QueueBackend::new(temp.path().to_path_buf())
}

fn invalid_params(
    result: Result<animus_queue_protocol::QueueEnqueueV2Response, QueueCallError>,
) -> String {
    match result {
        Err(QueueCallError::InvalidParams(message)) => message,
        other => panic!("expected InvalidParams, got {other:?}"),
    }
}

#[test]
fn adds_a_new_entry_with_the_first_generation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);

    let added = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("enqueue");

    assert!(added.enqueued);
    assert!(added.warning.is_none());
    assert_eq!(added.subject.qualified_id, "task:TASK-1");
    assert_eq!(added.subject.generation, 1);
    let listed = backend.list(&[], None, None).expect("list");
    assert_eq!(listed.entries[0].entry_id, added.entry_id);
    assert_eq!(listed.entries[0].subject_id, "task:TASK-1");
    assert_eq!(listed.entries[0].task_id.as_deref(), Some("TASK-1"));
    assert_eq!(listed.entries[0].status, "pending");
}

#[test]
fn five_tasks_each_get_generation_one() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    for n in 1..=5 {
        let added = backend
            .enqueue_v2(enqueue_request(&format!("TASK-{n}")))
            .expect("enqueue");
        assert!(added.enqueued);
        assert_eq!(added.subject.generation, 1);
    }
    assert_eq!(backend.stats().expect("stats").pending, 5);
}

#[test]
fn replay_with_same_key_and_content_returns_the_original_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let first = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("first");

    let replay = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("replay");

    assert!(!replay.enqueued);
    assert_eq!(replay.entry_id, first.entry_id);
    assert_eq!(replay.subject, first.subject);
    assert!(replay.warning.is_none());
    assert_eq!(backend.stats().expect("stats").total, 1);
}

#[test]
fn difference_2_replay_with_a_new_requested_at_returns_the_original_receipt() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let first = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("first");
    let mut retry = enqueue_request("TASK-1");
    retry.subject_dispatch.requested_at = Utc::now() + chrono::Duration::minutes(1);

    let replay = backend.enqueue_v2(retry).expect("replay");

    assert!(!replay.enqueued);
    assert_eq!(replay.entry_id, first.entry_id);
}

#[test]
fn same_key_with_different_content_is_an_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("first");
    let mut conflicting = enqueue_request("TASK-1");
    conflicting.repository = Some(reservation("DIFFERENT-BRANCH"));

    let message = invalid_params(backend.enqueue_v2(conflicting));

    assert!(
        message.contains("already bound to a different queue request"),
        "{message}"
    );
}

#[test]
fn replay_after_the_entry_finished_uses_the_history() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let first = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("first");
    backend.drop_entry(&first.entry_id).expect("drop");

    let replay = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("replay");
    assert!(!replay.enqueued);
    assert_eq!(replay.entry_id, first.entry_id);
    assert_eq!(replay.subject, first.subject);

    let mut conflicting = enqueue_request("TASK-1");
    conflicting.subject_dispatch.workflow_ref = "review".to_string();
    invalid_params(backend.enqueue_v2(conflicting));
}

#[test]
fn difference_3_second_add_for_a_live_task_returns_the_existing_entry() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let first = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("first");
    let warning = "subject task:TASK-1 already has an active generation; enqueue rejected";

    // Waiting.
    let mut second = enqueue_request("TASK-1");
    second.idempotency_key = Some("delivery-b".to_string());
    let rejected = backend.enqueue_v2(second).expect("second add");
    assert!(!rejected.enqueued);
    assert_eq!(rejected.entry_id, first.entry_id);
    assert_eq!(rejected.subject, first.subject);
    assert_eq!(rejected.warning.as_deref(), Some(warning));

    // Held.
    backend.hold(&first.entry_id).expect("hold");
    let mut third = enqueue_request("TASK-1");
    third.idempotency_key = Some("delivery-c".to_string());
    assert_eq!(
        backend.enqueue_v2(third).expect("third").entry_id,
        first.entry_id
    );

    // Running.
    edit_state(temp.path(), |state| {
        entry_mut(state, &first.entry_id).status = DispatchQueueEntryStatus::Assigned;
    });
    let mut fourth = enqueue_request("TASK-1");
    fourth.idempotency_key = Some("delivery-d".to_string());
    let rejected = backend.enqueue_v2(fourth).expect("fourth");
    assert_eq!(rejected.entry_id, first.entry_id);
    assert_eq!(rejected.warning.as_deref(), Some(warning));

    assert_eq!(backend.stats().expect("stats").total, 1);
}

#[test]
fn add_after_the_task_finished_gets_the_next_generation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let first = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("first");
    backend.drop_entry(&first.entry_id).expect("drop");
    let mut again = enqueue_request("TASK-1");
    again.idempotency_key = Some("delivery-again".to_string());

    let second = backend.enqueue_v2(again).expect("second");

    assert!(second.enqueued);
    assert_ne!(second.entry_id, first.entry_id);
    assert_eq!(second.subject.generation, 2);
}

#[test]
fn live_old_style_entry_gets_ticket_identity_when_it_blocks_an_add() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let old = backend
        .enqueue(task_dispatch("TASK-1", "coding"), None, None)
        .expect("old-style enqueue");

    let rejected = backend.enqueue_v2(enqueue_request("TASK-1")).expect("add");

    assert!(!rejected.enqueued);
    assert_eq!(rejected.entry_id, old.entry_id);
    assert_eq!(rejected.subject.qualified_id, "task:TASK-1");
    assert_eq!(rejected.subject.generation, 1);
    let entry = read_entry(temp.path(), &old.entry_id);
    assert_eq!(entry.subject_id.as_deref(), Some("task:TASK-1"));
    assert_eq!(entry.subject_generation, Some(1));
}

#[test]
fn difference_7_malformed_run_at_is_an_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let mut request = enqueue_request("TASK-BAD-TIME");
    request.run_at = Some("tomorrow morning".to_string());

    let message = invalid_params(backend.enqueue_v2(request));

    assert!(message.contains("RFC 3339"), "{message}");
    assert!(!queue_state_path(temp.path()).exists());
}

#[test]
fn deferred_entry_keeps_its_run_at() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let run_at = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    let mut request = enqueue_request("TASK-1");
    request.run_at = Some(run_at.clone());

    backend.enqueue_v2(request).expect("enqueue");

    let listed = backend.list(&[], None, None).expect("list");
    assert_eq!(listed.entries[0].run_at.as_deref(), Some(run_at.as_str()));
    assert_eq!(listed.stats.deferred, 1);
}

#[test]
fn canonical_ids_follow_v029() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let cases = [
        (
            SubjectRef::new("trigger_event", "github-delivery-1177"),
            "trigger_event:github-delivery-1177",
        ),
        (SubjectRef::task("task:TASK-1216"), "task:TASK-1216"),
        (
            SubjectRef::requirement("requirement:REQUIREMENT-075"),
            "requirement:REQUIREMENT-075",
        ),
    ];
    for (subject, expected) in cases {
        let added = backend
            .enqueue_v2(QueueEnqueueV2Request {
                subject_dispatch: SubjectDispatch::for_subject_with_metadata(
                    subject,
                    "nested-echo",
                    "test",
                    Utc::now(),
                ),
                idempotency_key: Some(format!("portal:{expected}")),
                repository: None,
                run_at: None,
                expire_after_secs: None,
            })
            .expect("enqueue");
        assert_eq!(added.subject.qualified_id, expected);
        assert_eq!(added.subject.generation, 1);
    }
}

#[test]
fn repository_is_stored_trimmed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let mut request = enqueue_request("TASK-1");
    let mut padded = reservation("TASK-1");
    padded.repository = format!("  {}  ", padded.repository);
    padded.head_ref = format!("{}  ", padded.head_ref);
    request.repository = Some(padded);

    let added = backend.enqueue_v2(request).expect("enqueue");

    assert_eq!(
        read_entry(temp.path(), &added.entry_id).repository,
        Some(reservation("TASK-1"))
    );
}

#[test]
fn invalid_requests_change_nothing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);

    let mut subjectless = enqueue_request("TASK-1");
    subjectless.subject_dispatch = SubjectDispatch::subjectless("coding", "test", Utc::now());
    invalid_params(backend.enqueue_v2(subjectless));

    let mut no_workflow = enqueue_request("TASK-1");
    no_workflow.subject_dispatch.workflow_ref = "  ".to_string();
    invalid_params(backend.enqueue_v2(no_workflow));

    let mut blank_key = enqueue_request("TASK-1");
    blank_key.idempotency_key = Some("   ".to_string());
    invalid_params(backend.enqueue_v2(blank_key));

    let mut long_key = enqueue_request("TASK-1");
    long_key.idempotency_key = Some("k".repeat(257));
    invalid_params(backend.enqueue_v2(long_key));

    let mut short_ref = enqueue_request("TASK-1");
    short_ref.repository.as_mut().unwrap().head_ref = "main".to_string();
    invalid_params(backend.enqueue_v2(short_ref));

    let mut blank_id = enqueue_request("TASK-1");
    blank_id.subject_dispatch.subject = Some(SubjectRef::task("   "));
    invalid_params(backend.enqueue_v2(blank_id));

    assert!(!temp.path().join(".animus").exists());
}

#[test]
fn retry_after_the_entry_expired_returns_the_original_receipt() {
    // v0.2.9 keeps an expired entry as a `dropped` row, so a retry with the
    // same key still finds it. Here the sweep and the key check happen in
    // one call, and the key check must see the entry the sweep just removed.
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let request = QueueEnqueueV2Request {
        run_at: Some("2020-01-01T00:00:00Z".to_string()),
        expire_after_secs: Some(1),
        ..enqueue_request("TASK-1")
    };
    let first = backend.enqueue_v2(request.clone()).expect("first");

    let retry = backend.enqueue_v2(request).expect("retry");

    assert!(!retry.enqueued);
    assert_eq!(retry.entry_id, first.entry_id);
    assert_eq!(backend.stats().expect("stats").total, 0);
}

#[test]
fn key_of_a_just_expired_entry_still_rejects_different_content() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let request = QueueEnqueueV2Request {
        run_at: Some("2020-01-01T00:00:00Z".to_string()),
        expire_after_secs: Some(1),
        ..enqueue_request("TASK-1")
    };
    backend.enqueue_v2(request.clone()).expect("first");

    let message = invalid_params(backend.enqueue_v2(QueueEnqueueV2Request {
        run_at: None,
        ..request
    }));

    assert_eq!(
        message,
        "idempotency_key is already bound to a different queue request"
    );
    // A refused call changes nothing: no new entry, and the expired one is
    // left for the next call's sweep.
    assert_eq!(backend.stats().expect("stats").total, 1);
}

#[test]
fn a_second_key_is_bound_to_the_entry_it_got_back() {
    // v0.2.9 binds every key it accepts, forever. Difference 3 only changes
    // which entry the second key gets; a retry must still replay it.
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let first = backend
        .enqueue_v2(enqueue_request("TASK-1"))
        .expect("first");
    let mut second = enqueue_request("TASK-1");
    second.idempotency_key = Some("second-producer".to_string());
    let receipt = backend.enqueue_v2(second.clone()).expect("second");
    assert_eq!(receipt.entry_id, first.entry_id);
    assert!(receipt.warning.is_some());

    // While the entry is live, and after it finished.
    let live_retry = backend.enqueue_v2(second.clone()).expect("live retry");
    assert_eq!(live_retry.entry_id, first.entry_id);
    assert!(!live_retry.enqueued);
    assert_eq!(live_retry.warning, None);
    backend.drop_entry(&first.entry_id).expect("drop");
    let late_retry = backend.enqueue_v2(second.clone()).expect("late retry");
    assert!(!late_retry.enqueued);
    assert_eq!(late_retry.entry_id, first.entry_id);
    assert_eq!(late_retry.subject, first.subject);
    assert_eq!(backend.stats().expect("stats").total, 0);

    // The key keeps its own content: different content is refused.
    second.subject_dispatch = task_dispatch("TASK-1", "review");
    let message = invalid_params(backend.enqueue_v2(second));
    assert!(message.contains("already bound"), "{message}");
}

#[test]
fn running_old_style_entry_does_not_block_a_ticketed_add() {
    // v0.2.9 gives old entries identity only while they wait or are held. A
    // running one belongs to a 0.6 daemon the guard now refuses, so the add
    // creates a new entry and the old one keeps its old-style handling.
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let old = backend
        .enqueue(task_dispatch("TASK-1", "coding"), None, None)
        .expect("old-style add");
    backend
        .lease(1, Some(vec!["legacy-workflow".to_string()]), None)
        .expect("old-style lease");

    let added = backend.enqueue_v2(enqueue_request("TASK-1")).expect("add");

    assert!(added.enqueued);
    assert_ne!(added.entry_id, old.entry_id);
    assert_eq!(added.subject.generation, 1);
    assert_eq!(added.warning, None);
    let old_entry = read_entry(temp.path(), &old.entry_id);
    assert!(!old_entry.is_ticketed());
    assert!(old_entry.extra_idempotency_keys.is_empty());
    backend
        .release_pending(&old.entry_id, "spawn-deferred")
        .expect("old-style put-back still works");
}

#[test]
fn expiry_that_no_timestamp_can_hold_is_an_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    for secs in [i64::MAX as u64, u64::MAX] {
        for run_at in [Some("2030-01-01T00:00:00Z"), None] {
            let mut request = enqueue_request("TASK-1");
            request.run_at = run_at.map(str::to_string);
            request.expire_after_secs = Some(secs);
            let message = invalid_params(backend.enqueue_v2(request));
            assert!(message.contains("expire_after_secs"), "{message}");
        }
    }
    // A valid number of seconds (about 285,000 years), but run_at plus that
    // is past the latest time a timestamp holds (year 262143).
    let mut request = enqueue_request("TASK-1");
    request.run_at = Some("2030-01-01T00:00:00Z".to_string());
    request.expire_after_secs = Some(9_000_000_000_000);
    invalid_params(backend.enqueue_v2(request));
    assert!(!queue_state_path(temp.path()).exists());

    let mut request = enqueue_request("TASK-1");
    request.run_at = Some("2030-01-01T00:00:00Z".to_string());
    request.expire_after_secs = Some(100 * 365 * 24 * 3600);
    assert!(
        backend
            .enqueue_v2(request)
            .expect("a century fits")
            .enqueued
    );
    backend.next_deadline().expect("sweeps still work");
}
