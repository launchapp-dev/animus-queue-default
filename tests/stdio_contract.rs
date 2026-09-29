//! Contract test against the 0.7 daemon's rules: the real binary
//! over stdio, every reply decoded with the protocol's strict types, and the
//! checks the daemon applies copied from animus-cli:
//!
//! - `require_generation_fenced_queue` (`plugin_clients.rs`)
//! - `validate_queue_transition` (`coding_scheduler.rs`)

mod common;

use std::path::Path;

use animus_execution_protocol::ExecutionFence;
use animus_plugin_protocol::InitializeResult;
use animus_queue_protocol::{
    QueueCapabilities, QueueEnqueueV2Response, QueueLeaseMutationOutcome,
    QueueLeaseMutationResponse, QueueLeaseV2Response, KIND, METHOD_QUEUE_COMPLETION_V2,
    METHOD_QUEUE_ENQUEUE_V2, METHOD_QUEUE_LEASE_RECOVER, METHOD_QUEUE_LEASE_RENEW,
    METHOD_QUEUE_LEASE_V2, METHOD_QUEUE_RELEASE_PENDING_V2,
};
use common::{enqueue_request, PluginProcess};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

/// Copy of the daemon's capability gate.
fn require_generation_fenced_queue(initialize: &InitializeResult) {
    let declared = initialize
        .kind_capabilities
        .get(KIND)
        .expect("typed queue capabilities");
    let capabilities: QueueCapabilities =
        serde_json::from_value(declared.extra.clone()).expect("well-formed queue capabilities");
    assert!(capabilities.generation_fenced_leases_v1);
    assert!(capabilities.max_lease_batch >= 5);
    for method in [
        METHOD_QUEUE_ENQUEUE_V2,
        METHOD_QUEUE_LEASE_V2,
        METHOD_QUEUE_LEASE_RENEW,
        METHOD_QUEUE_LEASE_RECOVER,
        METHOD_QUEUE_COMPLETION_V2,
        METHOD_QUEUE_RELEASE_PENDING_V2,
    ] {
        assert!(
            initialize.capabilities.methods.iter().any(|m| m == method),
            "missing {method}"
        );
    }
}

/// Copy of the daemon's `validate_queue_transition`, plus the identity checks
/// it makes before accepting an updated fence.
fn assert_valid_transition(previous: &ExecutionFence, current: &ExecutionFence) {
    current.validate_coding().expect("coding fence");
    assert!(previous.same_execution_generation(current));
    assert_eq!(previous.repository, current.repository);
    let old = previous.queue_lease.as_ref().unwrap();
    let new = current.queue_lease.as_ref().unwrap();
    assert_eq!(
        old.entry_id, new.entry_id,
        "queue mutation changed entry id"
    );
    if old.owner_id == new.owner_id {
        assert_eq!(
            old.generation, new.generation,
            "renew changed lease generation"
        );
        assert!(
            new.expires_at >= old.expires_at,
            "renew shortened lease expiry"
        );
    } else {
        assert_eq!(
            new.generation,
            old.generation + 1,
            "recovery did not increment lease generation exactly once"
        );
    }
}

/// Send one call and decode its result strictly.
fn call<T: DeserializeOwned>(plugin: &mut PluginProcess, method: &str, params: Value) -> T {
    let response = plugin.request(method, params);
    assert!(response.get("error").is_none(), "{method}: {response}");
    serde_json::from_value(response["result"].clone())
        .unwrap_or_else(|error| panic!("{method} result does not match the protocol: {error}"))
}

fn started(project_root: &Path, envs: &[(&str, &str)]) -> PluginProcess {
    let mut plugin = PluginProcess::spawn(envs);
    let response = plugin.initialize(project_root, "1.2.0");
    let initialize: InitializeResult =
        serde_json::from_value(response["result"].clone()).expect("initialize result");
    require_generation_fenced_queue(&initialize);
    plugin
}

fn lease(plugin: &mut PluginProcess, owner: &str) -> QueueLeaseV2Response {
    call(
        plugin,
        METHOD_QUEUE_LEASE_V2,
        json!({
            "max": 5,
            "owner_id": owner,
            "workflow_ids": (1..=5).map(|n| format!("wf-{owner}-{n}")).collect::<Vec<_>>(),
        }),
    )
}

#[test]
fn full_ticket_life_cycle_follows_the_daemon_rules() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = started(temp.path(), &[]);

    for task in ["TASK-1", "TASK-2"] {
        let added: QueueEnqueueV2Response = call(
            &mut plugin,
            METHOD_QUEUE_ENQUEUE_V2,
            serde_json::to_value(enqueue_request(task)).unwrap(),
        );
        assert!(added.enqueued);
    }

    let response = lease(&mut plugin, "daemon-a");
    assert_eq!(response.leased.len(), 2);
    for fenced in &response.leased {
        fenced.validate().expect("FencedQueueEntry::validate");
        fenced.execution.validate_coding().expect("coding fence");
    }
    let first = response.leased[0].execution.clone();
    let second = response.leased[1].execution.clone();

    let renewed: QueueLeaseMutationResponse = call(
        &mut plugin,
        METHOD_QUEUE_LEASE_RENEW,
        json!({ "execution": first }),
    );
    assert_eq!(renewed.outcome, QueueLeaseMutationOutcome::Applied);
    let renewed = renewed.execution.expect("renewed fence");
    assert_valid_transition(&first, &renewed);

    let shorter: QueueLeaseMutationResponse = call(
        &mut plugin,
        METHOD_QUEUE_LEASE_RENEW,
        json!({ "execution": renewed, "ttl_secs": 1 }),
    );
    assert_valid_transition(&renewed, &shorter.execution.expect("fence"));

    let done: QueueLeaseMutationResponse = call(
        &mut plugin,
        METHOD_QUEUE_COMPLETION_V2,
        json!({ "execution": renewed, "status": "completed", "workflow_ref": "coding" }),
    );
    assert_eq!(done.outcome, QueueLeaseMutationOutcome::Applied);
    let again: QueueLeaseMutationResponse = call(
        &mut plugin,
        METHOD_QUEUE_COMPLETION_V2,
        json!({ "execution": renewed, "status": "completed" }),
    );
    assert_eq!(again.outcome, QueueLeaseMutationOutcome::AlreadyApplied);

    let put_back: QueueLeaseMutationResponse = call(
        &mut plugin,
        METHOD_QUEUE_RELEASE_PENDING_V2,
        json!({ "execution": second, "reason": "shutting down" }),
    );
    assert_eq!(put_back.outcome, QueueLeaseMutationOutcome::Applied);
    let next = lease(&mut plugin, "daemon-b");
    assert_eq!(next.leased.len(), 1);
    let next = &next.leased[0].execution;
    assert!(second.same_execution_generation(next));
    assert_eq!(next.queue_lease.as_ref().unwrap().generation, 2);
}

#[test]
fn takeover_after_expiry_follows_the_daemon_rules() {
    let temp = tempfile::tempdir().expect("tempdir");
    // Difference 6: the host forwards the declared setting to the plugin.
    let mut plugin = started(temp.path(), &[("ANIMUS_QUEUE_LEASE_TTL_SECS", "3")]);
    let _: QueueEnqueueV2Response = call(
        &mut plugin,
        METHOD_QUEUE_ENQUEUE_V2,
        serde_json::to_value(enqueue_request("TASK-1")).unwrap(),
    );
    let fence = lease(&mut plugin, "daemon-a").leased.remove(0).execution;
    let ticket = fence.queue_lease.as_ref().unwrap();
    let length = (ticket.expires_at - chrono::Utc::now()).num_milliseconds();
    assert!(length <= 3_000, "ticket length {length} ms");

    let early: QueueLeaseMutationResponse = call(
        &mut plugin,
        METHOD_QUEUE_LEASE_RECOVER,
        json!({ "execution": fence, "new_owner_id": "daemon-b" }),
    );
    assert_eq!(early.outcome, QueueLeaseMutationOutcome::LeaseStillLive);

    std::thread::sleep(std::time::Duration::from_millis(3_200));
    let recovered: QueueLeaseMutationResponse = call(
        &mut plugin,
        METHOD_QUEUE_LEASE_RECOVER,
        json!({ "execution": fence, "new_owner_id": "daemon-b" }),
    );
    assert_eq!(recovered.outcome, QueueLeaseMutationOutcome::Applied);
    let recovered = recovered.execution.expect("recovered fence");
    assert_valid_transition(&fence, &recovered);

    let stale: QueueLeaseMutationResponse = call(
        &mut plugin,
        METHOD_QUEUE_COMPLETION_V2,
        json!({ "execution": fence, "status": "completed" }),
    );
    assert_eq!(stale.outcome, QueueLeaseMutationOutcome::StaleFence);
}

#[test]
fn bad_input_is_invalid_params() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = started(temp.path(), &[]);

    for (method, params) in [
        (
            METHOD_QUEUE_LEASE_V2,
            json!({ "max": 1, "owner_id": "d", "workflow_ids": ["w"], "surprise": true }),
        ),
        (
            METHOD_QUEUE_LEASE_V2,
            json!({ "max": 6, "owner_id": "d", "workflow_ids": ["1", "2", "3", "4", "5", "6"] }),
        ),
        (METHOD_QUEUE_LEASE_RENEW, json!({})),
        (
            METHOD_QUEUE_ENQUEUE_V2,
            json!({ "subject_dispatch": { "workflow_ref": "coding" } }),
        ),
    ] {
        let response = plugin.request(method, params);
        assert_eq!(response["error"]["code"], -32602, "{method}: {response}");
    }
}
