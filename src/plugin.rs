//! Stdio JSON-RPC loop for the `animus-queue-default` plugin.
//!
//! Handles `initialize`, `$/ping`, `health/check`, `shutdown`, `exit`,
//! `--manifest` / `--help` CLI shortcuts, the old-style `queue/*` methods and
//! the six ticketed `queue/v2/*` methods.

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes as plugin_error_codes, EnvRequirement, HealthCheckResult, HealthStatus,
    InitializeParams, InitializeResult, KindCapability, PluginCapabilities, PluginInfo,
    PluginManifest, RpcError, RpcRequest, RpcResponse, PLUGIN_KIND_QUEUE, PROTOCOL_VERSION,
};
use animus_queue_protocol::{
    error_codes as queue_error_codes, QueueCapabilities, QueueCompletionRequest, QueueDropRequest,
    QueueEnqueueRequest, QueueEnqueueResponse, QueueHoldRequest, QueueLeaseRequest,
    QueueListRequest, QueueMarkAssignedRequest, QueueReleasePendingParams, QueueReleaseRequest,
    QueueReorderRequest, KIND, METHOD_QUEUE_COMPLETION, METHOD_QUEUE_DROP, METHOD_QUEUE_ENQUEUE,
    METHOD_QUEUE_HOLD, METHOD_QUEUE_LEASE, METHOD_QUEUE_LIST, METHOD_QUEUE_MARK_ASSIGNED,
    METHOD_QUEUE_NEXT_DEADLINE, METHOD_QUEUE_RELEASE, METHOD_QUEUE_RELEASE_PENDING,
    METHOD_QUEUE_REORDER, METHOD_QUEUE_STATS, PROTOCOL_VERSION as QUEUE_PROTOCOL_VERSION,
};
use animus_queue_protocol::{
    METHOD_QUEUE_COMPLETION_V2, METHOD_QUEUE_ENQUEUE_V2, METHOD_QUEUE_LEASE_RECOVER,
    METHOD_QUEUE_LEASE_RENEW, METHOD_QUEUE_LEASE_V2, METHOD_QUEUE_RELEASE_PENDING_V2,
};
use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

use crate::fenced_queue::MAX_LEASE_BATCH;
use crate::host_guard::check_host;
use crate::lease_ttl::{lease_ttl_from_env, LEASE_TTL_ENV};
use crate::queue_service::{
    QueueBackend, QueueCallError, QueueLeaseError, QueueMutationError, QueueReleasePendingError,
};

const PLUGIN_NAME: &str = "animus-queue-default";
const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");
const PLUGIN_DESCRIPTION: &str =
    "Reference queue plugin for Animus 0.7 (file-backed dispatch queue with generation-fenced leases).";

/// Stable entrypoint for the plugin process. Call from `#[tokio::main]` in
/// `main.rs`.
pub async fn run() -> Result<()> {
    if handle_cli_args() {
        return Ok(());
    }

    if io::stdin().is_terminal() {
        eprintln!("{PLUGIN_NAME} is a STDIO plugin; pipe JSON-RPC on stdin or pass --manifest");
        std::process::exit(2);
    }

    let backend: Arc<RwLock<Option<QueueBackend>>> = Arc::new(RwLock::new(None));
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));

    let mut stdin = tokio::io::stdin();
    // Streaming JSON-RPC frame reader. Reads raw bytes into a buffer and
    // peels off complete JSON values with `serde_json::Deserializer`,
    // independent of newline framing. This accepts both the canonical
    // NDJSON wire form and pretty-printed (multi-line) frames.
    let mut buffer: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 4096];
    loop {
        let n = stdin.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..n]);

        loop {
            // Skip leading whitespace before attempting to deserialize.
            let leading_ws = buffer
                .iter()
                .take_while(|b| b.is_ascii_whitespace())
                .count();
            if leading_ws > 0 {
                buffer.drain(..leading_ws);
            }
            if buffer.is_empty() {
                break;
            }

            let mut stream =
                serde_json::Deserializer::from_slice(&buffer).into_iter::<RpcRequest>();
            match stream.next() {
                Some(Ok(request)) => {
                    let consumed = stream.byte_offset();
                    drop(stream);
                    buffer.drain(..consumed);
                    let backend = backend.clone();
                    let stdout = stdout.clone();
                    tokio::spawn(async move {
                        handle_request(request, backend, stdout).await;
                    });
                }
                Some(Err(error)) if error.is_eof() => {
                    // Need more bytes for the current frame.
                    break;
                }
                Some(Err(error)) => {
                    tracing::warn!(plugin = PLUGIN_NAME, %error, "invalid JSON-RPC frame");
                    // Recover by discarding bytes up to the next newline so we
                    // can keep parsing any valid frames that arrived in the
                    // same read. If no newline is in sight, wait for more
                    // bytes — clearing the buffer here would drop unread
                    // partial frames the host may complete on the next write.
                    if let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
                        buffer.drain(..=pos);
                        // Loop to attempt parsing any remaining buffered frames.
                        continue;
                    }
                    break;
                }
                None => break,
            }
        }
    }
    Ok(())
}

fn handle_cli_args() -> bool {
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--manifest" | "-m" => {
                print_manifest();
                return true;
            }
            "--help" | "-h" => {
                eprintln!("{PLUGIN_NAME} {PLUGIN_VERSION} — STDIO queue plugin for Animus");
                eprintln!("Usage:");
                eprintln!("  {PLUGIN_NAME} --manifest    Print plugin manifest as JSON and exit");
                eprintln!("  {PLUGIN_NAME}               Run JSON-RPC loop on stdin/stdout");
                return true;
            }
            _ => {}
        }
    }
    false
}

fn print_manifest() {
    let manifest = PluginManifest {
        name: PLUGIN_NAME.to_string(),
        version: PLUGIN_VERSION.to_string(),
        plugin_kind: PLUGIN_KIND_QUEUE.to_string(),
        description: PLUGIN_DESCRIPTION.to_string(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        capabilities: queue_methods().into_iter().map(|m| m.to_string()).collect(),
        env_required: vec![EnvRequirement {
            name: LEASE_TTL_ENV.to_string(),
            description: Some(
                "Queue lease (ticket) length in seconds, 1-604800. Default 1800.".to_string(),
            ),
            sensitive: false,
            required: false,
        }],
        notification_buffer_size: None,
        plugin_kinds: Vec::new(),
        supports_mcp: None,
    };
    let mut stdout = io::stdout().lock();
    let _ = writeln!(
        stdout,
        "{}",
        serde_json::to_string(&manifest).expect("serialize manifest")
    );
    let _ = stdout.flush();
}

fn queue_methods() -> Vec<&'static str> {
    vec![
        METHOD_QUEUE_ENQUEUE,
        METHOD_QUEUE_LIST,
        METHOD_QUEUE_LEASE,
        METHOD_QUEUE_STATS,
        METHOD_QUEUE_NEXT_DEADLINE,
        METHOD_QUEUE_HOLD,
        METHOD_QUEUE_RELEASE,
        METHOD_QUEUE_RELEASE_PENDING,
        METHOD_QUEUE_DROP,
        METHOD_QUEUE_REORDER,
        METHOD_QUEUE_MARK_ASSIGNED,
        METHOD_QUEUE_COMPLETION,
        METHOD_QUEUE_ENQUEUE_V2,
        METHOD_QUEUE_LEASE_V2,
        METHOD_QUEUE_LEASE_RENEW,
        METHOD_QUEUE_LEASE_RECOVER,
        METHOD_QUEUE_COMPLETION_V2,
        METHOD_QUEUE_RELEASE_PENDING_V2,
        "health/check",
    ]
}

async fn handle_request(
    request: RpcRequest,
    backend: Arc<RwLock<Option<QueueBackend>>>,
    stdout: Arc<Mutex<tokio::io::Stdout>>,
) {
    let id = request.id.clone();
    let response = match request.method.as_str() {
        "initialize" => Some(handle_initialize(id, request.params, &backend).await),
        "initialized" => None,
        "$/ping" => Some(RpcResponse::ok(id, json!({}))),
        "health/check" => Some(health_check(id)),
        "shutdown" => Some(RpcResponse::ok(id, json!({}))),
        "exit" => std::process::exit(0),
        other if other.starts_with("$/") => None,
        METHOD_QUEUE_ENQUEUE => Some(handle_enqueue(id, request.params, &backend).await),
        METHOD_QUEUE_LIST => Some(handle_list(id, request.params, &backend).await),
        METHOD_QUEUE_LEASE => Some(handle_lease(id, request.params, &backend).await),
        METHOD_QUEUE_STATS => Some(handle_stats(id, &backend).await),
        METHOD_QUEUE_NEXT_DEADLINE => Some(handle_next_deadline(id, &backend).await),
        METHOD_QUEUE_HOLD => Some(handle_hold(id, request.params, &backend).await),
        METHOD_QUEUE_RELEASE => Some(handle_release(id, request.params, &backend).await),
        METHOD_QUEUE_RELEASE_PENDING => {
            Some(handle_release_pending(id, request.params, &backend).await)
        }
        METHOD_QUEUE_DROP => Some(handle_drop(id, request.params, &backend).await),
        METHOD_QUEUE_REORDER => Some(handle_reorder(id, request.params, &backend).await),
        METHOD_QUEUE_MARK_ASSIGNED => {
            Some(handle_mark_assigned(id, request.params, &backend).await)
        }
        METHOD_QUEUE_COMPLETION => Some(handle_completion(id, request.params, &backend).await),
        METHOD_QUEUE_ENQUEUE_V2 => Some(
            handle_v2(
                id,
                request.params,
                &backend,
                METHOD_QUEUE_ENQUEUE_V2,
                QueueBackend::enqueue_v2,
            )
            .await,
        ),
        METHOD_QUEUE_LEASE_V2 => Some(
            handle_v2(
                id,
                request.params,
                &backend,
                METHOD_QUEUE_LEASE_V2,
                QueueBackend::lease_v2,
            )
            .await,
        ),
        METHOD_QUEUE_LEASE_RENEW => Some(
            handle_v2(
                id,
                request.params,
                &backend,
                METHOD_QUEUE_LEASE_RENEW,
                QueueBackend::renew_lease,
            )
            .await,
        ),
        METHOD_QUEUE_LEASE_RECOVER => Some(
            handle_v2(
                id,
                request.params,
                &backend,
                METHOD_QUEUE_LEASE_RECOVER,
                QueueBackend::recover_lease,
            )
            .await,
        ),
        METHOD_QUEUE_COMPLETION_V2 => Some(
            handle_v2(
                id,
                request.params,
                &backend,
                METHOD_QUEUE_COMPLETION_V2,
                QueueBackend::completion_v2,
            )
            .await,
        ),
        METHOD_QUEUE_RELEASE_PENDING_V2 => Some(
            handle_v2(
                id,
                request.params,
                &backend,
                METHOD_QUEUE_RELEASE_PENDING_V2,
                QueueBackend::release_pending_v2,
            )
            .await,
        ),
        other => Some(RpcResponse::err(
            id,
            RpcError {
                code: plugin_error_codes::METHOD_NOT_FOUND,
                message: format!("method '{other}' not implemented by {PLUGIN_NAME}"),
                data: None,
            },
        )),
    };

    if let Some(response) = response {
        write_frame(&stdout, &response).await;
    }
}

async fn write_frame<T: serde::Serialize>(stdout: &Arc<Mutex<tokio::io::Stdout>>, frame: &T) {
    if let Ok(mut payload) = serde_json::to_string(frame) {
        payload.push('\n');
        let mut guard = stdout.lock().await;
        let _ = guard.write_all(payload.as_bytes()).await;
        let _ = guard.flush().await;
    }
}

fn health_check(id: Option<Value>) -> RpcResponse {
    match serde_json::to_value(HealthCheckResult {
        status: HealthStatus::Healthy,
        uptime_ms: None,
        memory_usage_bytes: None,
        last_error: None,
    }) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => {
            internal_error_response(id, format!("failed to encode health result: {error}"))
        }
    }
}

// ============================================================
// initialize
// ============================================================

async fn handle_initialize(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let Some(params) = params else {
        return RpcResponse::err(id, invalid_params("missing params for initialize"));
    };
    // Refuse 0.6.x and older hosts first, before the project binding is even
    // read, so a refused host never reaches the queue files.
    let host_protocol = params.get("protocol_version").and_then(Value::as_str);
    let host_version = params
        .get("host_info")
        .and_then(|host_info| host_info.get("version"))
        .and_then(Value::as_str);
    if let Err(error) = check_host(host_protocol, host_version) {
        return RpcResponse::err(id, error);
    }
    let init: InitializeParams = match serde_json::from_value(params) {
        Ok(value) => value,
        Err(error) => {
            return RpcResponse::err(
                id,
                invalid_params(format!("invalid initialize params: {error}")),
            );
        }
    };

    let project_root = match extract_project_root(&init) {
        Ok(path) => path,
        Err(error) => return RpcResponse::err(id, error),
    };

    *backend.write().await =
        Some(QueueBackend::new(project_root).with_lease_ttl(lease_ttl_from_env()));

    // Identical to animus-postgres v0.2.9 and animus-queue-postgres v0.2.0.
    // The 0.7 daemon requires the flag and a batch of at least 5.
    let capabilities = QueueCapabilities {
        priority_weighted: false,
        max_lease_batch: MAX_LEASE_BATCH as u32,
        generation_fenced_leases_v1: true,
    };
    let extra = serde_json::to_value(capabilities).unwrap_or(Value::Null);
    let mut kind_capabilities = std::collections::HashMap::new();
    kind_capabilities.insert(
        KIND.to_string(),
        KindCapability {
            crate_version: QUEUE_PROTOCOL_VERSION.to_string(),
            extra,
        },
    );

    let result = InitializeResult {
        protocol_version: PROTOCOL_VERSION.to_string(),
        plugin_info: PluginInfo {
            name: PLUGIN_NAME.to_string(),
            version: PLUGIN_VERSION.to_string(),
            plugin_kind: PLUGIN_KIND_QUEUE.to_string(),
            plugin_kinds: Vec::new(),
            description: Some(PLUGIN_DESCRIPTION.to_string()),
        },
        capabilities: PluginCapabilities {
            methods: queue_methods()
                .into_iter()
                .map(ToString::to_string)
                .collect(),
            streaming: false,
            progress: false,
            cancellation: false,
            projections: Vec::new(),
            subject_kinds: Vec::new(),
            mcp_tools: Vec::new(),
        },
        kind_capabilities,
    };

    match serde_json::to_value(result) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => {
            internal_error_response(id, format!("failed to encode initialize result: {error}"))
        }
    }
}

fn extract_project_root(init: &InitializeParams) -> std::result::Result<PathBuf, RpcError> {
    let binding = init
        .init_extensions
        .get("project_binding")
        .ok_or_else(|| RpcError {
            code: queue_error_codes::PROJECT_BINDING_MISMATCH,
            message: "init_extensions.project_binding is required to bind a project root"
                .to_string(),
            data: None,
        })?;

    let project_root = binding
        .get("project_root")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError {
            code: queue_error_codes::PROJECT_BINDING_MISMATCH,
            message: "init_extensions.project_binding.project_root must be a string".to_string(),
            data: None,
        })?;

    Ok(PathBuf::from(project_root))
}

// ============================================================
// queue/enqueue
// ============================================================

async fn handle_enqueue(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };

    let request: QueueEnqueueRequest = match parse_params(id.clone(), params, "queue/enqueue") {
        Ok(req) => req,
        Err(response) => return response,
    };

    match backend.enqueue(
        request.subject_dispatch,
        request.run_at,
        request.expire_after_secs,
    ) {
        Ok(outcome) => to_value_response(
            id,
            &QueueEnqueueResponse {
                enqueued: outcome.enqueued,
                entry_id: outcome.entry_id,
                subject_id: outcome.subject_id,
                warning: outcome.warning,
            },
        ),
        Err(error) => call_error_response(id, error, "queue/enqueue"),
    }
}

// ============================================================
// queue/list
// ============================================================

async fn handle_list(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };

    let request: QueueListRequest = match params {
        Some(value) => match serde_json::from_value(value) {
            Ok(req) => req,
            Err(error) => {
                return RpcResponse::err(
                    id,
                    invalid_params(format!("invalid queue/list params: {error}")),
                );
            }
        },
        None => QueueListRequest::default(),
    };

    match backend.list(&request.status, request.limit, request.offset) {
        Ok(response) => to_value_response(id, &response),
        Err(error) => internal_error_response(id, format!("queue/list failed: {error:#}")),
    }
}

// ============================================================
// queue/lease
// ============================================================

async fn handle_lease(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };

    let request: QueueLeaseRequest = match parse_params(id.clone(), params, "queue/lease") {
        Ok(req) => req,
        Err(response) => return response,
    };

    let exclude_subjects = request
        .exclude_subjects
        .map(|ids| ids.into_iter().map(|id| id.0).collect::<Vec<String>>());
    match backend.lease(request.max, request.workflow_ids, exclude_subjects) {
        Ok(response) => to_value_response(id, &response),
        Err(QueueLeaseError::WorkflowIdCountMismatch { expected, actual }) => RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_LEASE_WORKFLOW_ID_COUNT_MISMATCH,
                message: format!("workflow_ids.len()={actual} did not match max={expected}"),
                data: Some(json!({"expected": expected, "actual": actual})),
            },
        ),
        Err(QueueLeaseError::Backend(error)) => {
            internal_error_response(id, format!("queue/lease failed: {error:#}"))
        }
    }
}

// ============================================================
// queue/stats
// ============================================================

async fn handle_stats(
    id: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    match backend.stats() {
        Ok(stats) => to_value_response(id, &stats),
        Err(error) => internal_error_response(id, format!("queue/stats failed: {error:#}")),
    }
}

// ============================================================
// queue/next_deadline
// ============================================================

async fn handle_next_deadline(
    id: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    match backend.next_deadline() {
        Ok(resp) => to_value_response(id, &resp),
        Err(error) => internal_error_response(id, format!("queue/next_deadline failed: {error:#}")),
    }
}

// ============================================================
// queue/hold + queue/release + queue/drop + queue/reorder
// + queue/mark_assigned + queue/completion
// ============================================================

async fn handle_hold(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    let request: QueueHoldRequest = match parse_params(id.clone(), params, "queue/hold") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match backend.hold(&request.entry_id) {
        Ok(response) => to_value_response(id, &response),
        Err(error) => mutation_error_response(id, error, "queue/hold"),
    }
}

async fn handle_release(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    let request: QueueReleaseRequest = match parse_params(id.clone(), params, "queue/release") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match backend.release(&request.entry_id) {
        Ok(response) => to_value_response(id, &response),
        Err(error) => mutation_error_response(id, error, "queue/release"),
    }
}

async fn handle_release_pending(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    let request: QueueReleasePendingParams =
        match parse_params(id.clone(), params, "queue/release_pending") {
            Ok(req) => req,
            Err(response) => return response,
        };
    match backend.release_pending(&request.entry_id, &request.reason) {
        Ok(response) => to_value_response(id, &response),
        // TODO(codex-p2): consider QUEUE_ENTRY_NOT_FOUND (-32201) here. The
        // v0.5 fold-in brief specified -32602 invalid_params for the missing
        // entry_id case so we honor that for now; revisit when queue clients
        // need to distinguish a stale entry id from a malformed request.
        Err(QueueReleasePendingError::NotFound { entry_id }) => RpcResponse::err(
            id,
            RpcError {
                code: plugin_error_codes::INVALID_PARAMS,
                message: format!("entry_id not found: {entry_id}"),
                data: None,
            },
        ),
        Err(error @ QueueReleasePendingError::Fenced { .. }) => RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_STALE_FENCE,
                message: error.to_string(),
                data: None,
            },
        ),
        Err(QueueReleasePendingError::NotAssigned {
            entry_id,
            actual_state,
        }) => RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_ENTRY_NOT_ASSIGNED,
                message: format!(
                    "entry {entry_id} is in state '{actual_state}', expected 'assigned'"
                ),
                data: Some(json!({ "actual_state": actual_state })),
            },
        ),
        Err(QueueReleasePendingError::Backend(error)) => {
            internal_error_response(id, format!("queue/release_pending failed: {error:#}"))
        }
    }
}

async fn handle_drop(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    let request: QueueDropRequest = match parse_params(id.clone(), params, "queue/drop") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match backend.drop_entry(&request.entry_id) {
        Ok(response) => to_value_response(id, &response),
        Err(error) => internal_error_response(id, format!("queue/drop failed: {error:#}")),
    }
}

async fn handle_reorder(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    let request: QueueReorderRequest = match parse_params(id.clone(), params, "queue/reorder") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match backend.reorder(&request.entry_ids) {
        Ok(response) => to_value_response(id, &response),
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_REORDER_FAILED,
                message: format!("queue/reorder failed: {error:#}"),
                data: None,
            },
        ),
    }
}

async fn handle_mark_assigned(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    let request: QueueMarkAssignedRequest =
        match parse_params(id.clone(), params, "queue/mark_assigned") {
            Ok(req) => req,
            Err(response) => return response,
        };
    match backend.mark_assigned(&request.entry_id, request.workflow_id) {
        Ok(response) => to_value_response(id, &response),
        Err(error) => mutation_error_response(id, error, "queue/mark_assigned"),
    }
}

async fn handle_completion(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> RpcResponse {
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    let request: QueueCompletionRequest = match parse_params(id.clone(), params, "queue/completion")
    {
        Ok(req) => req,
        Err(response) => return response,
    };
    match backend.completion(
        &request.entry_id,
        &request.status,
        request.workflow_ref.as_deref(),
        request.workflow_id.as_deref(),
    ) {
        Ok(response) => to_value_response(id, &response),
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: plugin_error_codes::INVALID_PARAMS,
                message: format!("queue/completion failed: {error:#}"),
                data: None,
            },
        ),
    }
}

// ============================================================
// queue/v2/*
// ============================================================

/// Shared handler for the ticketed methods. Params are the protocol's strict
/// request types (unknown fields are rejected); bad input is `-32602`, and
/// ticket problems come back as normal outcomes inside the result.
async fn handle_v2<Req, Resp>(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
    method: &str,
    call: fn(&QueueBackend, Req) -> std::result::Result<Resp, QueueCallError>,
) -> RpcResponse
where
    Req: serde::de::DeserializeOwned,
    Resp: serde::Serialize,
{
    let backend = match require_backend(id.clone(), backend).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    let request: Req = match parse_params(id.clone(), params, method) {
        Ok(req) => req,
        Err(response) => return response,
    };
    match call(&backend, request) {
        Ok(response) => to_value_response(id, &response),
        Err(error) => call_error_response(id, error, method),
    }
}

// ============================================================
// helpers
// ============================================================

#[allow(clippy::result_large_err)]
async fn require_backend(
    id: Option<Value>,
    backend: &Arc<RwLock<Option<QueueBackend>>>,
) -> std::result::Result<QueueBackend, RpcResponse> {
    match backend.read().await.as_ref().cloned() {
        Some(b) => Ok(b),
        None => Err(RpcResponse::err(
            id,
            RpcError {
                code: plugin_error_codes::PLUGIN_NOT_INITIALIZED,
                message: format!("{PLUGIN_NAME} received a queue/* method before initialize"),
                data: None,
            },
        )),
    }
}

#[allow(clippy::result_large_err)]
fn parse_params<T: serde::de::DeserializeOwned>(
    id: Option<Value>,
    params: Option<Value>,
    method: &str,
) -> std::result::Result<T, RpcResponse> {
    let value = params.ok_or_else(|| {
        RpcResponse::err(
            id.clone(),
            invalid_params(format!("missing params for {method}")),
        )
    })?;
    serde_json::from_value::<T>(value).map_err(|error| {
        RpcResponse::err(
            id,
            invalid_params(format!("invalid {method} params: {error}")),
        )
    })
}

fn to_value_response<T: serde::Serialize>(id: Option<Value>, value: &T) -> RpcResponse {
    match serde_json::to_value(value) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => internal_error_response(id, format!("failed to encode response: {error}")),
    }
}

fn mutation_error_response(
    id: Option<Value>,
    error: QueueMutationError,
    method: &str,
) -> RpcResponse {
    let code = match &error {
        QueueMutationError::NotPending { .. } => queue_error_codes::QUEUE_ENTRY_NOT_PENDING,
        QueueMutationError::Fenced { .. } => queue_error_codes::QUEUE_STALE_FENCE,
        QueueMutationError::Backend(error) => {
            return internal_error_response(id, format!("{method} failed: {error:#}"));
        }
    };
    RpcResponse::err(
        id,
        RpcError {
            code,
            message: error.to_string(),
            data: None,
        },
    )
}

fn call_error_response(id: Option<Value>, error: QueueCallError, method: &str) -> RpcResponse {
    match error {
        QueueCallError::InvalidParams(message) => RpcResponse::err(id, invalid_params(message)),
        QueueCallError::Backend(error) => {
            internal_error_response(id, format!("{method} failed: {error:#}"))
        }
    }
}

fn invalid_params(message: impl Into<String>) -> RpcError {
    RpcError {
        code: plugin_error_codes::INVALID_PARAMS,
        message: message.into(),
        data: None,
    }
}

fn internal_error_response(id: Option<Value>, message: impl Into<String>) -> RpcResponse {
    RpcResponse::err(
        id,
        RpcError {
            code: plugin_error_codes::INTERNAL_ERROR,
            message: message.into(),
            data: None,
        },
    )
}
