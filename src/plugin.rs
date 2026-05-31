//! Stdio JSON-RPC loop for the `animus-queue-default` plugin.
//!
//! Handles `initialize`, `$/ping`, `health/check`, `shutdown`, `exit`,
//! `--manifest` / `--help` CLI shortcuts, and the 10 `queue/*` methods.

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes as plugin_error_codes, HealthCheckResult, HealthStatus, InitializeParams,
    InitializeResult, KindCapability, PluginCapabilities, PluginInfo, PluginManifest, RpcError,
    RpcRequest, RpcResponse, PLUGIN_KIND_QUEUE, PROTOCOL_VERSION,
};
use animus_queue_protocol::{
    error_codes as queue_error_codes, QueueCapabilities, QueueCompletionRequest, QueueDropRequest,
    QueueEnqueueRequest, QueueEnqueueResponse, QueueHoldRequest, QueueLeaseRequest,
    QueueListRequest, QueueMarkAssignedRequest, QueueReleaseRequest, QueueReorderRequest, KIND,
    METHOD_QUEUE_COMPLETION, METHOD_QUEUE_DROP, METHOD_QUEUE_ENQUEUE, METHOD_QUEUE_HOLD,
    METHOD_QUEUE_LEASE, METHOD_QUEUE_LIST, METHOD_QUEUE_MARK_ASSIGNED, METHOD_QUEUE_RELEASE,
    METHOD_QUEUE_REORDER, METHOD_QUEUE_STATS, PROTOCOL_VERSION as QUEUE_PROTOCOL_VERSION,
};
use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

use crate::queue_service::{QueueBackend, QueueLeaseError};

const PLUGIN_NAME: &str = "animus-queue-default";
const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");
const PLUGIN_DESCRIPTION: &str =
    "Reference queue plugin for Animus v0.5 (file-backed dispatch queue with atomic lease).";

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
        env_required: Vec::new(),
        notification_buffer_size: None,
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
        METHOD_QUEUE_HOLD,
        METHOD_QUEUE_RELEASE,
        METHOD_QUEUE_DROP,
        METHOD_QUEUE_REORDER,
        METHOD_QUEUE_MARK_ASSIGNED,
        METHOD_QUEUE_COMPLETION,
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
        METHOD_QUEUE_HOLD => Some(handle_hold(id, request.params, &backend).await),
        METHOD_QUEUE_RELEASE => Some(handle_release(id, request.params, &backend).await),
        METHOD_QUEUE_DROP => Some(handle_drop(id, request.params, &backend).await),
        METHOD_QUEUE_REORDER => Some(handle_reorder(id, request.params, &backend).await),
        METHOD_QUEUE_MARK_ASSIGNED => {
            Some(handle_mark_assigned(id, request.params, &backend).await)
        }
        METHOD_QUEUE_COMPLETION => Some(handle_completion(id, request.params, &backend).await),
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
    let init: InitializeParams = match params
        .ok_or_else(|| invalid_params("missing params for initialize"))
        .and_then(|value| {
            serde_json::from_value(value)
                .map_err(|error| invalid_params(format!("invalid initialize params: {error}")))
        }) {
        Ok(value) => value,
        Err(error) => return RpcResponse::err(id, error),
    };

    let project_root = match extract_project_root(&init) {
        Ok(path) => path,
        Err(error) => return RpcResponse::err(id, error),
    };

    *backend.write().await = Some(QueueBackend::new(project_root));

    let capabilities = QueueCapabilities {
        priority_weighted: false,
        // No backend-side cap on lease batch size — file-locked state happily
        // handles batches of any size the daemon's capacity budgeter requests.
        // Hosts clamp `queue/lease.max` to this value; advertising `u32::MAX`
        // is the "effectively unlimited" sentinel for the reference plugin.
        max_lease_batch: u32::MAX,
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

    match backend.enqueue(request.subject_dispatch) {
        Ok(outcome) => to_value_response(
            id,
            &QueueEnqueueResponse {
                enqueued: outcome.enqueued,
                entry_id: outcome.entry_id,
                subject_id: outcome.subject_id,
            },
        ),
        Err(error) => internal_error_response(id, format!("queue/enqueue failed: {error:#}")),
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

    match backend.lease(request.max, request.workflow_ids) {
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
        Err(error) => not_pending_or_internal(id, &error, "queue/hold"),
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
        Err(error) => not_pending_or_internal(id, &error, "queue/release"),
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
        Err(error) => not_pending_or_internal(id, &error, "queue/mark_assigned"),
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

fn not_pending_or_internal(id: Option<Value>, error: &anyhow::Error, method: &str) -> RpcResponse {
    let msg = error.to_string();
    if msg.contains("not in the expected pre-mutation status") {
        return RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_ENTRY_NOT_PENDING,
                message: msg,
                data: None,
            },
        );
    }
    internal_error_response(id, format!("{method} failed: {error:#}"))
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
