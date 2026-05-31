//! End-to-end stdio smoke test against the compiled plugin binary.
//!
//! Spawns the `animus-queue-default` binary, sends `initialize` + a couple of
//! `queue/*` frames, asserts the responses.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use serde_json::{json, Value};

fn binary_path() -> PathBuf {
    // CARGO_BIN_EXE_<name> is exported by cargo for integration tests.
    PathBuf::from(env!("CARGO_BIN_EXE_animus-queue-default"))
}

#[test]
fn manifest_prints_valid_json() {
    let output = Command::new(binary_path())
        .arg("--manifest")
        .output()
        .expect("spawn manifest");
    assert!(
        output.status.success(),
        "manifest exit: {:?}",
        output.status
    );
    let manifest: Value = serde_json::from_slice(&output.stdout).expect("parse manifest JSON");
    assert_eq!(manifest["name"], "animus-queue-default");
    assert_eq!(manifest["plugin_kind"], "queue");
    let methods = manifest["capabilities"]
        .as_array()
        .expect("capabilities array");
    assert!(methods.iter().any(|v| v == "queue/lease"));
    assert!(methods.iter().any(|v| v == "queue/enqueue"));
}

#[test]
fn stdio_initialize_then_enqueue_and_lease() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut child = Command::new(binary_path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn plugin");

    let stdin = child.stdin.as_mut().expect("stdin");
    let stdout = child.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);

    // 1. initialize
    let init_frame = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocol_version": "1.1.0",
            "host_info": { "name": "animus", "version": "0.5.0" },
            "capabilities": {},
            "init_extensions": {
                "project_binding": {
                    "project_root": temp.path().to_string_lossy()
                }
            }
        }
    });
    writeln!(stdin, "{init_frame}").expect("write init");
    stdin.flush().expect("flush init");

    let init_response = read_frame(&mut reader);
    assert_eq!(init_response["id"], 1);
    assert!(init_response["result"]["protocol_version"].is_string());

    // 2. enqueue
    let enqueue_frame = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "queue/enqueue",
        "params": {
            "subject_dispatch": {
                "subject": { "kind": "task", "id": "TASK-1" },
                "workflow_ref": "standard",
                "trigger_source": "smoke-test",
                "requested_at": "2026-05-31T00:00:00Z"
            }
        }
    });
    writeln!(stdin, "{enqueue_frame}").expect("write enqueue");
    stdin.flush().expect("flush enqueue");
    let enqueue_response = read_frame(&mut reader);
    assert_eq!(enqueue_response["id"], 2);
    let entry_id = enqueue_response["result"]["entry_id"]
        .as_str()
        .expect("entry_id present")
        .to_string();
    assert!(enqueue_response["result"]["enqueued"].as_bool().unwrap());

    // 3. lease
    let lease_frame = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "queue/lease",
        "params": { "max": 1, "workflow_ids": ["wf-smoke"] }
    });
    writeln!(stdin, "{lease_frame}").expect("write lease");
    stdin.flush().expect("flush lease");
    let lease_response = read_frame(&mut reader);
    assert_eq!(lease_response["id"], 3);
    let leased = lease_response["result"]["leased"]
        .as_array()
        .expect("leased array");
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0]["entry_id"].as_str(), Some(entry_id.as_str()));
    assert_eq!(leased[0]["workflow_id"], "wf-smoke");
    assert_eq!(leased[0]["status"], "assigned");

    // 4. exit
    let exit_frame = json!({ "jsonrpc": "2.0", "id": 4, "method": "exit" });
    let _ = writeln!(stdin, "{exit_frame}");
    let _ = stdin.flush();
    let _ = child.wait();
}

fn read_frame<R: BufRead>(reader: &mut R) -> Value {
    let mut line = String::new();
    reader.read_line(&mut line).expect("read line");
    serde_json::from_str(line.trim()).unwrap_or_else(|error| {
        panic!("invalid JSON frame: {error}; raw: {line}");
    })
}
