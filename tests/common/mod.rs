//! Helpers shared by the integration tests. Each test file uses a subset.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use animus_queue_default::{
    load_queue_state, save_queue_state, DispatchQueueEntry, DispatchQueueState,
};
use animus_subject_protocol::{SubjectDispatch, SubjectRef};
use chrono::Utc;
use serde_json::{json, Value};

/// Load the queue file, apply `edit`, and save it. For setting up states the
/// public API can't reach directly (expired leases, old files).
pub fn edit_state(project_root: &Path, edit: impl FnOnce(&mut DispatchQueueState)) {
    let mut state = load_queue_state(project_root)
        .expect("load queue state")
        .unwrap_or_default();
    edit(&mut state);
    save_queue_state(project_root, &state).expect("save queue state");
}

/// The live entry with `entry_id`.
pub fn entry_mut<'a>(
    state: &'a mut DispatchQueueState,
    entry_id: &str,
) -> &'a mut DispatchQueueEntry {
    state
        .entries
        .iter_mut()
        .find(|entry| entry.entry_id == entry_id)
        .unwrap_or_else(|| panic!("no live entry {entry_id}"))
}

/// Read one live entry from the queue file.
pub fn read_entry(project_root: &Path, entry_id: &str) -> DispatchQueueEntry {
    load_queue_state(project_root)
        .expect("load queue state")
        .expect("queue state")
        .entries
        .into_iter()
        .find(|entry| entry.entry_id == entry_id)
        .unwrap_or_else(|| panic!("no live entry {entry_id}"))
}

/// Move a lease's expiry into the past.
pub fn expire_lease(project_root: &Path, entry_id: &str) {
    edit_state(project_root, |state| {
        entry_mut(state, entry_id).lease_expires_at =
            Some(Utc::now() - chrono::Duration::seconds(60));
    });
}

/// A task dispatch requested now.
pub fn task_dispatch(task_id: &str, workflow_ref: &str) -> SubjectDispatch {
    SubjectDispatch::for_subject_with_metadata(
        SubjectRef::task(task_id),
        workflow_ref,
        "integration-test",
        Utc::now(),
    )
}

/// A running plugin process driven over stdio, one request at a time.
pub struct PluginProcess {
    child: Child,
    stdin: ChildStdin,
    reader: BufReader<ChildStdout>,
    next_id: u64,
}

impl PluginProcess {
    /// Spawn the compiled plugin binary with extra environment variables.
    pub fn spawn(envs: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_animus-queue-default"));
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in envs {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawn plugin");
        let stdin = child.stdin.take().expect("stdin");
        let reader = BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            reader,
            next_id: 1,
        }
    }

    /// Send one request and return its full response frame.
    pub fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        writeln!(self.stdin, "{frame}").expect("write frame");
        self.stdin.flush().expect("flush frame");
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("read frame");
        let response: Value = serde_json::from_str(line.trim())
            .unwrap_or_else(|error| panic!("invalid JSON frame: {error}; raw: {line}"));
        assert_eq!(response["id"], id, "response id");
        response
    }

    /// `initialize` bound to `project_root`, announcing `protocol_version`.
    pub fn initialize(&mut self, project_root: &Path, protocol_version: &str) -> Value {
        self.request(
            "initialize",
            json!({
                "protocol_version": protocol_version,
                "host_info": { "name": "animus", "version": "0.1.0" },
                "capabilities": {},
                "init_extensions": {
                    "project_binding": { "project_root": project_root.to_string_lossy() }
                }
            }),
        )
    }
}

impl Drop for PluginProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
