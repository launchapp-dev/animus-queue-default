# animus-queue-default v0.4.0 (ticketed queue) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task by task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `animus-queue-default` the generation-fenced ("ticketed") leases that the Animus 0.7 daemon requires. It should behave like `animus-postgres` v0.2.9 except in the seven agreed places, and refuse Animus 0.6.x hosts, which must stay on queue v0.3.3.

**Architecture:**

- **Storage:** the file-backed queue keeps its `queue.json` layout. It only adds fields, a format marker and per-task counters, so v0.3.3 can still read it. Finished entries move to an append-only `queue-history.jsonl`.
- **New module:** a `fenced_queue` module implements the six `queue/v2/*` methods on the existing `QueueBackend`, under the same file lock.
- **Old-style methods:** they stay, brought to v0.2.9 behaviour.
- **Guard:** an `initialize` guard rejects hosts announcing plugin protocol below 1.1.0.

**Tech Stack:** Rust 2021, tokio stdio JSON-RPC, `animus-protocol` crates at tag `v0.7.0-rc.43` (plugin, queue, subject, execution), `fs2` file locks, `sha2`, `semver`, `tempfile` for tests.

**Spec:** `docs/superpowers/specs/2026-09-28-fenced-leases-design.md` (approved). Read it before Task 1. Where this plan and the spec disagree, the spec wins; stop and ask.

**Reference implementation:** `animus-postgres` v0.2.9 `src/queue.ts`. Line numbers cited below refer to that file.

**Provenance:** every code block in Tasks 1–13 was built and tested in a scratch copy of this repo before this plan was written. The finished tree passes `cargo fmt --check`, `cargo clippy --all-targets -D warnings`, 138 tests, and `cargo build --release`. Patches apply in order on top of the spec commit `889d1fb`.

## Global Constraints

- **Version:** crate version `0.4.0`.
- **Protocol:** `animus-plugin-protocol`, `animus-queue-protocol`, `animus-subject-protocol` and `animus-execution-protocol` from `https://github.com/launchapp-dev/animus-protocol` at tag `v0.7.0-rc.43`.
- **Host guard:**
  - Refuse `initialize` when the host's `protocol_version` is missing, unparseable, or below `1.1.0` (semver comparison, so pre-releases of 1.1.0 are below).
  - Refuse before any file access.
  - The message is exactly: animus-queue-default v0.4.0 requires Animus 0.7 or newer. This Animus is 0.6 or older (plugin protocol 1.0.0). Install the queue version made for it: `animus plugin install launchapp-dev/animus-queue-default@v0.3.3` (the version in parentheses is whatever the host sent, or "not sent").
- **Capabilities:** `priority_weighted: false`, `max_lease_batch: 5`, `generation_fenced_leases_v1: true`; `capabilities.methods` lists all six `queue/v2/*` methods.
- **Ticket length:**
  - Set by `ANIMUS_QUEUE_LEASE_TTL_SECS`: default 1800, valid 1..=604800, anything else gives the default.
  - Declared in the manifest's `env_required` with `required: false` and `sensitive: false`.
- **Rollback compatibility of `queue.json`:**
  - Only add fields.
  - `task_id` stays a required string.
  - Status words stay `pending`, `assigned` and `held`.
  - Subjectless entries are rejected.
- **`queue.json` format:**
  - The file carries `format_version: 2`; a file without it is format 0.
  - A version above 2 is refused.
  - The file is written atomically (temp file, fsync, rename) and never deleted.
- **History:** finishing an entry appends and fsyncs its `queue-history.jsonl` line before `queue.json` is replaced. Readers take the first line per `entry_id` and skip unreadable lines.
- **Behaviour:** `animus-postgres` v0.2.9 everywhere except:
  1. "Done" and "put back" with an expired ticket are accepted if the owner and numbers still match.
  2. The idempotency hash ignores `dispatch.requested_at`.
  3. A ticketed add for a task with a waiting, running or held entry returns that entry with a warning.
  4. Old-style lease, mark-assigned and put-back touch only old-style entries.
  5. Old entries get ticket identity at their first ticketed hand-out (or when difference 3 returns them).
  6. `ANIMUS_QUEUE_LEASE_TTL_SECS` is declared in the manifest.
  7. A malformed `run_at` on a ticketed add is an error.
- **Tests:** tests use `tempfile` directories only. The manual end-to-end task uses an isolated `HOME`. Never read or write the real `~/.animus`.
- **Gates** before every commit:
  - `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test --all-features` (plus `cargo build --release` in Task 14).
  - Then `codex review --uncommitted` until it reports no `[P1]`.
- **Attribution:**
  - Never add Claude as a co-author and never include a Claude session link or ID in anything shipped.
  - That means no `Co-Authored-By: Claude …` trailer, no `Claude-Session: …` trailer, no `https://claude.ai/code/session_…` link, and no "Generated with Claude Code" line.
  - This applies to commits, PR titles, bodies and comments, including anything a subagent writes.
- **Release:**
  - Work on branch `feat/fenced-leases`, and open the PR into `main`.
  - Do not push a tag. The owner says when to tag `v0.4.0`.

## Before you start

- Work in `~/Animus_projects/animus-queue-default` on branch `feat/fenced-leases`. It was cut from tag `v0.3.3` and has one commit on top: the spec, `889d1fb`.
- Confirm a clean tree with `git status --short`, which should print nothing, and `git log --oneline -1`, which should print `889d1fb docs: design spec for ticketed queue (v0.4.0)`.
- Git dependencies need network access on the first build.
- When a step says "Apply to `path`", the block is a unified diff. Save it to a file and run `git apply <file>`, or make the same edits by hand. When a step says "Create" or "Replace the whole of", write the block as the file's full content.

## File map

| File | Responsibility | Task |
|---|---|---|
| `Cargo.toml` | version, protocol tag, new deps (`sha2`, `semver`, `animus-execution-protocol`), dev-dep `queue_v033` | 1, 2, 3, 12 |
| `src/identity.rs` | canonical `<kind>:<id>` and legacy subject keys (v0.2.9 port) | 2 |
| `src/request_hash.rs` | canonical-JSON sha256 used by idempotency keys | 2 |
| `src/host_guard.rs` | refuse hosts below plugin protocol 1.1.0 | 3 |
| `src/lease_ttl.rs` | ticket-length setting | 4 |
| `src/dispatch_queue_state.rs` | entry and file shapes, ticket fields, `execution_fence()` | 5, 7 |
| `src/dispatch_queue_store.rs` | load/save with format marker, fsync and atomic replace | 5 |
| `src/queue_history.rs` | `queue-history.jsonl` append and lookup | 6 |
| `src/queue_service.rs` | `QueueBackend` and old-style methods | 1, 4, 6, 7, 9 |
| `src/fenced_queue.rs` | the six `queue/v2/*` methods | 8, 9, 10 |
| `src/plugin.rs` | stdio JSON-RPC, handshake, capabilities, method routing | 1, 3, 4, 7, 11 |
| `tests/common/mod.rs` | stdio driver and fixtures shared by integration tests | 3, 6, 7, 8 |
| `tests/*.rs` | one file per area; see each task | 3–12 |
| `README.md`, `docs/releases/v0.4.0.md` | user docs | 13 |

## Notes on reading the spec

These came up while building the scratch copy. Tests pin notes 1 and 2.

1. **`expired_lease_recovery_required`:** spec §7.1 names this reason for expired tickets.
   - v0.2.9 never emits it, because `leaseV2` only considers waiting rows. This plan follows v0.2.9: an expired ticket is neither handed out nor listed as blocked.
   - The 0.7 daemon recovers such tasks from its own records with `queue/v2/lease/recover`.
   - Test: `fenced_lease::expired_ticket_is_not_handed_out_again`.
2. **Renew never moves the expiry earlier** (spec §7.1, §8.2).
   - v0.2.9 sets exactly now + ttl, which could move it earlier only if a caller asked for a shorter `ttl_secs`. The daemon rejects a shortened expiry.
   - Implemented as the later of the two.
   - Test: `fenced_tickets::renew_never_moves_the_expiry_earlier`.
3. **Branch collisions use the protocol's `collision_key()`**, as the 0.7 daemon does.
   - It trims the repository; v0.2.9 doesn't. They agree on every reservation this queue stores, because the ticketed add trims them.
4. **An empty stored workflow id counts as none** when handing out, as v0.2.9's `resolveLeaseWorkflowId` does for old-style leases.
5. **Retrying a relative `--at` with an idempotency key:** such a retry computes a new `run_at`, so its content differs and it is refused ("idempotency_key is already bound to a different queue request"). v0.2.9 does the same; difference 2 only drops `requested_at`.

---

### Task 1: Move to the rc.43 protocol and reject subjectless entries

The 0.7 CLI speaks `animus-protocol` `v0.7.0-rc.43`. Moving the three protocol crates to that tag breaks the build in three places: the manifest and plugin-info types grew fields, `QueueCapabilities` gained `generation_fenced_leases_v1`, and `SubjectDispatch::subject` became an `Option`.

A dispatch with no subject would make the whole `queue.json` unreadable for v0.3.3, which breaks rollback (spec §3.3), so the old-style add now refuses one with invalid params before it touches any file. This task also adds `animus-execution-protocol` (the fence types) at the same tag.

For this task the failing test is the build itself, plus the unit test `enqueue_rejects_subjectless_dispatch` inside the Step 3 patch.

**Files:**

- Modify: `Cargo.toml`
- Modify: `src/dispatch_queue_state.rs`
- Modify: `src/plugin.rs`
- Modify: `src/queue_service.rs`
- Modify: `Cargo.lock` (cargo updates it; commit the result)

**Interfaces:**

- Produces (`src/queue_service.rs`):
  - `pub enum QueueCallError { InvalidParams(String), Backend(#[from] anyhow::Error) }`. Every later task returns it for "bad caller input" vs "backend failure".
  - `QueueBackend::enqueue(&self, dispatch: SubjectDispatch, run_at: Option<String>, expire_after_secs: Option<u64>) -> Result<EnqueueOutcome, QueueCallError>`
- Produces (`src/plugin.rs`): `fn call_error_response(id: Option<Value>, error: QueueCallError, method: &str) -> RpcResponse`. `InvalidParams` becomes `-32602`; `Backend` becomes an internal error.
- `QueueCapabilities` is still advertised with `generation_fenced_leases_v1: false` until Task 11.

- [ ] **Step 1: Bump the dependencies (this breaks the build)**

Replace the whole of `Cargo.toml` with:

```toml
[package]
name = "animus-queue-default"
version = "0.4.0"
edition = "2021"
description = "Reference queue plugin for Animus: file-backed dispatch queue with generation-fenced (ticketed) leases"
license = "MIT OR Apache-2.0"
repository = "https://github.com/launchapp-dev/animus-queue-default"

[[bin]]
name = "animus-queue-default"
path = "src/main.rs"

[lib]
name = "animus_queue_default"
path = "src/lib.rs"

[dependencies]
animus-plugin-protocol = { git = "https://github.com/launchapp-dev/animus-protocol", tag = "v0.7.0-rc.43" }
animus-queue-protocol = { git = "https://github.com/launchapp-dev/animus-protocol", tag = "v0.7.0-rc.43" }
animus-subject-protocol = { git = "https://github.com/launchapp-dev/animus-protocol", tag = "v0.7.0-rc.43" }

serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "io-util", "io-std", "sync", "fs"] }
tracing = "0.1"
chrono = { version = "0.4", features = ["serde"] }
anyhow = "1"
thiserror = "1"
fs2 = "0.4"
uuid = { version = "1", features = ["v4"] }
async-trait = "0.1"

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo build`

Expected: compile errors: missing fields `plugin_kinds` and `supports_mcp` in `PluginManifest`, missing `plugin_kinds` in `PluginInfo`, missing `generation_fenced_leases_v1` in `QueueCapabilities`, and `Option<String>` found where `String` was expected for `subject_key()`.

- [ ] **Step 3: Implement**

Apply to `src/dispatch_queue_state.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/dispatch_queue_state.rs b/src/dispatch_queue_state.rs
index 482a571..b126900 100644
--- a/src/dispatch_queue_state.rs
+++ b/src/dispatch_queue_state.rs
@@ -120,7 +120,7 @@ impl DispatchQueueEntry {
     ) -> Self {
         Self {
             entry_id: uuid::Uuid::new_v4().to_string(),
-            subject_id: Some(dispatch.subject_key()),
+            subject_id: dispatch.subject_key(),
             task_id: dispatch.task_id().unwrap_or_default().to_string(),
             dispatch: Some(dispatch),
             status: DispatchQueueEntryStatus::Pending,
@@ -182,8 +182,8 @@ impl DispatchQueueEntry {
         {
             return subject_id;
         }
-        if let Some(dispatch) = &self.dispatch {
-            return dispatch.subject_id();
+        if let Some(subject_id) = self.dispatch.as_ref().and_then(SubjectDispatch::subject_id) {
+            return subject_id;
         }
         self.task_id.as_str()
     }
```

Apply to `src/plugin.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/plugin.rs b/src/plugin.rs
index f5e2032..1200e0a 100644
--- a/src/plugin.rs
+++ b/src/plugin.rs
@@ -26,7 +26,9 @@ use serde_json::{json, Value};
 use tokio::io::{AsyncReadExt, AsyncWriteExt};
 use tokio::sync::{Mutex, RwLock};
 
-use crate::queue_service::{QueueBackend, QueueLeaseError, QueueReleasePendingError};
+use crate::queue_service::{
+    QueueBackend, QueueCallError, QueueLeaseError, QueueReleasePendingError,
+};
 
 const PLUGIN_NAME: &str = "animus-queue-default";
 const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");
@@ -143,6 +145,8 @@ fn print_manifest() {
         capabilities: queue_methods().into_iter().map(|m| m.to_string()).collect(),
         env_required: Vec::new(),
         notification_buffer_size: None,
+        plugin_kinds: Vec::new(),
+        supports_mcp: None,
     };
     let mut stdout = io::stdout().lock();
     let _ = writeln!(
@@ -272,6 +276,7 @@ async fn handle_initialize(
         // Hosts clamp `queue/lease.max` to this value; advertising `u32::MAX`
         // is the "effectively unlimited" sentinel for the reference plugin.
         max_lease_batch: u32::MAX,
+        generation_fenced_leases_v1: false,
     };
     let extra = serde_json::to_value(capabilities).unwrap_or(Value::Null);
     let mut kind_capabilities = std::collections::HashMap::new();
@@ -289,6 +294,7 @@ async fn handle_initialize(
             name: PLUGIN_NAME.to_string(),
             version: PLUGIN_VERSION.to_string(),
             plugin_kind: PLUGIN_KIND_QUEUE.to_string(),
+            plugin_kinds: Vec::new(),
             description: Some(PLUGIN_DESCRIPTION.to_string()),
         },
         capabilities: PluginCapabilities {
@@ -370,7 +376,7 @@ async fn handle_enqueue(
                 warning: outcome.warning,
             },
         ),
-        Err(error) => internal_error_response(id, format!("queue/enqueue failed: {error:#}")),
+        Err(error) => call_error_response(id, error, "queue/enqueue"),
     }
 }
 
@@ -732,6 +738,15 @@ fn not_pending_or_internal(id: Option<Value>, error: &anyhow::Error, method: &st
     internal_error_response(id, format!("{method} failed: {error:#}"))
 }
 
+fn call_error_response(id: Option<Value>, error: QueueCallError, method: &str) -> RpcResponse {
+    match error {
+        QueueCallError::InvalidParams(message) => RpcResponse::err(id, invalid_params(message)),
+        QueueCallError::Backend(error) => {
+            internal_error_response(id, format!("{method} failed: {error:#}"))
+        }
+    }
+}
+
 fn invalid_params(message: impl Into<String>) -> RpcError {
     RpcError {
         code: plugin_error_codes::INVALID_PARAMS,
```

Apply to `src/queue_service.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/queue_service.rs b/src/queue_service.rs
index 3806a1f..375bb05 100644
--- a/src/queue_service.rs
+++ b/src/queue_service.rs
@@ -69,7 +69,16 @@ impl QueueBackend {
         dispatch: SubjectDispatch,
         run_at: Option<String>,
         expire_after_secs: Option<u64>,
-    ) -> Result<EnqueueOutcome> {
+    ) -> std::result::Result<EnqueueOutcome, QueueCallError> {
+        // A subjectless entry would make the whole file unreadable for queue
+        // v0.3.3 (its dispatch type requires a subject), which breaks the
+        // documented rollback path. Reject it before touching state.
+        let Some(subject_id) = dispatch.subject_key() else {
+            return Err(QueueCallError::InvalidParams(
+                "queue subject identity is missing".to_string(),
+            ));
+        };
+
         let _lock = acquire_queue_lock(&self.project_root)?;
         let mut state = load_queue_state(&self.project_root)?.unwrap_or_default();
 
@@ -77,8 +86,6 @@ impl QueueBackend {
         // duplicate counts reflect the live queue.
         sweep_expired_entries(&mut state, Utc::now());
 
-        let subject_id = dispatch.subject_key();
-
         // Count live (non-Unknown) entries already targeting this subject —
         // used for the advisory warning. Enqueue is NOT idempotent in either
         // direction: immediate and deferred enqueues both always create a new
@@ -281,7 +288,7 @@ impl QueueBackend {
                 let key_owned = entry
                     .dispatch
                     .as_ref()
-                    .map(|d| d.subject_key())
+                    .and_then(SubjectDispatch::subject_key)
                     .unwrap_or_else(|| entry.subject_id_ref().to_string());
                 if set.contains(&key_owned) {
                     continue;
@@ -648,6 +655,17 @@ enum MutationError {
     NotPending,
 }
 
+/// Errors from calls that validate caller input before touching state.
+#[derive(Debug, thiserror::Error)]
+pub enum QueueCallError {
+    /// Bad caller input. Surfaced as JSON-RPC `-32602` invalid params.
+    #[error("{0}")]
+    InvalidParams(String),
+    /// Wrapped backend error (I/O, lock acquisition, persistence).
+    #[error(transparent)]
+    Backend(#[from] anyhow::Error),
+}
+
 /// Typed errors specific to `queue/lease`.
 #[derive(Debug, thiserror::Error)]
 pub enum QueueLeaseError {
@@ -836,6 +854,24 @@ mod tests {
         assert_eq!(listed.entries[0].subject_id, "TASK-1");
     }
 
+    #[test]
+    fn enqueue_rejects_subjectless_dispatch() {
+        let temp = tempfile::tempdir().expect("tempdir");
+        let backend = QueueBackend::new(temp.path().to_path_buf());
+        let dispatch = SubjectDispatch::subjectless("standard", "manual-queue-enqueue", Utc::now());
+
+        let error = backend
+            .enqueue(dispatch, None, None)
+            .expect_err("subjectless enqueue must be rejected");
+
+        assert!(matches!(error, QueueCallError::InvalidParams(_)));
+        assert_eq!(error.to_string(), "queue subject identity is missing");
+        assert!(
+            !temp.path().join(".animus").exists(),
+            "a rejected enqueue must not create queue files"
+        );
+    }
+
     #[test]
     fn hold_release_and_reorder_use_entry_ids() {
         let temp = tempfile::tempdir().expect("tempdir");
@@ -1021,7 +1057,10 @@ mod tests {
             .expect("enqueue second");
 
         assert!(first.enqueued);
-        assert!(second.enqueued, "immediate duplicate is now enqueued, not deduped");
+        assert!(
+            second.enqueued,
+            "immediate duplicate is now enqueued, not deduped"
+        );
         assert_ne!(first.entry_id, second.entry_id);
         assert!(second.warning.is_some(), "collision surfaces a warning");
 
@@ -1050,9 +1089,16 @@ mod tests {
             .enqueue(task_dispatch("LATER", "standard"), Some(later), None)
             .expect("later");
         backend
-            .enqueue(task_dispatch("SOONER", "standard"), Some(sooner.clone()), None)
+            .enqueue(
+                task_dispatch("SOONER", "standard"),
+                Some(sooner.clone()),
+                None,
+            )
             .expect("sooner");
 
-        assert_eq!(backend.next_deadline().expect("nd").next_run_at, Some(sooner));
+        assert_eq!(
+            backend.next_deadline().expect("nd").next_run_at,
+            Some(sooner)
+        );
     }
 }
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --all-features`

Expected: all tests pass, including `queue_service::tests::enqueue_rejects_subjectless_dispatch`.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "build: move to animus-protocol v0.7.0-rc.43; reject subjectless enqueue"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 2: Task IDs and the request hash

Two pure helper modules ported from `animus-postgres` v0.2.9:

- **`identity`:** `canonicalSubjectId` (`queue.ts:166`) gives the `<kind>:<id>` form that ticketed entries use. `subjectKey` gives the old-style key that v0.3.3 already writes (spec §6.5).
- **`request_hash`:** `requestHash` over canonical JSON (`queue.ts:150-162`). The idempotency check uses it. Unlike v0.2.9 it leaves out `dispatch.requested_at` (difference 2).

**Files:**

- Modify: `Cargo.toml`
- Create: `src/identity.rs`
- Modify: `src/lib.rs`
- Create: `src/request_hash.rs`
- Modify: `Cargo.lock` (cargo updates it; commit the result)

**Interfaces:**

- Produces (`src/identity.rs`):
  - `pub const MISSING_SUBJECT_IDENTITY: &str`
  - `pub fn legacy_subject_key(kind: &str, id: &str) -> String`
  - `pub fn canonical_subject_id(kind: &str, id: &str) -> Result<String, String>`
  - `pub fn dispatch_canonical_id(dispatch: &SubjectDispatch) -> Result<String, String>`
  - `pub fn dispatch_legacy_key(dispatch: &SubjectDispatch) -> Option<String>`
  - `pub fn is_task_kind(kind: &str) -> bool`
  - `pub fn dispatch_task_id(dispatch: &SubjectDispatch) -> Option<&str>`
- Produces (`src/request_hash.rs`):
  - `pub fn canonical_json(value: &Value) -> String`
  - `pub fn enqueue_request_hash(dispatch: &SubjectDispatch, repository: Option<&RepositoryReservation>, run_at: Option<&str>, expire_after_secs: Option<u64>) -> String`, which returns lowercase sha256 hex.

- [ ] **Step 1: Write the failing tests**

This task's tests are unit tests inside the source files in Step 3 (the `#[cfg(test)] mod tests` blocks). Add those blocks first, with the functions they call declared but unimplemented (`todo!()`), so they compile and fail.

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --lib -- identity request_hash`

Expected: the new tests fail (they panic at `todo!()`).

- [ ] **Step 3: Implement**

Apply to `Cargo.toml` (`git apply` accepts this patch):

```diff
diff --git a/Cargo.toml b/Cargo.toml
index d1ce11f..b6fca46 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -18,6 +18,7 @@ path = "src/lib.rs"
 animus-plugin-protocol = { git = "https://github.com/launchapp-dev/animus-protocol", tag = "v0.7.0-rc.43" }
 animus-queue-protocol = { git = "https://github.com/launchapp-dev/animus-protocol", tag = "v0.7.0-rc.43" }
 animus-subject-protocol = { git = "https://github.com/launchapp-dev/animus-protocol", tag = "v0.7.0-rc.43" }
+animus-execution-protocol = { git = "https://github.com/launchapp-dev/animus-protocol", tag = "v0.7.0-rc.43" }
 
 serde = { version = "1", features = ["derive"] }
 serde_json = "1"
@@ -29,6 +30,7 @@ thiserror = "1"
 fs2 = "0.4"
 uuid = { version = "1", features = ["v4"] }
 async-trait = "0.1"
+sha2 = "0.10"
 
 [dev-dependencies]
 tempfile = "3"
```

Create `src/identity.rs`:

```rust
//! Subject identity rules, ported from animus-postgres v0.2.9 (`src/queue.ts`).
//!
//! Two id forms exist:
//!
//! - The **legacy key** (v0.2.9 `subjectKey`): the bare id for built-in kinds,
//!   `<kind>::<id>` for everything else. Old-style entries store it in
//!   `subject_id`, and old-style `exclude_subjects` lists use it.
//! - The **canonical id** (v0.2.9 `canonicalSubjectId`): `<kind>:<id>`, with
//!   `animus.task` shortened to `task` and `animus.requirement` to
//!   `requirement`. Ticketed entries store it in `subject_id`, and it is the
//!   `qualified_id` inside every execution fence.

use animus_subject_protocol::SubjectDispatch;

/// Error text for a subject that cannot be identified (v0.2.9 wording).
pub const MISSING_SUBJECT_IDENTITY: &str = "queue subject identity is missing";

/// Kinds whose legacy key is the bare id. Matched exactly, as v0.2.9 does.
const BARE_ID_KINDS: [&str; 5] = [
    "animus.task",
    "animus.requirement",
    "custom",
    "task",
    "requirement",
];

/// Legacy key: the bare id for built-in kinds, `<kind>::<id>` otherwise.
pub fn legacy_subject_key(kind: &str, id: &str) -> String {
    if BARE_ID_KINDS.contains(&kind) {
        id.to_string()
    } else {
        format!("{kind}::{id}")
    }
}

/// Canonical `<kind>:<id>` identity.
///
/// Both parts are trimmed, and an id that already starts with `<kind>:` (in
/// any ASCII case) is not prefixed again, so `task:task:X` never appears.
/// An empty kind or id is an error. So is an id that is only the prefix
/// (`task:`), because the fence protocol requires a non-empty native id.
pub fn canonical_subject_id(kind: &str, id: &str) -> Result<String, String> {
    let kind = match kind {
        "animus.task" => "task",
        "animus.requirement" => "requirement",
        other => other,
    }
    .trim();
    let id = id.trim();
    if kind.is_empty() || id.is_empty() {
        return Err(MISSING_SUBJECT_IDENTITY.to_string());
    }
    let prefix = format!("{kind}:");
    let native = match id.get(..prefix.len()) {
        Some(head) if head.eq_ignore_ascii_case(&prefix) => &id[prefix.len()..],
        _ => id,
    };
    if native.is_empty() {
        return Err(MISSING_SUBJECT_IDENTITY.to_string());
    }
    Ok(format!("{kind}:{native}"))
}

/// Canonical id of a dispatch's subject. Errors for a subjectless dispatch.
pub fn dispatch_canonical_id(dispatch: &SubjectDispatch) -> Result<String, String> {
    let subject = dispatch
        .subject()
        .ok_or_else(|| MISSING_SUBJECT_IDENTITY.to_string())?;
    canonical_subject_id(subject.kind(), subject.id())
}

/// Legacy key of a dispatch's subject. `None` for a subjectless dispatch.
pub fn dispatch_legacy_key(dispatch: &SubjectDispatch) -> Option<String> {
    dispatch
        .subject()
        .map(|subject| legacy_subject_key(subject.kind(), subject.id()))
}

/// `true` for the task kinds v0.2.9 reports a `task_id` for.
pub fn is_task_kind(kind: &str) -> bool {
    kind.eq_ignore_ascii_case("animus.task") || kind.eq_ignore_ascii_case("task")
}

/// The raw subject id when the dispatch's subject is a task.
pub fn dispatch_task_id(dispatch: &SubjectDispatch) -> Option<&str> {
    dispatch
        .subject()
        .filter(|subject| is_task_kind(subject.kind()))
        .map(|subject| subject.id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_subject_protocol::SubjectRef;
    use chrono::Utc;

    #[test]
    fn canonical_ids_match_v029_cases() {
        let cases = [
            ("animus.task", "TASK-1", "task:TASK-1"),
            ("animus.task", "task:TASK-1216", "task:TASK-1216"),
            ("animus.task", "TASK:X", "task:X"),
            (
                "animus.requirement",
                "requirement:REQUIREMENT-075",
                "requirement:REQUIREMENT-075",
            ),
            ("task", "TASK-LEGACY", "task:TASK-LEGACY"),
            (
                "trigger_event",
                "github-delivery-1177",
                "trigger_event:github-delivery-1177",
            ),
            (" task ", " TASK-2 ", "task:TASK-2"),
        ];
        for (kind, id, expected) in cases {
            assert_eq!(
                canonical_subject_id(kind, id).unwrap(),
                expected,
                "{kind}/{id}"
            );
        }
    }

    #[test]
    fn canonical_id_rejects_missing_parts() {
        for (kind, id) in [("", "X"), ("task", "   "), ("   ", "X"), ("task", "task:")] {
            assert_eq!(
                canonical_subject_id(kind, id).unwrap_err(),
                MISSING_SUBJECT_IDENTITY,
                "{kind:?}/{id:?}"
            );
        }
    }

    #[test]
    fn legacy_keys_match_v029_cases() {
        assert_eq!(legacy_subject_key("animus.task", "TASK-1"), "TASK-1");
        assert_eq!(legacy_subject_key("task", "TASK-1"), "TASK-1");
        assert_eq!(legacy_subject_key("requirement", "REQ-1"), "REQ-1");
        assert_eq!(legacy_subject_key("custom", "nightly"), "nightly");
        assert_eq!(
            legacy_subject_key("pack.review", "REV-7"),
            "pack.review::REV-7"
        );
        // v0.2.9 matches kinds exactly, so a differently-cased kind is qualified.
        assert_eq!(legacy_subject_key("Animus.Task", "X"), "Animus.Task::X");
    }

    #[test]
    fn dispatch_helpers_read_both_subject_wire_shapes() {
        let legacy: SubjectDispatch = serde_json::from_value(serde_json::json!({
            "subject": { "Task": { "id": "TASK-9" } },
            "workflow_ref": "standard",
            "trigger_source": "test",
            "requested_at": "2026-09-28T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(dispatch_canonical_id(&legacy).unwrap(), "task:TASK-9");
        assert_eq!(dispatch_legacy_key(&legacy).as_deref(), Some("TASK-9"));
        assert_eq!(dispatch_task_id(&legacy), Some("TASK-9"));

        let generic: SubjectDispatch = serde_json::from_value(serde_json::json!({
            "subject": { "kind": "task", "id": "TASK-10" },
            "workflow_ref": "standard",
            "trigger_source": "test",
            "requested_at": "2026-09-28T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(dispatch_canonical_id(&generic).unwrap(), "task:TASK-10");
        assert_eq!(dispatch_legacy_key(&generic).as_deref(), Some("TASK-10"));
        assert_eq!(dispatch_task_id(&generic), Some("TASK-10"));

        let review = SubjectDispatch::for_subject_with_metadata(
            SubjectRef::new("pack.review", "REV-7"),
            "review",
            "test",
            Utc::now(),
        );
        assert_eq!(dispatch_canonical_id(&review).unwrap(), "pack.review:REV-7");
        assert_eq!(
            dispatch_legacy_key(&review).as_deref(),
            Some("pack.review::REV-7")
        );
        assert_eq!(dispatch_task_id(&review), None);
    }

    #[test]
    fn subjectless_dispatch_has_no_identity() {
        let dispatch = SubjectDispatch::subjectless("standard", "test", Utc::now());
        assert_eq!(
            dispatch_canonical_id(&dispatch).unwrap_err(),
            MISSING_SUBJECT_IDENTITY
        );
        assert_eq!(dispatch_legacy_key(&dispatch), None);
        assert_eq!(dispatch_task_id(&dispatch), None);
    }
}
```

Apply to `src/lib.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/lib.rs b/src/lib.rs
index 1977357..4cadfd5 100644
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -20,8 +20,10 @@
 
 pub mod dispatch_queue_state;
 pub mod dispatch_queue_store;
+pub mod identity;
 pub mod plugin;
 pub mod queue_service;
+pub mod request_hash;
 
 pub use dispatch_queue_state::{
     DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
```

Create `src/request_hash.rs`:

```rust
//! Content hash that a `queue/v2/enqueue` idempotency key is bound to.

use animus_execution_protocol::RepositoryReservation;
use animus_subject_protocol::SubjectDispatch;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Serialize `value` with object keys sorted at every level, like v0.2.9's
/// `canonicalJson`. Independent of serde_json's map ordering features.
pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Array(items) => {
            let body: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", body.join(","))
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let body: Vec<String> = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        Value::String(key.clone()),
                        canonical_json(&map[key])
                    )
                })
                .collect();
            format!("{{{}}}", body.join(","))
        }
        scalar => scalar.to_string(),
    }
}

/// sha256 (lowercase hex) of the enqueue content an idempotency key is bound
/// to.
///
/// `dispatch.requested_at` is left out (difference 2 from animus-postgres
/// v0.2.9): the CLI stamps it with the current time on every attempt, so
/// including it would turn identical retries into conflicts.
pub fn enqueue_request_hash(
    dispatch: &SubjectDispatch,
    repository: Option<&RepositoryReservation>,
    run_at: Option<&str>,
    expire_after_secs: Option<u64>,
) -> String {
    let mut dispatch = serde_json::to_value(dispatch).expect("SubjectDispatch serializes to JSON");
    if let Value::Object(map) = &mut dispatch {
        map.remove("requested_at");
    }
    let content = json!({
        "dispatch": dispatch,
        "repository": repository,
        "run_at": run_at,
        "expire_after_secs": expire_after_secs,
    });
    format!("{:x}", Sha256::digest(canonical_json(&content).as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_subject_protocol::SubjectRef;
    use chrono::{TimeZone, Utc};

    fn dispatch(workflow_ref: &str, second: u32) -> SubjectDispatch {
        SubjectDispatch::for_subject_with_metadata(
            SubjectRef::task("TASK-1"),
            workflow_ref,
            "manual-queue-enqueue",
            Utc.with_ymd_and_hms(2026, 9, 28, 12, 0, second).unwrap(),
        )
    }

    fn reservation(head: &str) -> RepositoryReservation {
        RepositoryReservation {
            repository: "https://github.com/launchapp-dev/animus-cli.git".to_string(),
            base_ref: "refs/heads/main".to_string(),
            head_ref: format!("refs/heads/{head}"),
        }
    }

    #[test]
    fn canonical_json_sorts_keys_at_every_level() {
        let value = json!({"b": 1, "a": {"d": [{"z": 1, "y": "x"}], "c": null}});
        assert_eq!(
            canonical_json(&value),
            r#"{"a":{"c":null,"d":[{"y":"x","z":1}]},"b":1}"#
        );
    }

    #[test]
    fn hash_ignores_requested_at() {
        assert_eq!(
            enqueue_request_hash(&dispatch("coding", 0), None, None, None),
            enqueue_request_hash(&dispatch("coding", 59), None, None, None)
        );
    }

    #[test]
    fn hash_covers_everything_else() {
        let base = enqueue_request_hash(&dispatch("coding", 0), None, None, None);
        assert_eq!(base.len(), 64);
        assert!(base
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        let variants = [
            enqueue_request_hash(&dispatch("review", 0), None, None, None),
            enqueue_request_hash(&dispatch("coding", 0), Some(&reservation("a")), None, None),
            enqueue_request_hash(
                &dispatch("coding", 0),
                None,
                Some("2030-01-01T00:00:00Z"),
                None,
            ),
            enqueue_request_hash(&dispatch("coding", 0), None, None, Some(60)),
        ];
        for variant in variants {
            assert_ne!(variant, base);
        }
        assert_ne!(
            enqueue_request_hash(&dispatch("coding", 0), Some(&reservation("a")), None, None),
            enqueue_request_hash(&dispatch("coding", 0), Some(&reservation("b")), None, None)
        );
    }
}
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --lib -- identity request_hash`

Expected: all `identity::tests::*` and `request_hash::tests::*` pass.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: canonical subject ids and idempotency request hash (v0.2.9 port)"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 3: Refuse Animus 0.6.x hosts

The guard from spec §3.2. `initialize` reads `protocol_version` before anything else. If it is missing, unparseable, or below `1.1.0` (compared as semantic versions, so `1.1.0-rc.1` counts as below), the call fails with invalid params and the recovery message. No file is opened. Every 0.6.x CLI sends `1.0.0`; 0.7 sends `1.1.0` or `1.2.0`.

This task also adds `tests/common/mod.rs`, which later tasks extend: starting with a stdio `PluginProcess` driver.

**Files:**

- Modify: `Cargo.toml`
- Create: `src/host_guard.rs`
- Modify: `src/lib.rs`
- Modify: `src/plugin.rs`
- Create: `tests/common/mod.rs`
- Create: `tests/host_guard.rs`
- Modify: `Cargo.lock` (cargo updates it; commit the result)

**Interfaces:**

- Produces (`src/host_guard.rs`):
  - `pub const MIN_HOST_PROTOCOL_VERSION: Version` (1.1.0)
  - `pub const LEGACY_QUEUE_VERSION: &str` ("v0.3.3")
  - `pub fn check_host_protocol(raw: Option<&str>) -> Result<(), RpcError>`
- Produces (`tests/common/mod.rs`):
  - `PluginProcess::spawn(envs: &[(&str, &str)]) -> PluginProcess`
  - `PluginProcess::request(&mut self, method: &str, params: Value) -> Value` (the full response frame)
  - `PluginProcess::initialize(&mut self, project_root: &Path, protocol_version: &str) -> Value`

- [ ] **Step 1: Write the failing tests**

Create `tests/common/mod.rs`:

```rust
//! Helpers shared by the integration tests. Each test file uses a subset.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{json, Value};

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
```

Create `tests/host_guard.rs`:

```rust
//! The old-CLI guard, exercised through the real binary.

mod common;

use common::PluginProcess;
use serde_json::json;

const INVALID_PARAMS: i64 = -32602;
const PLUGIN_NOT_INITIALIZED: i64 = -32000;

#[test]
fn protocol_1_0_host_is_refused_before_any_file_access() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = PluginProcess::spawn(&[]);

    let refused = plugin.initialize(temp.path(), "1.0.0");
    assert_eq!(refused["error"]["code"], INVALID_PARAMS);
    let message = refused["error"]["message"].as_str().expect("message");
    assert!(
        message.contains("requires Animus 0.7 or newer"),
        "{message}"
    );
    assert!(
        message.contains("animus plugin install launchapp-dev/animus-queue-default@v0.3.3"),
        "{message}"
    );

    // The refused host gets no backend, so queue calls cannot touch files.
    let listed = plugin.request("queue/list", json!({}));
    assert_eq!(listed["error"]["code"], PLUGIN_NOT_INITIALIZED);
    assert!(
        !temp.path().join(".animus").exists(),
        "a refused host must not create or read queue files"
    );
}

#[test]
fn missing_protocol_version_is_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = PluginProcess::spawn(&[]);

    let refused = plugin.request(
        "initialize",
        json!({
            "host_info": { "name": "animus", "version": "0.1.0" },
            "capabilities": {},
            "init_extensions": {
                "project_binding": { "project_root": temp.path().to_string_lossy() }
            }
        }),
    );

    assert_eq!(refused["error"]["code"], INVALID_PARAMS);
    assert!(refused["error"]["message"]
        .as_str()
        .expect("message")
        .contains("(plugin protocol not sent)"));
}

#[test]
fn protocol_1_1_and_1_2_hosts_are_accepted() {
    for version in ["1.1.0", "1.2.0"] {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut plugin = PluginProcess::spawn(&[]);
        let accepted = plugin.initialize(temp.path(), version);
        assert!(accepted.get("error").is_none(), "{version}: {accepted}");
        assert_eq!(accepted["result"]["protocol_version"], "1.2.0");
    }
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test host_guard`

Expected: `protocol_1_0_host_is_refused_before_any_file_access` and `missing_protocol_version_is_refused` fail (initialize succeeds); the unit tests in `host_guard` fail at `todo!()`.

- [ ] **Step 3: Implement**

Apply to `Cargo.toml` (`git apply` accepts this patch):

```diff
diff --git a/Cargo.toml b/Cargo.toml
index b6fca46..a908f05 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -31,6 +31,7 @@ fs2 = "0.4"
 uuid = { version = "1", features = ["v4"] }
 async-trait = "0.1"
 sha2 = "0.10"
+semver = "1"
 
 [dev-dependencies]
 tempfile = "3"
```

Create `src/host_guard.rs`:

```rust
//! Refuse hosts that predate generation-fenced queue leases.
//!
//! Animus CLIs v0.4.0 – v0.6.33 announce plugin protocol `1.0.0` and never
//! use tickets, so they must stay on animus-queue-default v0.3.3. The first
//! 0.7 pre-releases announce `1.1.0` and later ones `1.2.0`. The host's
//! `host_info.version` can't tell them apart: it is the plugin-host crate's
//! own version, `0.1.0`, in both lines. The protocol version can.

use animus_plugin_protocol::{error_codes, RpcError};
use semver::Version;
use serde_json::json;

/// Oldest host plugin protocol this queue accepts.
pub const MIN_HOST_PROTOCOL_VERSION: Version = Version::new(1, 1, 0);

/// Queue release that 0.6.x and older hosts should install instead.
pub const LEGACY_QUEUE_VERSION: &str = "v0.3.3";

/// Accept `raw` only when it is a semantic version of at least 1.1.0.
/// A missing or unparseable version is refused.
pub fn check_host_protocol(raw: Option<&str>) -> Result<(), RpcError> {
    let accepted = raw
        .and_then(|value| Version::parse(value.trim()).ok())
        .is_some_and(|version| version >= MIN_HOST_PROTOCOL_VERSION);
    if accepted {
        return Ok(());
    }
    let install_command =
        format!("animus plugin install launchapp-dev/animus-queue-default@{LEGACY_QUEUE_VERSION}");
    Err(RpcError {
        code: error_codes::INVALID_PARAMS,
        message: format!(
            "animus-queue-default v{} requires Animus 0.7 or newer. This Animus is 0.6 or older \
             (plugin protocol {}). Install the queue version made for it: `{install_command}`",
            env!("CARGO_PKG_VERSION"),
            raw.unwrap_or("not sent"),
        ),
        data: Some(json!({
            "host_protocol_version": raw,
            "minimum_host_protocol_version": MIN_HOST_PROTOCOL_VERSION.to_string(),
            "compatible_queue_version": LEGACY_QUEUE_VERSION,
            "install_command": install_command,
        })),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_protocol_1_0_hosts_with_the_documented_message() {
        let error = check_host_protocol(Some("1.0.0")).unwrap_err();
        assert_eq!(error.code, error_codes::INVALID_PARAMS);
        assert_eq!(
            error.message,
            format!(
                "animus-queue-default v{} requires Animus 0.7 or newer. This Animus is 0.6 or \
                 older (plugin protocol 1.0.0). Install the queue version made for it: \
                 `animus plugin install launchapp-dev/animus-queue-default@v0.3.3`",
                env!("CARGO_PKG_VERSION")
            )
        );
        let data = error.data.expect("error data");
        assert_eq!(data["host_protocol_version"], "1.0.0");
        assert_eq!(data["minimum_host_protocol_version"], "1.1.0");
        assert_eq!(data["compatible_queue_version"], "v0.3.3");
    }

    #[test]
    fn refuses_missing_unparseable_and_older_versions() {
        for raw in [
            None,
            Some(""),
            Some("one.two"),
            Some("1.1"),
            Some("1.0.9"),
            Some("0.9.0"),
            Some("1.1.0-rc.1"),
        ] {
            assert!(check_host_protocol(raw).is_err(), "{raw:?} must be refused");
        }
        let missing = check_host_protocol(None).unwrap_err();
        assert!(missing.message.contains("(plugin protocol not sent)"));
    }

    #[test]
    fn accepts_protocol_1_1_and_newer() {
        for raw in ["1.1.0", "1.2.0", "1.9.0", " 1.2.0 ", "2.0.0"] {
            assert!(
                check_host_protocol(Some(raw)).is_ok(),
                "{raw} must be accepted"
            );
        }
    }
}
```

Apply to `src/lib.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/lib.rs b/src/lib.rs
index 4cadfd5..1f2d637 100644
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -20,6 +20,7 @@
 
 pub mod dispatch_queue_state;
 pub mod dispatch_queue_store;
+pub mod host_guard;
 pub mod identity;
 pub mod plugin;
 pub mod queue_service;
```

Apply to `src/plugin.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/plugin.rs b/src/plugin.rs
index 1200e0a..8c785b2 100644
--- a/src/plugin.rs
+++ b/src/plugin.rs
@@ -26,6 +26,7 @@ use serde_json::{json, Value};
 use tokio::io::{AsyncReadExt, AsyncWriteExt};
 use tokio::sync::{Mutex, RwLock};
 
+use crate::host_guard::check_host_protocol;
 use crate::queue_service::{
     QueueBackend, QueueCallError, QueueLeaseError, QueueReleasePendingError,
 };
@@ -252,14 +253,23 @@ async fn handle_initialize(
     params: Option<Value>,
     backend: &Arc<RwLock<Option<QueueBackend>>>,
 ) -> RpcResponse {
-    let init: InitializeParams = match params
-        .ok_or_else(|| invalid_params("missing params for initialize"))
-        .and_then(|value| {
-            serde_json::from_value(value)
-                .map_err(|error| invalid_params(format!("invalid initialize params: {error}")))
-        }) {
+    let Some(params) = params else {
+        return RpcResponse::err(id, invalid_params("missing params for initialize"));
+    };
+    // Refuse 0.6.x and older hosts first, before the project binding is even
+    // read, so a refused host never reaches the queue files.
+    let host_protocol = params.get("protocol_version").and_then(Value::as_str);
+    if let Err(error) = check_host_protocol(host_protocol) {
+        return RpcResponse::err(id, error);
+    }
+    let init: InitializeParams = match serde_json::from_value(params) {
         Ok(value) => value,
-        Err(error) => return RpcResponse::err(id, error),
+        Err(error) => {
+            return RpcResponse::err(
+                id,
+                invalid_params(format!("invalid initialize params: {error}")),
+            );
+        }
     };
 
     let project_root = match extract_project_root(&init) {
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --test host_guard && cargo test --lib host_guard`

Expected: all pass. The refusal message is exactly: animus-queue-default v0.4.0 requires Animus 0.7 or newer. This Animus is 0.6 or older (plugin protocol 1.0.0). Install the queue version made for it: `animus plugin install launchapp-dev/animus-queue-default@v0.3.3`

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: refuse hosts older than plugin protocol 1.1.0 (Animus 0.6.x)"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 4: Ticket length setting

Difference 6. `ANIMUS_QUEUE_LEASE_TTL_SECS` sets the ticket length: a whole number from 1 to 604800 (whitespace trimmed); anything else falls back to 1800. The manifest declares the variable (`required: false`, `sensitive: false`) because the host forwards only declared variables. `QueueBackend` carries the length so tests can set it directly.

**Files:**

- Create: `src/lease_ttl.rs`
- Modify: `src/lib.rs`
- Modify: `src/plugin.rs`
- Modify: `src/queue_service.rs`
- Modify: `tests/stdio_smoke.rs`

**Interfaces:**

- Produces (`src/lease_ttl.rs`):
  - `pub const LEASE_TTL_ENV: &str = "ANIMUS_QUEUE_LEASE_TTL_SECS"`
  - `pub const DEFAULT_LEASE_TTL_SECS: i64 = 1800`
  - `pub const MAX_LEASE_TTL_SECS: i64 = 604800`
  - `pub fn parse_lease_ttl(raw: Option<&str>) -> i64`
  - `pub fn lease_ttl_from_env() -> i64`
- Produces (`src/queue_service.rs`):
  - `QueueBackend::with_lease_ttl(self, secs: i64) -> Self`
  - `QueueBackend::lease_ttl_secs(&self) -> i64`
  - `QueueBackend::project_root(&self) -> &Path`

- [ ] **Step 1: Write the failing tests**

Apply to `tests/stdio_smoke.rs` (`git apply` accepts this patch):

```diff
diff --git a/tests/stdio_smoke.rs b/tests/stdio_smoke.rs
index 69ae72e..925a8ee 100644
--- a/tests/stdio_smoke.rs
+++ b/tests/stdio_smoke.rs
@@ -33,6 +33,14 @@ fn manifest_prints_valid_json() {
         .expect("capabilities array");
     assert!(methods.iter().any(|v| v == "queue/lease"));
     assert!(methods.iter().any(|v| v == "queue/enqueue"));
+
+    // The host forwards only declared variables, so the ticket-length
+    // setting must be declared to reach the plugin.
+    let env = manifest["env_required"].as_array().expect("env_required");
+    assert_eq!(env.len(), 1);
+    assert_eq!(env[0]["name"], "ANIMUS_QUEUE_LEASE_TTL_SECS");
+    assert_eq!(env[0]["required"], false);
+    assert_eq!(env[0]["sensitive"], false);
 }
 
 #[test]
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test stdio_smoke && cargo test --lib lease_ttl`

Expected: `manifest_prints_valid_json` fails (no `env_required` entry for `ANIMUS_QUEUE_LEASE_TTL_SECS`); the `lease_ttl` unit tests fail at `todo!()`.

- [ ] **Step 3: Implement**

Create `src/lease_ttl.rs`:

```rust
//! Lease (ticket) length.
//!
//! animus-postgres v0.2.9 reads `ANIMUS_QUEUE_LEASE_TTL_SECS` but never
//! declares it in its manifest, so the host never forwards it and its tickets
//! always last 30 minutes. This queue declares it (difference 6), with the
//! same 1800-second default.

/// Environment variable that sets the lease length in seconds.
pub const LEASE_TTL_ENV: &str = "ANIMUS_QUEUE_LEASE_TTL_SECS";

/// Lease length when the variable is unset or invalid.
pub const DEFAULT_LEASE_TTL_SECS: i64 = 1800;

/// Longest accepted lease length (7 days). Larger values fall back to the
/// default rather than risk timestamp overflow.
pub const MAX_LEASE_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// Parse a lease length. Anything but a whole number from 1 to
/// [`MAX_LEASE_TTL_SECS`] gives [`DEFAULT_LEASE_TTL_SECS`].
pub fn parse_lease_ttl(raw: Option<&str>) -> i64 {
    raw.and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|secs| (1..=MAX_LEASE_TTL_SECS).contains(secs))
        .unwrap_or(DEFAULT_LEASE_TTL_SECS)
}

/// Lease length from [`LEASE_TTL_ENV`].
pub fn lease_ttl_from_env() -> i64 {
    parse_lease_ttl(std::env::var(LEASE_TTL_ENV).ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_values_are_used() {
        assert_eq!(parse_lease_ttl(Some("60")), 60);
        assert_eq!(parse_lease_ttl(Some(" 90 ")), 90);
        assert_eq!(parse_lease_ttl(Some("1")), 1);
        assert_eq!(parse_lease_ttl(Some("604800")), MAX_LEASE_TTL_SECS);
    }

    #[test]
    fn missing_or_invalid_values_fall_back_to_the_default() {
        for raw in [
            None,
            Some(""),
            Some("0"),
            Some("-5"),
            Some("abc"),
            Some("30s"),
            Some("604801"),
        ] {
            assert_eq!(parse_lease_ttl(raw), DEFAULT_LEASE_TTL_SECS, "{raw:?}");
        }
    }
}
```

Apply to `src/lib.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/lib.rs b/src/lib.rs
index 1f2d637..d435f7a 100644
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -22,6 +22,7 @@ pub mod dispatch_queue_state;
 pub mod dispatch_queue_store;
 pub mod host_guard;
 pub mod identity;
+pub mod lease_ttl;
 pub mod plugin;
 pub mod queue_service;
 pub mod request_hash;
```

Apply to `src/plugin.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/plugin.rs b/src/plugin.rs
index 8c785b2..c49a689 100644
--- a/src/plugin.rs
+++ b/src/plugin.rs
@@ -8,9 +8,9 @@ use std::path::PathBuf;
 use std::sync::Arc;
 
 use animus_plugin_protocol::{
-    error_codes as plugin_error_codes, HealthCheckResult, HealthStatus, InitializeParams,
-    InitializeResult, KindCapability, PluginCapabilities, PluginInfo, PluginManifest, RpcError,
-    RpcRequest, RpcResponse, PLUGIN_KIND_QUEUE, PROTOCOL_VERSION,
+    error_codes as plugin_error_codes, EnvRequirement, HealthCheckResult, HealthStatus,
+    InitializeParams, InitializeResult, KindCapability, PluginCapabilities, PluginInfo,
+    PluginManifest, RpcError, RpcRequest, RpcResponse, PLUGIN_KIND_QUEUE, PROTOCOL_VERSION,
 };
 use animus_queue_protocol::{
     error_codes as queue_error_codes, QueueCapabilities, QueueCompletionRequest, QueueDropRequest,
@@ -27,6 +27,7 @@ use tokio::io::{AsyncReadExt, AsyncWriteExt};
 use tokio::sync::{Mutex, RwLock};
 
 use crate::host_guard::check_host_protocol;
+use crate::lease_ttl::{lease_ttl_from_env, LEASE_TTL_ENV};
 use crate::queue_service::{
     QueueBackend, QueueCallError, QueueLeaseError, QueueReleasePendingError,
 };
@@ -144,7 +145,14 @@ fn print_manifest() {
         description: PLUGIN_DESCRIPTION.to_string(),
         protocol_version: PROTOCOL_VERSION.to_string(),
         capabilities: queue_methods().into_iter().map(|m| m.to_string()).collect(),
-        env_required: Vec::new(),
+        env_required: vec![EnvRequirement {
+            name: LEASE_TTL_ENV.to_string(),
+            description: Some(
+                "Queue lease (ticket) length in seconds, 1-604800. Default 1800.".to_string(),
+            ),
+            sensitive: false,
+            required: false,
+        }],
         notification_buffer_size: None,
         plugin_kinds: Vec::new(),
         supports_mcp: None,
@@ -277,7 +285,8 @@ async fn handle_initialize(
         Err(error) => return RpcResponse::err(id, error),
     };
 
-    *backend.write().await = Some(QueueBackend::new(project_root));
+    *backend.write().await =
+        Some(QueueBackend::new(project_root).with_lease_ttl(lease_ttl_from_env()));
 
     let capabilities = QueueCapabilities {
         priority_weighted: false,
```

Apply to `src/queue_service.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/queue_service.rs b/src/queue_service.rs
index 375bb05..372876a 100644
--- a/src/queue_service.rs
+++ b/src/queue_service.rs
@@ -19,11 +19,13 @@ use crate::dispatch_queue_state::{
     DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
 };
 use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state, save_queue_state};
+use crate::lease_ttl::DEFAULT_LEASE_TTL_SECS;
 
 /// File-locked backend wrapping a single project root's queue state.
 #[derive(Debug, Clone)]
 pub struct QueueBackend {
     project_root: PathBuf,
+    lease_ttl_secs: i64,
 }
 
 /// Result of a single enqueue.
@@ -44,7 +46,17 @@ impl QueueBackend {
     /// Bind the backend to a project root. State / lock files live under
     /// `<project_root>/.animus/`.
     pub fn new(project_root: PathBuf) -> Self {
-        Self { project_root }
+        Self {
+            project_root,
+            lease_ttl_secs: DEFAULT_LEASE_TTL_SECS,
+        }
+    }
+
+    /// Use `secs` as the lease (ticket) length instead of the default.
+    /// Callers pass a value from [`crate::lease_ttl::parse_lease_ttl`].
+    pub fn with_lease_ttl(mut self, secs: i64) -> Self {
+        self.lease_ttl_secs = secs;
+        self
     }
 
     /// Bound project root.
@@ -52,6 +64,11 @@ impl QueueBackend {
         &self.project_root
     }
 
+    /// Lease (ticket) length in seconds.
+    pub fn lease_ttl_secs(&self) -> i64 {
+        self.lease_ttl_secs
+    }
+
     // ============================================================
     // queue/enqueue
     // ============================================================
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --test stdio_smoke && cargo test --lib lease_ttl`

Expected: all pass.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: ANIMUS_QUEUE_LEASE_TTL_SECS, declared in the manifest"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 5: queue.json format 2 with ticket fields

The storage change from spec §6.1 and §3.3:

- **Entry fields:** each entry gains optional ticket fields (subject generation, workflow generation, lease owner/generation/expiry, repository, idempotency key, request hash). All of them are skipped when empty, so old-style entries serialise exactly as in v0.3.3.
- **Header:** the file gains `format_version` (2) and `subject_generations`, the per-task counters. Counters never decrease.
- **Load:** refuses a `format_version` above 2.
- **Save:** writes a temp file, fsyncs it, renames it, and never deletes the file.

`task_id` stays a required string and the status words don't change, so v0.3.3 can still read the file.

**Files:**

- Modify: `src/dispatch_queue_state.rs`
- Modify: `src/dispatch_queue_store.rs`

**Interfaces:**

- Produces (`src/dispatch_queue_state.rs`):
  - `pub const QUEUE_FORMAT_VERSION: u32 = 2`
  - new `DispatchQueueEntry` fields: `subject_generation: Option<u64>`, `workflow_generation: Option<u64>`, `lease_owner: Option<String>`, `lease_generation: u64`, `lease_expires_at: Option<DateTime<Utc>>`, `repository: Option<RepositoryReservation>`, `idempotency_key: Option<String>`, `request_hash: Option<String>`
  - `DispatchQueueEntry` now derives `Default` and `PartialEq`
  - `DispatchQueueState { format_version: u32, subject_generations: BTreeMap<String, u64>, entries }`
  - `DispatchQueueEntry::is_ticketed(&self) -> bool`, which is `subject_generation.is_some()`
  - `DispatchQueueEntry::lease_is_live(&self, now: DateTime<Utc>) -> bool`
  - `DispatchQueueEntry::execution_fence(&self) -> Option<ExecutionFence>` (v0.2.9 `executionFromRow`; `None` unless every part is present)

- [ ] **Step 1: Write the failing tests**

This task's tests are unit tests inside the source files in Step 3 (the `#[cfg(test)] mod tests` blocks). Add those blocks first, with the functions they call declared but unimplemented (`todo!()`), so they compile and fail.

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --lib dispatch_queue`

Expected: the new unit tests fail to compile until the fields exist, then fail at `todo!()`.

- [ ] **Step 3: Implement**

Replace the whole of `src/dispatch_queue_state.rs` with:

```rust
//! In-memory queue state shapes (also the on-disk persistence format).
//!
//! Compatibility rule: queue v0.3.3 must still read files written here (the
//! documented rollback path). So fields are only ever added, `task_id` stays
//! a required string, and the status words stay `pending`, `assigned` and
//! `held`. v0.3.3 ignores the fields it doesn't know.

use std::collections::BTreeMap;

use animus_execution_protocol::{
    ExecutionFence, QueueLeaseFence, RepositoryReservation, SubjectGeneration,
    EXECUTION_FENCE_SCHEMA_ID, EXECUTION_FENCE_VERSION,
};
use animus_subject_protocol::SubjectDispatch;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Format version written to `queue.json`. Files without the marker (queue
/// v0.3.3 and older) load as version 0.
pub const QUEUE_FORMAT_VERSION: u32 = 2;

/// Entry status. Wire form matches `animus-queue-protocol::status::*` strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DispatchQueueEntryStatus {
    /// Waiting to be leased.
    #[default]
    Pending,
    /// Leased; a workflow is running against it.
    Assigned,
    /// Held by operator action; non-dispatchable.
    Held,
    /// Forward-compat fallthrough for unknown wire values.
    #[serde(other)]
    Unknown,
}

impl DispatchQueueEntryStatus {
    /// Wire string value matching `animus_queue_protocol::status::*`.
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Pending => animus_queue_protocol::status::PENDING,
            Self::Assigned => animus_queue_protocol::status::ASSIGNED,
            Self::Held => animus_queue_protocol::status::HELD,
            // Unknown is wire-only — never returned to clients as a status.
            Self::Unknown => "unknown",
        }
    }
}

/// One queue entry. Keyed by `entry_id` (a UUID v4 string assigned on
/// enqueue) for all mutation calls.
///
/// An entry is *ticketed* once it has a `subject_generation`: it was added by
/// `queue/v2/enqueue`, or it is an old-style entry that got ticket identity
/// at its first `queue/v2/lease` hand-out. Ticketed entries store their
/// canonical `<kind>:<id>` in `subject_id`; old-style entries store the
/// legacy subject key there.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DispatchQueueEntry {
    /// Stable entry id (UUID v4) assigned on enqueue. Defaulted to the empty
    /// string on deserialization so legacy in-tree queue state (which did
    /// not carry `entry_id`) is detectable: `load_queue_state` walks the
    /// loaded entries, mints a fresh UUID for each empty slot, and persists
    /// the migrated file back to disk so subsequent calls see the same ids.
    #[serde(default)]
    pub entry_id: String,
    /// Cached subject id from the dispatch envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<String>,
    /// Task id (when the subject is a built-in task). Kept as a non-Option
    /// String for back-compat with the in-tree shape.
    pub task_id: String,
    /// Full dispatch envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<SubjectDispatch>,
    /// Current status.
    #[serde(default)]
    pub status: DispatchQueueEntryStatus,
    /// Attached workflow id (when Assigned).
    #[serde(default)]
    pub workflow_id: Option<String>,
    /// RFC 3339 enqueue timestamp.
    #[serde(default)]
    pub enqueued_at: Option<String>,
    /// RFC 3339 assignment timestamp.
    #[serde(default)]
    pub assigned_at: Option<String>,
    /// RFC 3339 hold timestamp.
    #[serde(default)]
    pub held_at: Option<String>,
    /// RFC 3339 earliest-dispatch time for a deferred entry. While `now` is
    /// before this instant the entry stays Pending but is excluded from
    /// `queue/lease`. `None` for ordinary (dispatch-ASAP) entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<String>,
    /// Grace window in seconds after `run_at` before a still-pending deferred
    /// entry is expired and dropped on sweep. `None` = never expire. Ignored
    /// when `run_at` is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_secs: Option<u64>,
    /// Audit log of state transitions recorded by reason-carrying mutations
    /// (currently `queue/release_pending`). Older transitions remain absent
    /// because legacy mutations did not record reasons.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_log: Vec<DispatchQueueAuditEntry>,
    /// Immutable subject generation. Set on ticketed entries only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_generation: Option<u64>,
    /// Workflow generation. Set to 1 at the first ticketed hand-out and kept
    /// through put-back and takeover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_generation: Option<u64>,
    /// Current lease holder: a daemon's owner id for ticketed leases, the
    /// workflow id for old-style leases (as v0.2.9 does).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_owner: Option<String>,
    /// Lease generation. Rises by one at every ticketed hand-out and
    /// takeover. 0 means never ticket-leased.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub lease_generation: u64,
    /// Lease expiry. Old-style leases get one too, but only old-style
    /// hand-outs by this version set it (v0.3.3 leases have none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// Repository and branch reserved by this entry (ticketed entries only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryReservation>,
    /// Producer idempotency key from `queue/v2/enqueue`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Content hash the idempotency key is bound to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_hash: Option<String>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// One row in [`DispatchQueueEntry::audit_log`]. Captures who/why a
/// reason-carrying state transition happened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DispatchQueueAuditEntry {
    /// RFC 3339 timestamp of the transition.
    pub at: String,
    /// JSON-RPC method that caused the transition (e.g. `queue/release_pending`).
    pub method: String,
    /// Status the entry held before the transition (wire form).
    pub from_status: String,
    /// Status the entry holds after the transition (wire form).
    pub to_status: String,
    /// Caller-supplied audit reason.
    pub reason: String,
}

/// On-disk top-level state shape.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct DispatchQueueState {
    /// File format version; see [`QUEUE_FORMAT_VERSION`]. 0 for files
    /// written by queue v0.3.3 and older.
    #[serde(default)]
    pub format_version: u32,
    /// Highest subject generation handed out per canonical subject id. Never
    /// decreases, and survives the queue emptying, so a finished task's next
    /// run gets a higher generation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub subject_generations: BTreeMap<String, u64>,
    /// Queue entries in priority/FIFO order.
    #[serde(default)]
    pub entries: Vec<DispatchQueueEntry>,
}

impl DispatchQueueEntry {
    /// Build a fresh Pending entry from a `SubjectDispatch`, assigning a
    /// stable `entry_id` and capturing `enqueued_at`. `run_at` /
    /// `expire_after_secs` carry deferred-dispatch metadata (both `None`
    /// for an ordinary dispatch-ASAP entry).
    pub fn from_dispatch(
        dispatch: SubjectDispatch,
        run_at: Option<String>,
        expire_after_secs: Option<u64>,
    ) -> Self {
        Self {
            entry_id: uuid::Uuid::new_v4().to_string(),
            subject_id: dispatch.subject_key(),
            task_id: dispatch.task_id().unwrap_or_default().to_string(),
            dispatch: Some(dispatch),
            status: DispatchQueueEntryStatus::Pending,
            enqueued_at: Some(chrono::Utc::now().to_rfc3339()),
            run_at,
            expire_after_secs,
            ..Self::default()
        }
    }

    /// `true` once the entry has ticket identity.
    pub fn is_ticketed(&self) -> bool {
        self.subject_generation.is_some()
    }

    /// `true` while the lease expiry is in the future.
    pub fn lease_is_live(&self, now: DateTime<Utc>) -> bool {
        self.lease_expires_at
            .is_some_and(|expires_at| expires_at > now)
    }

    /// The entry's execution fence, or `None` when any part of its ticket
    /// identity is missing (v0.2.9 `executionFromRow`).
    pub fn execution_fence(&self) -> Option<ExecutionFence> {
        let qualified_id = self.subject_id.clone().filter(|id| !id.is_empty())?;
        let subject_generation = self.subject_generation.filter(|g| *g > 0)?;
        let workflow_id = self.workflow_id.clone().filter(|id| !id.is_empty())?;
        let workflow_generation = self.workflow_generation.filter(|g| *g > 0)?;
        let owner_id = self.lease_owner.clone().filter(|id| !id.is_empty())?;
        let expires_at = self.lease_expires_at?;
        if self.lease_generation == 0 {
            return None;
        }
        Some(ExecutionFence {
            schema: EXECUTION_FENCE_SCHEMA_ID.to_string(),
            version: EXECUTION_FENCE_VERSION,
            workflow_id,
            workflow_generation,
            subject: Some(SubjectGeneration {
                qualified_id,
                generation: subject_generation,
            }),
            queue_lease: Some(QueueLeaseFence {
                entry_id: self.entry_id.clone(),
                owner_id,
                generation: self.lease_generation,
                expires_at,
            }),
            repository: self.repository.clone(),
        })
    }

    /// `true` when this entry is deferred and its `run_at` instant has not
    /// yet been reached as of `now`. Unparseable `run_at` values are treated
    /// as eligible (dispatch now) so a malformed timestamp never wedges an
    /// entry permanently.
    pub fn is_deferred_until_future(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        match self.parsed_run_at() {
            Some(run_at) => now < run_at,
            None => false,
        }
    }

    /// Parse `run_at` into a UTC instant, if present and well-formed.
    pub fn parsed_run_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let raw = self.run_at.as_deref()?;
        match chrono::DateTime::parse_from_rfc3339(raw) {
            Ok(dt) => Some(dt.with_timezone(&chrono::Utc)),
            Err(err) => {
                tracing::warn!(
                    entry_id = %self.entry_id,
                    run_at = raw,
                    error = %err,
                    "queue entry has unparseable run_at; treating as immediately eligible"
                );
                None
            }
        }
    }

    /// The instant at which a deferred entry should be expired (dropped
    /// instead of dispatched late): `run_at + expire_after_secs`. `None`
    /// when the entry is not deferred or has no expiry window.
    pub fn expiry_deadline(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let run_at = self.parsed_run_at()?;
        let secs = self.expire_after_secs?;
        Some(run_at + chrono::Duration::seconds(secs as i64))
    }

    /// Effective subject id (falls back to `dispatch.subject_id` then to
    /// `task_id`).
    pub fn subject_id_ref(&self) -> &str {
        if let Some(subject_id) = self
            .subject_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return subject_id;
        }
        if let Some(subject_id) = self.dispatch.as_ref().and_then(SubjectDispatch::subject_id) {
            return subject_id;
        }
        self.task_id.as_str()
    }

    /// Effective task id (None when this entry's subject is not a built-in
    /// task).
    pub fn task_id_ref(&self) -> Option<&str> {
        self.dispatch
            .as_ref()
            .and_then(SubjectDispatch::task_id)
            .or_else(|| (!self.task_id.trim().is_empty()).then_some(self.task_id.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_subject_protocol::SubjectRef;

    fn ticketed_entry() -> DispatchQueueEntry {
        DispatchQueueEntry {
            entry_id: "entry-1".to_string(),
            subject_id: Some("task:TASK-1".to_string()),
            task_id: "TASK-1".to_string(),
            dispatch: Some(SubjectDispatch::for_subject_with_metadata(
                SubjectRef::task("TASK-1"),
                "coding",
                "test",
                Utc::now(),
            )),
            status: DispatchQueueEntryStatus::Assigned,
            workflow_id: Some("workflow-1".to_string()),
            subject_generation: Some(3),
            workflow_generation: Some(1),
            lease_owner: Some("daemon-a".to_string()),
            lease_generation: 2,
            lease_expires_at: Some("2030-01-01T00:00:00Z".parse().unwrap()),
            repository: Some(RepositoryReservation {
                repository: "https://github.com/launchapp-dev/animus-cli.git".to_string(),
                base_ref: "refs/heads/main".to_string(),
                head_ref: "refs/heads/animus/TASK-1".to_string(),
            }),
            ..DispatchQueueEntry::default()
        }
    }

    #[test]
    fn execution_fence_carries_the_full_ticket() {
        let entry = ticketed_entry();
        let fence = entry.execution_fence().expect("complete identity");
        fence.validate_coding().expect("valid coding fence");
        assert_eq!(fence.workflow_id, "workflow-1");
        assert_eq!(fence.workflow_generation, 1);
        let subject = fence.subject.as_ref().unwrap();
        assert_eq!(subject.qualified_id, "task:TASK-1");
        assert_eq!(subject.generation, 3);
        let lease = fence.queue_lease.as_ref().unwrap();
        assert_eq!(lease.entry_id, "entry-1");
        assert_eq!(lease.owner_id, "daemon-a");
        assert_eq!(lease.generation, 2);
        assert_eq!(fence.repository, entry.repository);
    }

    #[test]
    fn execution_fence_needs_every_identity_part() {
        let mut missing_owner = ticketed_entry();
        missing_owner.lease_owner = None;
        assert!(missing_owner.execution_fence().is_none());

        let mut never_leased = ticketed_entry();
        never_leased.lease_generation = 0;
        assert!(never_leased.execution_fence().is_none());

        let mut old_style = ticketed_entry();
        old_style.subject_generation = None;
        assert!(!old_style.is_ticketed());
        assert!(old_style.execution_fence().is_none());
    }

    #[test]
    fn lease_liveness_follows_expiry() {
        let mut entry = ticketed_entry();
        let now = Utc::now();
        entry.lease_expires_at = Some(now + chrono::Duration::seconds(5));
        assert!(entry.lease_is_live(now));
        entry.lease_expires_at = Some(now - chrono::Duration::seconds(5));
        assert!(!entry.lease_is_live(now));
        entry.lease_expires_at = None;
        assert!(!entry.lease_is_live(now));
    }

    #[test]
    fn old_style_entries_serialize_without_ticket_fields() {
        let entry = DispatchQueueEntry::from_dispatch(
            SubjectDispatch::for_subject_with_metadata(
                SubjectRef::task("TASK-2"),
                "standard",
                "test",
                Utc::now(),
            ),
            None,
            None,
        );
        let value = serde_json::to_value(&entry).unwrap();
        for field in [
            "subject_generation",
            "workflow_generation",
            "lease_owner",
            "lease_generation",
            "lease_expires_at",
            "repository",
            "idempotency_key",
            "request_hash",
        ] {
            assert!(value.get(field).is_none(), "{field} must be omitted");
        }
        // v0.3.3 requires these.
        assert_eq!(value["task_id"], "TASK-2");
        assert_eq!(value["status"], "pending");
    }

    #[test]
    fn ticket_fields_round_trip() {
        let entry = ticketed_entry();
        let text = serde_json::to_string(&entry).unwrap();
        let back: DispatchQueueEntry = serde_json::from_str(&text).unwrap();
        assert_eq!(back, entry);
    }
}
```

Replace the whole of `src/dispatch_queue_store.rs` with:

````rust
//! File-locked persistence for the queue state.
//!
//! Layout under the bound project root:
//!
//! ```text
//! <project_root>/.animus/queue.json
//! <project_root>/.animus/queue.lock
//! ```
//!
//! Writes go to a temp file that is flushed to disk (fsync) and then renamed
//! over `queue.json`, so readers see either the old or the new state.
//! Mutations hold an exclusive `fs2` lock across the read-modify-write cycle.
//! The file is kept even when the queue is empty, because it carries the
//! per-subject generation counters.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use fs2::FileExt;
use uuid::Uuid;

use crate::dispatch_queue_state::{DispatchQueueEntry, DispatchQueueState, QUEUE_FORMAT_VERSION};

const ANIMUS_DIR: &str = ".animus";
const QUEUE_STATE_FILE: &str = "queue.json";
const QUEUE_LOCK_FILE: &str = "queue.lock";

/// Absolute path to the queue state file for the bound project root.
pub fn queue_state_path(project_root: &Path) -> PathBuf {
    project_root.join(ANIMUS_DIR).join(QUEUE_STATE_FILE)
}

/// Absolute path to the queue lock file for the bound project root.
pub fn queue_lock_path(project_root: &Path) -> PathBuf {
    project_root.join(ANIMUS_DIR).join(QUEUE_LOCK_FILE)
}

/// Acquire an exclusive lock on `queue.lock`. Returned guard releases the
/// lock on drop.
///
/// Held only across read-modify-write cycles, never across IPC.
pub(crate) fn acquire_queue_lock(project_root: &Path) -> Result<File> {
    let lock_path = queue_lock_path(project_root);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create animus state dir at {}", parent.display())
        })?;
    }
    let file = File::create(&lock_path)
        .with_context(|| format!("failed to open queue lock file at {}", lock_path.display()))?;
    file.lock_exclusive()
        .with_context(|| format!("failed to acquire queue lock at {}", lock_path.display()))?;
    Ok(file)
}

/// Load the queue state from disk. Returns `Ok(None)` when no state file
/// exists yet.
pub fn load_queue_state(project_root: &Path) -> Result<Option<DispatchQueueState>> {
    let path = queue_state_path(project_root);
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        // No file yet: a project that never queued anything.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!(
                "failed to read queue state file at {}",
                path.display()
            )));
        }
    };
    if content.trim().is_empty() {
        return Ok(Some(DispatchQueueState::default()));
    }

    // Tolerate the legacy bare-array shape that earlier dev builds wrote.
    let mut state: DispatchQueueState = serde_json::from_str::<DispatchQueueState>(&content)
        .or_else(|_| {
            serde_json::from_str::<Vec<DispatchQueueEntry>>(&content).map(|entries| {
                DispatchQueueState {
                    entries,
                    ..DispatchQueueState::default()
                }
            })
        })
        .with_context(|| format!("failed to parse queue state file at {}", path.display()))?;
    if state.format_version > QUEUE_FORMAT_VERSION {
        anyhow::bail!(
            "queue state at {} has format_version {}, but animus-queue-default v{} reads up to \
             {QUEUE_FORMAT_VERSION}; install a newer queue plugin",
            path.display(),
            state.format_version,
            env!("CARGO_PKG_VERSION"),
        );
    }

    // Migration: legacy in-tree queue state did not carry `entry_id`. Mint
    // stable UUIDs for any empty ids and persist the migrated file back so
    // subsequent calls see the same ids (otherwise `queue/list` would hand
    // out ids that are invalidated on the next mutation reload).
    let migrated = migrate_missing_entry_ids(&mut state);
    if migrated {
        // Persist atomically; callers that hold the queue lock will retry
        // their read-modify-write cycle on the migrated state.
        save_queue_state(project_root, &state).with_context(|| {
            format!(
                "failed to persist migrated entry ids for queue state at {}",
                path.display()
            )
        })?;
    }
    Ok(Some(state))
}

fn migrate_missing_entry_ids(state: &mut DispatchQueueState) -> bool {
    let mut migrated = false;
    for entry in state.entries.iter_mut() {
        if entry.entry_id.trim().is_empty() {
            entry.entry_id = Uuid::new_v4().to_string();
            migrated = true;
        }
    }
    migrated
}

/// Persist the queue state to disk, stamped with [`QUEUE_FORMAT_VERSION`].
/// The file is written even when the queue is empty.
pub fn save_queue_state(project_root: &Path, state: &DispatchQueueState) -> Result<()> {
    let path = queue_state_path(project_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create animus state dir at {}", parent.display())
        })?;
    }

    let mut on_disk = state.clone();
    on_disk.format_version = QUEUE_FORMAT_VERSION;
    let payload =
        serde_json::to_string_pretty(&on_disk).context("failed to serialize queue state")?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(QUEUE_STATE_FILE);
    let tmp_path = path.with_file_name(format!("{}.{}.tmp", file_name, Uuid::new_v4()));
    let write_tmp = || -> std::io::Result<()> {
        let mut file = File::create(&tmp_path)?;
        file.write_all(payload.as_bytes())?;
        file.sync_all()
    };
    write_tmp().with_context(|| {
        format!(
            "failed to write temporary queue state at {}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, &path)
        .with_context(|| format!("failed to publish queue state to {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_state_is_written_and_keeps_generation_counters() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut state = DispatchQueueState::default();
        state
            .subject_generations
            .insert("task:TASK-1".to_string(), 4);

        save_queue_state(temp.path(), &state).expect("save");

        let raw: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(queue_state_path(temp.path())).unwrap())
                .unwrap();
        assert_eq!(raw["format_version"], QUEUE_FORMAT_VERSION);
        assert_eq!(raw["subject_generations"]["task:TASK-1"], 4);
        assert_eq!(raw["entries"], serde_json::json!([]));
        let loaded = load_queue_state(temp.path()).expect("load").expect("state");
        assert_eq!(loaded.subject_generations.get("task:TASK-1"), Some(&4));
    }

    #[test]
    fn file_without_marker_loads_as_format_0() {
        let temp = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join(".animus")).unwrap();
        fs::write(queue_state_path(temp.path()), r#"{"entries": []}"#).unwrap();

        let loaded = load_queue_state(temp.path()).expect("load").expect("state");

        assert_eq!(loaded.format_version, 0);
        assert!(loaded.subject_generations.is_empty());
    }

    #[test]
    fn newer_format_is_refused() {
        let temp = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(temp.path().join(".animus")).unwrap();
        fs::write(
            queue_state_path(temp.path()),
            r#"{"format_version": 3, "entries": []}"#,
        )
        .unwrap();

        let error = load_queue_state(temp.path()).expect_err("format 3 must be refused");

        assert!(error.to_string().contains("format_version 3"), "{error}");
    }

    #[test]
    fn save_leaves_no_temp_files_behind() {
        let temp = tempfile::tempdir().expect("tempdir");
        save_queue_state(temp.path(), &DispatchQueueState::default()).expect("save");
        let names: Vec<String> = fs::read_dir(temp.path().join(".animus"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["queue.json".to_string()]);
    }
}
````

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --all-features`

Expected: all tests pass, including `dispatch_queue_store::tests::newer_format_is_refused` and `empty_state_is_written_and_keeps_generation_counters`.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: queue.json format 2 with ticket fields and per-task counters"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 6: Finished-entry history

Spec §6.1 and §6.2. Finished entries (completed, failed, cancelled, dropped) move to `.animus/queue-history.jsonl`, one JSON line each. The line is appended and fsynced **before** `queue.json` is replaced, so a crash between the two leaves the entry live and the retry finishes it again. Readers take the first line per entry and skip lines that don't parse (for example a torn final write). The history is read only when a call can't find its entry in `queue.json`.

The old-style `completion`, `drop` and the expiry sweep now record history through one `commit` helper.

**Files:**

- Modify: `src/lib.rs`
- Create: `src/queue_history.rs`
- Modify: `src/queue_service.rs`
- Modify: `tests/common/mod.rs`
- Create: `tests/history.rs`

**Interfaces:**

- Produces (`src/queue_history.rs`):
  - `pub enum HistoryOutcome { Completed, Failed, Cancelled, Dropped }`
    - `from_completion_status(&str) -> Option<Self>`
    - `state_word(self) -> &'static str`, giving `"done"` or `"dropped"`
  - `pub struct HistoryRecord { outcome, finished_at, finished_by, reason, entry }`
    - `HistoryRecord::finished(entry: &DispatchQueueEntry, outcome, finished_by: &str, reason: Option<&str>) -> Self`
  - `pub fn queue_history_path(project_root: &Path) -> PathBuf`
  - `pub fn append_history(project_root: &Path, records: &[HistoryRecord]) -> Result<()>`
  - `pub fn find_history_by_entry_id(project_root: &Path, entry_id: &str) -> Result<Option<HistoryRecord>>`
  - `pub fn find_history_by_idempotency_key(project_root: &Path, key: &str) -> Result<Option<HistoryRecord>>`
- Produces (`src/queue_service.rs`):
  - `pub(crate) fn commit(&self, state: &DispatchQueueState, finished: &[HistoryRecord]) -> Result<()>`
  - `pub(crate) fn sweep_expired_entries(state: &mut DispatchQueueState, now: DateTime<Utc>) -> Vec<HistoryRecord>`
- Produces (`tests/common/mod.rs`): `task_dispatch(task_id: &str, workflow_ref: &str) -> SubjectDispatch`.

- [ ] **Step 1: Write the failing tests**

Apply to `tests/common/mod.rs` (`git apply` accepts this patch):

```diff
diff --git a/tests/common/mod.rs b/tests/common/mod.rs
index 2dccc68..a011865 100644
--- a/tests/common/mod.rs
+++ b/tests/common/mod.rs
@@ -5,8 +5,20 @@ use std::io::{BufRead, BufReader, Write};
 use std::path::Path;
 use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
 
+use animus_subject_protocol::{SubjectDispatch, SubjectRef};
+use chrono::Utc;
 use serde_json::{json, Value};
 
+/// A task dispatch requested now.
+pub fn task_dispatch(task_id: &str, workflow_ref: &str) -> SubjectDispatch {
+    SubjectDispatch::for_subject_with_metadata(
+        SubjectRef::task(task_id),
+        workflow_ref,
+        "integration-test",
+        Utc::now(),
+    )
+}
+
 /// A running plugin process driven over stdio, one request at a time.
 pub struct PluginProcess {
     child: Child,
```

Create `tests/history.rs`:

```rust
//! Finished entries move from `queue.json` to `queue-history.jsonl`.

mod common;

use animus_queue_default::queue_history::{find_history_by_entry_id, HistoryOutcome};
use animus_queue_default::{queue_state_path, QueueBackend};
use chrono::Utc;
use common::task_dispatch;

#[test]
fn completion_moves_the_entry_to_history() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    backend
        .lease(1, Some(vec!["wf-1".to_string()]), None)
        .expect("lease");

    let outcome = backend
        .completion(&enqueued.entry_id, "failed", None, Some("wf-1"))
        .expect("completion");

    assert!(outcome.changed);
    let record = find_history_by_entry_id(temp.path(), &enqueued.entry_id)
        .expect("read history")
        .expect("history record");
    assert_eq!(record.outcome, HistoryOutcome::Failed);
    assert_eq!(record.finished_by, "queue/completion");
    assert_eq!(record.entry.workflow_id.as_deref(), Some("wf-1"));
    assert_eq!(backend.list(&[], None, None).expect("list").total, 0);
    assert!(
        queue_state_path(temp.path()).exists(),
        "queue.json stays even when empty"
    );
}

#[test]
fn drop_moves_the_entry_to_history() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");

    assert!(
        backend
            .drop_entry(&enqueued.entry_id)
            .expect("drop")
            .changed
    );

    let record = find_history_by_entry_id(temp.path(), &enqueued.entry_id)
        .expect("read history")
        .expect("history record");
    assert_eq!(record.outcome, HistoryOutcome::Dropped);
    assert_eq!(record.finished_by, "queue/drop");
}

#[test]
fn expired_deferred_entries_are_recorded_as_dropped() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = QueueBackend::new(temp.path().to_path_buf());
    let run_at = (Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), Some(run_at), Some(60))
        .expect("enqueue");

    assert!(backend
        .lease(5, None, None)
        .expect("lease")
        .leased
        .is_empty());

    let record = find_history_by_entry_id(temp.path(), &enqueued.entry_id)
        .expect("read history")
        .expect("history record");
    assert_eq!(record.outcome, HistoryOutcome::Dropped);
    assert_eq!(record.finished_by, "expiry-sweep");
    assert!(record.reason.is_some());
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test history`

Expected: compile error: unresolved import `animus_queue_default::queue_history`.

- [ ] **Step 3: Implement**

Apply to `src/lib.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/lib.rs b/src/lib.rs
index d435f7a..0bf2d5c 100644
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -24,6 +24,7 @@ pub mod host_guard;
 pub mod identity;
 pub mod lease_ttl;
 pub mod plugin;
+pub mod queue_history;
 pub mod queue_service;
 pub mod request_hash;
 
```

Create `src/queue_history.rs`:

```rust
//! Finished-entry history: `<project_root>/.animus/queue-history.jsonl`.
//!
//! One JSON line per finished entry: completed, failed, cancelled or dropped.
//! Kept forever, like the Postgres queues' finished rows. It is read only
//! when a call doesn't find its entry in `queue.json`: a repeated ticketed
//! "done", or an idempotency key whose entry already finished.
//!
//! Crash safety: finishing an entry appends and fsyncs its history line
//! *before* `queue.json` is replaced. A crash in between leaves the entry
//! live, so the caller's retry finishes it again and appends a second line.
//! Readers take the first line per entry, and skip lines that don't parse,
//! such as a torn final write.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use animus_queue_protocol::completion_status;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::dispatch_queue_state::DispatchQueueEntry;

const ANIMUS_DIR: &str = ".animus";
const QUEUE_HISTORY_FILE: &str = "queue-history.jsonl";

/// How an entry finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryOutcome {
    /// The workflow completed.
    Completed,
    /// The workflow failed.
    Failed,
    /// The workflow was cancelled.
    Cancelled,
    /// An operator dropped the entry, or it expired before it could run.
    Dropped,
}

impl HistoryOutcome {
    /// Outcome for a terminal completion status; `None` for anything else.
    pub fn from_completion_status(status: &str) -> Option<Self> {
        match status {
            completion_status::COMPLETED => Some(Self::Completed),
            completion_status::FAILED => Some(Self::Failed),
            completion_status::CANCELLED => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// animus-postgres v0.2.9's state word for a finished row.
    pub fn state_word(self) -> &'static str {
        match self {
            Self::Dropped => "dropped",
            Self::Completed | Self::Failed | Self::Cancelled => "done",
        }
    }
}

/// One finished entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryRecord {
    /// How the entry finished.
    pub outcome: HistoryOutcome,
    /// When it finished.
    pub finished_at: DateTime<Utc>,
    /// What finished it: an RPC method name or `expiry-sweep`.
    pub finished_by: String,
    /// Optional detail, such as why a sweep dropped the entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The entry as it was when it finished, minus its dispatch envelope and
    /// audit log (kept out to bound the line size).
    pub entry: DispatchQueueEntry,
}

impl HistoryRecord {
    /// Record `entry` finishing now.
    pub fn finished(
        entry: &DispatchQueueEntry,
        outcome: HistoryOutcome,
        finished_by: &str,
        reason: Option<&str>,
    ) -> Self {
        let mut snapshot = entry.clone();
        snapshot.dispatch = None;
        snapshot.audit_log.clear();
        Self {
            outcome,
            finished_at: Utc::now(),
            finished_by: finished_by.to_string(),
            reason: reason.map(str::to_string),
            entry: snapshot,
        }
    }
}

/// Absolute path to the history file for the bound project root.
pub fn queue_history_path(project_root: &Path) -> PathBuf {
    project_root.join(ANIMUS_DIR).join(QUEUE_HISTORY_FILE)
}

/// Append `records` and flush them to disk (fsync). Call with the queue lock
/// held and before saving `queue.json`.
pub fn append_history(project_root: &Path, records: &[HistoryRecord]) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let path = queue_history_path(project_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create animus state dir at {}", parent.display())
        })?;
    }
    let mut payload = String::new();
    for record in records {
        payload.push_str(&serde_json::to_string(record).context("failed to encode history")?);
        payload.push('\n');
    }
    let append = || -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        // A crash can leave a torn last line without its newline. Start on a
        // fresh line so the new records stay readable.
        if ends_without_newline(&mut file)? {
            file.write_all(b"\n")?;
        }
        file.write_all(payload.as_bytes())?;
        file.sync_all()
    };
    append().with_context(|| format!("failed to append queue history at {}", path.display()))
}

fn ends_without_newline(file: &mut File) -> std::io::Result<bool> {
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(len - 1))?;
    let mut last = [0u8; 1];
    file.read_exact(&mut last)?;
    Ok(last[0] != b'\n')
}

/// The first history record for `entry_id`.
pub fn find_history_by_entry_id(
    project_root: &Path,
    entry_id: &str,
) -> Result<Option<HistoryRecord>> {
    find_first(project_root, |record| record.entry.entry_id == entry_id)
}

/// The first history record whose entry carried idempotency key `key`.
pub fn find_history_by_idempotency_key(
    project_root: &Path,
    key: &str,
) -> Result<Option<HistoryRecord>> {
    find_first(project_root, |record| {
        record.entry.idempotency_key.as_deref() == Some(key)
    })
}

fn find_first(
    project_root: &Path,
    matches: impl Fn(&HistoryRecord) -> bool,
) -> Result<Option<HistoryRecord>> {
    let path = queue_history_path(project_root);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!(
                "failed to open queue history at {}",
                path.display()
            )));
        }
    };
    // Split on raw bytes: a torn line may end inside a UTF-8 sequence.
    for (index, line) in BufReader::new(file).split(b'\n').enumerate() {
        let line =
            line.with_context(|| format!("failed to read queue history at {}", path.display()))?;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<HistoryRecord>(&line) {
            Ok(record) if matches(&record) => return Ok(Some(record)),
            Ok(_) => {}
            Err(error) => tracing::warn!(
                path = %path.display(),
                line = index + 1,
                %error,
                "skipping unreadable queue history line"
            ),
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(entry_id: &str, key: Option<&str>) -> DispatchQueueEntry {
        DispatchQueueEntry {
            entry_id: entry_id.to_string(),
            subject_id: Some("task:TASK-1".to_string()),
            task_id: "TASK-1".to_string(),
            idempotency_key: key.map(str::to_string),
            ..DispatchQueueEntry::default()
        }
    }

    #[test]
    fn missing_file_finds_nothing() {
        let temp = tempfile::tempdir().expect("tempdir");
        assert!(find_history_by_entry_id(temp.path(), "e1")
            .unwrap()
            .is_none());
        assert!(find_history_by_idempotency_key(temp.path(), "k1")
            .unwrap()
            .is_none());
    }

    #[test]
    fn records_are_found_by_entry_id_and_key() {
        let temp = tempfile::tempdir().expect("tempdir");
        append_history(
            temp.path(),
            &[
                HistoryRecord::finished(
                    &entry("e1", Some("k1")),
                    HistoryOutcome::Completed,
                    "queue/v2/completion",
                    None,
                ),
                HistoryRecord::finished(
                    &entry("e2", None),
                    HistoryOutcome::Dropped,
                    "queue/drop",
                    None,
                ),
            ],
        )
        .unwrap();

        let by_id = find_history_by_entry_id(temp.path(), "e2")
            .unwrap()
            .unwrap();
        assert_eq!(by_id.outcome, HistoryOutcome::Dropped);
        assert_eq!(by_id.finished_by, "queue/drop");
        let by_key = find_history_by_idempotency_key(temp.path(), "k1")
            .unwrap()
            .unwrap();
        assert_eq!(by_key.entry.entry_id, "e1");
    }

    #[test]
    fn first_record_per_entry_wins() {
        let temp = tempfile::tempdir().expect("tempdir");
        let e1 = entry("e1", None);
        append_history(
            temp.path(),
            &[HistoryRecord::finished(
                &e1,
                HistoryOutcome::Completed,
                "queue/v2/completion",
                None,
            )],
        )
        .unwrap();
        append_history(
            temp.path(),
            &[HistoryRecord::finished(
                &e1,
                HistoryOutcome::Failed,
                "queue/v2/completion",
                None,
            )],
        )
        .unwrap();

        let found = find_history_by_entry_id(temp.path(), "e1")
            .unwrap()
            .unwrap();
        assert_eq!(found.outcome, HistoryOutcome::Completed);
    }

    #[test]
    fn torn_last_line_is_skipped_and_later_appends_stay_readable() {
        let temp = tempfile::tempdir().expect("tempdir");
        append_history(
            temp.path(),
            &[HistoryRecord::finished(
                &entry("e1", None),
                HistoryOutcome::Completed,
                "queue/completion",
                None,
            )],
        )
        .unwrap();
        // Simulate a crash mid-write: half a record, ending inside a UTF-8
        // sequence, with no newline.
        let torn = b"{\"outcome\":\"completed\",\"entry\":{\"entry_id\":\"e9\xE2\x82";
        let path = queue_history_path(temp.path());
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(torn).unwrap();
        drop(file);

        append_history(
            temp.path(),
            &[HistoryRecord::finished(
                &entry("e2", None),
                HistoryOutcome::Failed,
                "queue/completion",
                None,
            )],
        )
        .unwrap();

        assert!(find_history_by_entry_id(temp.path(), "e1")
            .unwrap()
            .is_some());
        assert_eq!(
            find_history_by_entry_id(temp.path(), "e2")
                .unwrap()
                .unwrap()
                .outcome,
            HistoryOutcome::Failed
        );
        assert!(find_history_by_entry_id(temp.path(), "e9")
            .unwrap()
            .is_none());
    }

    #[test]
    fn records_leave_out_the_dispatch_and_audit_log() {
        let mut with_payload = entry("e1", None);
        with_payload
            .audit_log
            .push(crate::dispatch_queue_state::DispatchQueueAuditEntry {
                at: "2026-09-28T00:00:00Z".to_string(),
                method: "queue/release_pending".to_string(),
                from_status: "assigned".to_string(),
                to_status: "pending".to_string(),
                reason: "test".to_string(),
            });
        let record = HistoryRecord::finished(
            &with_payload,
            HistoryOutcome::Completed,
            "queue/completion",
            None,
        );
        assert!(record.entry.dispatch.is_none());
        assert!(record.entry.audit_log.is_empty());
    }

    #[test]
    fn state_words_follow_v029() {
        assert_eq!(HistoryOutcome::Completed.state_word(), "done");
        assert_eq!(HistoryOutcome::Failed.state_word(), "done");
        assert_eq!(HistoryOutcome::Cancelled.state_word(), "done");
        assert_eq!(HistoryOutcome::Dropped.state_word(), "dropped");
        assert_eq!(
            HistoryOutcome::from_completion_status("failed"),
            Some(HistoryOutcome::Failed)
        );
        assert_eq!(HistoryOutcome::from_completion_status("done"), None);
    }
}
```

Apply to `src/queue_service.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/queue_service.rs b/src/queue_service.rs
index 372876a..89d3169 100644
--- a/src/queue_service.rs
+++ b/src/queue_service.rs
@@ -7,9 +7,8 @@
 use std::path::{Path, PathBuf};
 
 use animus_queue_protocol::{
-    completion_status, status, QueueEntry, QueueLeaseResponse, QueueListResponse,
-    QueueMutationResponse, QueueNextDeadlineResponse, QueueReleasePendingResponse,
-    QueueReorderResponse, QueueStats,
+    status, QueueEntry, QueueLeaseResponse, QueueListResponse, QueueMutationResponse,
+    QueueNextDeadlineResponse, QueueReleasePendingResponse, QueueReorderResponse, QueueStats,
 };
 use animus_subject_protocol::SubjectDispatch;
 use anyhow::Result;
@@ -20,6 +19,7 @@ use crate::dispatch_queue_state::{
 };
 use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state, save_queue_state};
 use crate::lease_ttl::DEFAULT_LEASE_TTL_SECS;
+use crate::queue_history::{append_history, HistoryOutcome, HistoryRecord};
 
 /// File-locked backend wrapping a single project root's queue state.
 #[derive(Debug, Clone)]
@@ -101,7 +101,7 @@ impl QueueBackend {
 
         // Drop any expired deferred entries before evaluating this enqueue so
         // duplicate counts reflect the live queue.
-        sweep_expired_entries(&mut state, Utc::now());
+        let finished = sweep_expired_entries(&mut state, Utc::now());
 
         // Count live (non-Unknown) entries already targeting this subject —
         // used for the advisory warning. Enqueue is NOT idempotent in either
@@ -129,7 +129,7 @@ impl QueueBackend {
         let entry = DispatchQueueEntry::from_dispatch(dispatch, run_at, expire_after_secs);
         let entry_id = entry.entry_id.clone();
         state.entries.push(entry);
-        save_queue_state(&self.project_root, &state)?;
+        self.commit(&state, &finished)?;
         Ok(EnqueueOutcome {
             enqueued: true,
             entry_id,
@@ -208,7 +208,7 @@ impl QueueBackend {
         let _lock = acquire_queue_lock(&self.project_root)?;
         let mut state = load_queue_state(&self.project_root)?.unwrap_or_default();
         let now = Utc::now();
-        let swept = sweep_expired_entries(&mut state, now);
+        let finished = sweep_expired_entries(&mut state, now);
         let next_run_at = state
             .entries
             .iter()
@@ -217,8 +217,8 @@ impl QueueBackend {
             .filter(|run_at| *run_at > now)
             .min()
             .map(|run_at| run_at.to_rfc3339());
-        if swept > 0 {
-            save_queue_state(&self.project_root, &state)?;
+        if !finished.is_empty() {
+            self.commit(&state, &finished)?;
         }
         Ok(QueueNextDeadlineResponse { next_run_at })
     }
@@ -268,7 +268,7 @@ impl QueueBackend {
         let now = Utc::now();
         // Drop deferred entries that blew past their expiry window while the
         // daemon was unavailable, instead of dispatching them late.
-        let swept = sweep_expired_entries(&mut state, now);
+        let finished = sweep_expired_entries(&mut state, now);
         let now_rfc3339 = now.to_rfc3339();
         let mut leased: Vec<QueueEntry> = Vec::new();
         let mut assigned_index = 0usize;
@@ -330,8 +330,9 @@ impl QueueBackend {
             }
         }
 
-        if !leased.is_empty() || swept > 0 {
-            save_queue_state(&self.project_root, &state).map_err(QueueLeaseError::Backend)?;
+        if !leased.is_empty() || !finished.is_empty() {
+            self.commit(&state, &finished)
+                .map_err(QueueLeaseError::Backend)?;
         }
         Ok(QueueLeaseResponse { leased })
     }
@@ -380,16 +381,19 @@ impl QueueBackend {
                 not_found: true,
             });
         };
-        let before = state.entries.len();
-        state.entries.retain(|entry| entry.entry_id != entry_id);
-        let removed = before.saturating_sub(state.entries.len());
-        if removed == 0 {
+        let Some(index) = state
+            .entries
+            .iter()
+            .position(|entry| entry.entry_id == entry_id)
+        else {
             return Ok(QueueMutationResponse {
                 changed: false,
                 not_found: true,
             });
-        }
-        save_queue_state(&self.project_root, &state)?;
+        };
+        let dropped = state.entries.remove(index);
+        let record = HistoryRecord::finished(&dropped, HistoryOutcome::Dropped, "queue/drop", None);
+        self.commit(&state, &[record])?;
         Ok(QueueMutationResponse {
             changed: true,
             not_found: false,
@@ -492,14 +496,11 @@ impl QueueBackend {
         workflow_ref: Option<&str>,
         workflow_id: Option<&str>,
     ) -> Result<QueueMutationResponse> {
-        if !matches!(
-            status,
-            completion_status::COMPLETED | completion_status::FAILED | completion_status::CANCELLED
-        ) {
+        let Some(outcome) = HistoryOutcome::from_completion_status(status) else {
             return Err(anyhow::anyhow!(
                 "invalid completion status: '{status}' (expected one of: completed, failed, cancelled)"
             ));
-        }
+        };
 
         let _lock = acquire_queue_lock(&self.project_root)?;
         let Some(mut state) = load_queue_state(&self.project_root)? else {
@@ -508,46 +509,34 @@ impl QueueBackend {
                 not_found: true,
             });
         };
-        let before = state.entries.len();
-        state.entries.retain(|entry| {
-            if entry.entry_id != entry_id {
-                return true;
-            }
-            // Completion only prunes Assigned entries — a stale or misrouted
-            // completion frame for a Pending/Held entry must NOT delete queued
-            // work that was never leased.
-            if entry.status != DispatchQueueEntryStatus::Assigned {
-                return true;
-            }
-            // Match workflow_ref / workflow_id when provided.
-            if let Some(workflow_ref) = workflow_ref {
-                if entry
-                    .dispatch
-                    .as_ref()
-                    .is_some_and(|dispatch| dispatch.workflow_ref != workflow_ref)
-                {
-                    return true;
-                }
-            }
-            if let Some(workflow_id) = workflow_id {
-                if entry
-                    .workflow_id
-                    .as_deref()
-                    .is_some_and(|existing| existing != workflow_id)
-                {
-                    return true;
-                }
-            }
-            false
-        });
-        let removed = before.saturating_sub(state.entries.len());
-        if removed == 0 {
+        let Some(index) = state.entries.iter().position(|entry| {
+            entry.entry_id == entry_id
+                // Completion only finishes Assigned entries — a stale or
+                // misrouted completion frame for a Pending/Held entry must NOT
+                // delete queued work that was never leased.
+                && entry.status == DispatchQueueEntryStatus::Assigned
+                // Match workflow_ref / workflow_id when provided.
+                && workflow_ref.is_none_or(|workflow_ref| {
+                    entry
+                        .dispatch
+                        .as_ref()
+                        .is_none_or(|dispatch| dispatch.workflow_ref == workflow_ref)
+                })
+                && workflow_id.is_none_or(|workflow_id| {
+                    entry
+                        .workflow_id
+                        .as_deref()
+                        .is_none_or(|existing| existing == workflow_id)
+                })
+        }) else {
             return Ok(QueueMutationResponse {
                 changed: false,
                 not_found: true,
             });
-        }
-        save_queue_state(&self.project_root, &state)?;
+        };
+        let done = state.entries.remove(index);
+        let record = HistoryRecord::finished(&done, outcome, "queue/completion", None);
+        self.commit(&state, &[record])?;
         Ok(QueueMutationResponse {
             changed: true,
             not_found: false,
@@ -623,6 +612,17 @@ impl QueueBackend {
     // Internal helpers
     // ============================================================
 
+    /// Persist `state`, first recording `finished` entries in the history
+    /// (see [`crate::queue_history`] for why this order is crash-safe).
+    pub(crate) fn commit(
+        &self,
+        state: &DispatchQueueState,
+        finished: &[HistoryRecord],
+    ) -> Result<()> {
+        append_history(&self.project_root, finished)?;
+        save_queue_state(&self.project_root, state)
+    }
+
     fn mutate_entry<F>(&self, entry_id: &str, mutate: F) -> Result<QueueMutationResponse>
     where
         F: FnOnce(&mut DispatchQueueEntry) -> std::result::Result<bool, MutationError>,
@@ -770,12 +770,13 @@ fn entry_to_protocol(entry: &DispatchQueueEntry) -> Option<QueueEntry> {
 /// Remove Pending deferred entries whose expiry window has elapsed (`now`
 /// is past `run_at + expire_after_secs`). Only Pending entries are swept —
 /// an entry already Assigned/Held is in flight and out of scope. Returns
-/// the number of entries dropped so callers know whether to persist.
-fn sweep_expired_entries(
+/// history records for the dropped entries; callers persist them with
+/// [`QueueBackend::commit`].
+pub(crate) fn sweep_expired_entries(
     state: &mut DispatchQueueState,
     now: chrono::DateTime<chrono::Utc>,
-) -> usize {
-    let before = state.entries.len();
+) -> Vec<HistoryRecord> {
+    let mut finished = Vec::new();
     state.entries.retain(|entry| {
         if entry.status != DispatchQueueEntryStatus::Pending {
             return true;
@@ -787,12 +788,18 @@ fn sweep_expired_entries(
                     subject_id = entry.subject_id_ref(),
                     "queue: expiring deferred entry past its run_at + expire_after_secs window"
                 );
+                finished.push(HistoryRecord::finished(
+                    entry,
+                    HistoryOutcome::Dropped,
+                    "expiry-sweep",
+                    Some("run_at + expire_after_secs passed before the entry was leased"),
+                ));
                 false
             }
             _ => true,
         }
     });
-    before - state.entries.len()
+    finished
 }
 
 fn stats_from_state(state: &DispatchQueueState) -> QueueStats {
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --all-features`

Expected: all pass.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: queue-history.jsonl for finished entries, written before queue.json"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 7: Old-style calls: v0.2.9 parity and difference 4

Brings the old-style methods to v0.2.9 behaviour and fences them off from ticketed entries.

**v0.2.9 behaviour:**

- **`list`:** default 500, maximum 2000, minimum 1.
- **Old-style `lease`:**
  - Hands out due waiting entries and expired old-style leases (`queue.ts:561-585`).
  - Skips subjects in `exclude_subjects`, and hands out at most one entry per subject per call.
  - Keeps an entry's existing workflow id, otherwise gives the i-th chosen entry the i-th proposed id.
  - Sets `lease_owner` to the workflow id and the expiry to now plus the ticket length.
- **`mark_assigned`:** always replaces the workflow id (`queue.ts:702`).
- **`release_pending`:** clears the lease.
- **Old-style `enqueue`:** a `run_at` it can't read is treated as "now".

**Difference 4:** old-style `lease` skips ticketed entries. `mark_assigned` refuses them with `-32209`. `release_pending` refuses them with `-32209` too, before checking the status. `hold`, `release`, `drop`, `reorder` and `completion` still work on every entry.

**Files:**

- Modify: `src/dispatch_queue_state.rs`
- Modify: `src/plugin.rs`
- Modify: `src/queue_service.rs`
- Modify: `tests/common/mod.rs`
- Create: `tests/old_style.rs`

**Interfaces:**

- Consumes: `dispatch_legacy_key`, `dispatch_task_id` (Task 2); `DispatchQueueEntry::is_ticketed` (Task 5); `lease_ttl_secs` (Task 4).
- Produces (`src/queue_service.rs`):
  - `pub enum QueueMutationError { NotPending { entry_id }, Fenced { entry_id }, Backend(anyhow::Error) }`, returned by `hold`, `release` and `mark_assigned`
  - `QueueReleasePendingError::Fenced { entry_id }`
- Produces (`src/plugin.rs`): `fn mutation_error_response(id, QueueMutationError, method) -> RpcResponse`. `NotPending` becomes `-32203` and `Fenced` becomes `-32209`.
- Produces (`tests/common/mod.rs`): `edit_state(project_root, impl FnOnce(&mut DispatchQueueState))`, `entry_mut(&mut state, entry_id) -> &mut DispatchQueueEntry`, `read_entry(project_root, entry_id) -> DispatchQueueEntry` and `expire_lease(project_root, entry_id)`.

- [ ] **Step 1: Write the failing tests**

Apply to `tests/common/mod.rs` (`git apply` accepts this patch):

```diff
diff --git a/tests/common/mod.rs b/tests/common/mod.rs
index a011865..e3e73c9 100644
--- a/tests/common/mod.rs
+++ b/tests/common/mod.rs
@@ -5,10 +5,54 @@ use std::io::{BufRead, BufReader, Write};
 use std::path::Path;
 use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
 
+use animus_queue_default::{
+    load_queue_state, save_queue_state, DispatchQueueEntry, DispatchQueueState,
+};
 use animus_subject_protocol::{SubjectDispatch, SubjectRef};
 use chrono::Utc;
 use serde_json::{json, Value};
 
+/// Load the queue file, apply `edit`, and save it. For setting up states the
+/// public API can't reach directly (expired leases, old files).
+pub fn edit_state(project_root: &Path, edit: impl FnOnce(&mut DispatchQueueState)) {
+    let mut state = load_queue_state(project_root)
+        .expect("load queue state")
+        .unwrap_or_default();
+    edit(&mut state);
+    save_queue_state(project_root, &state).expect("save queue state");
+}
+
+/// The live entry with `entry_id`.
+pub fn entry_mut<'a>(
+    state: &'a mut DispatchQueueState,
+    entry_id: &str,
+) -> &'a mut DispatchQueueEntry {
+    state
+        .entries
+        .iter_mut()
+        .find(|entry| entry.entry_id == entry_id)
+        .unwrap_or_else(|| panic!("no live entry {entry_id}"))
+}
+
+/// Read one live entry from the queue file.
+pub fn read_entry(project_root: &Path, entry_id: &str) -> DispatchQueueEntry {
+    load_queue_state(project_root)
+        .expect("load queue state")
+        .expect("queue state")
+        .entries
+        .into_iter()
+        .find(|entry| entry.entry_id == entry_id)
+        .unwrap_or_else(|| panic!("no live entry {entry_id}"))
+}
+
+/// Move a lease's expiry into the past.
+pub fn expire_lease(project_root: &Path, entry_id: &str) {
+    edit_state(project_root, |state| {
+        entry_mut(state, entry_id).lease_expires_at =
+            Some(Utc::now() - chrono::Duration::seconds(60));
+    });
+}
+
 /// A task dispatch requested now.
 pub fn task_dispatch(task_id: &str, workflow_ref: &str) -> SubjectDispatch {
     SubjectDispatch::for_subject_with_metadata(
```

Create `tests/old_style.rs`:

```rust
//! Old-style `queue/*` calls behave like animus-postgres v0.2.9, except that
//! they leave ticketed entries alone (difference 4).

mod common;

use animus_queue_default::queue_service::{QueueMutationError, QueueReleasePendingError};
use animus_queue_default::{DispatchQueueEntry, DispatchQueueEntryStatus, QueueBackend};
use animus_subject_protocol::SubjectDispatch;
use chrono::Utc;
use common::{edit_state, entry_mut, expire_lease, read_entry, task_dispatch, PluginProcess};
use serde_json::json;

fn backend(temp: &tempfile::TempDir) -> QueueBackend {
    QueueBackend::new(temp.path().to_path_buf())
}

/// Give an old-style entry ticket identity, as a ticketed hand-out would.
fn make_ticketed(temp: &tempfile::TempDir, entry_id: &str, task_id: &str) {
    edit_state(temp.path(), |state| {
        let entry = entry_mut(state, entry_id);
        entry.subject_id = Some(format!("task:{task_id}"));
        entry.subject_generation = Some(1);
    });
}

#[test]
fn lease_keeps_an_existing_workflow_id_across_five_slots() {
    // Port of v0.2.9 "lease keeps one daemon-selected id through five-slot
    // assignment and expired reclaim".
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let ids: Vec<String> = (1..=5)
        .map(|n| {
            backend
                .enqueue(task_dispatch(&format!("TASK-{n}"), "standard"), None, None)
                .expect("enqueue")
                .entry_id
        })
        .collect();
    edit_state(temp.path(), |state| {
        let third = entry_mut(state, &ids[2]);
        third.status = DispatchQueueEntryStatus::Assigned;
        third.workflow_id = Some("workflow-before-restart".to_string());
        third.lease_owner = Some("workflow-before-restart".to_string());
        third.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(60));
    });
    let proposed: Vec<String> = (1..=5).map(|n| format!("daemon-workflow-{n}")).collect();

    let leased = backend.lease(5, Some(proposed), None).expect("lease");

    let workflow_ids: Vec<&str> = leased
        .leased
        .iter()
        .map(|entry| entry.workflow_id.as_deref().unwrap())
        .collect();
    assert_eq!(
        workflow_ids,
        [
            "daemon-workflow-1",
            "daemon-workflow-2",
            "workflow-before-restart",
            "daemon-workflow-4",
            "daemon-workflow-5"
        ]
    );
}

#[test]
fn expired_old_style_lease_is_handed_out_again_unless_excluded() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    backend
        .lease(1, Some(vec!["wf-1".to_string()]), None)
        .expect("lease");
    expire_lease(temp.path(), &enqueued.entry_id);

    let excluded = backend
        .lease(1, None, Some(vec!["TASK-1".to_string()]))
        .expect("lease with exclude");
    assert!(excluded.leased.is_empty());

    let reclaimed = backend.lease(1, None, None).expect("lease again");
    assert_eq!(reclaimed.leased.len(), 1);
    assert_eq!(reclaimed.leased[0].entry_id, enqueued.entry_id);
    assert_eq!(reclaimed.leased[0].workflow_id.as_deref(), Some("wf-1"));
}

#[test]
fn live_old_style_lease_is_not_handed_out_again() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    backend.lease(1, None, None).expect("lease");

    assert!(backend
        .lease(1, None, None)
        .expect("lease again")
        .leased
        .is_empty());
}

#[test]
fn lease_hands_out_one_entry_per_subject_per_call() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    for _ in 0..2 {
        backend
            .enqueue(task_dispatch("TASK-1", "standard"), None, None)
            .expect("enqueue");
    }

    let leased = backend.lease(5, None, None).expect("lease");

    assert_eq!(leased.leased.len(), 1);
    let stats = backend.stats().expect("stats");
    assert_eq!((stats.pending, stats.assigned), (1, 1));
}

#[test]
fn lease_sets_the_owner_and_an_expiry_from_the_ttl() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp).with_lease_ttl(120);
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    let before = Utc::now();

    backend
        .lease(1, Some(vec!["wf-1".to_string()]), None)
        .expect("lease");

    let entry = read_entry(temp.path(), &enqueued.entry_id);
    assert_eq!(entry.lease_owner.as_deref(), Some("wf-1"));
    let expires_at = entry.lease_expires_at.expect("expiry");
    assert!(expires_at >= before + chrono::Duration::seconds(120));
    assert!(expires_at <= Utc::now() + chrono::Duration::seconds(120));
}

#[test]
fn mark_assigned_replaces_an_old_workflow_id() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    edit_state(temp.path(), |state| {
        entry_mut(state, &enqueued.entry_id).workflow_id = Some("stale-id".to_string());
    });

    assert!(
        backend
            .mark_assigned(&enqueued.entry_id, None)
            .expect("mark")
            .changed
    );

    let entry = read_entry(temp.path(), &enqueued.entry_id);
    let workflow_id = entry.workflow_id.clone().expect("workflow id");
    assert_ne!(workflow_id, "stale-id");
    assert_eq!(
        workflow_id.len(),
        36,
        "fresh UUID expected, got {workflow_id}"
    );
    assert_eq!(entry.lease_owner, entry.workflow_id);
    assert!(entry.lease_expires_at.is_some());
}

#[test]
fn release_pending_clears_the_lease() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let enqueued = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    backend.lease(1, None, None).expect("lease");

    backend
        .release_pending(&enqueued.entry_id, "operator-cancel")
        .expect("release_pending");

    let entry = read_entry(temp.path(), &enqueued.entry_id);
    assert_eq!(entry.status, DispatchQueueEntryStatus::Pending);
    assert!(entry.workflow_id.is_none());
    assert!(entry.lease_owner.is_none());
    assert!(entry.lease_expires_at.is_none());
}

#[test]
fn unreadable_run_at_dispatches_now() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    backend
        .enqueue(
            task_dispatch("TASK-1", "standard"),
            Some("tomorrow morning".to_string()),
            None,
        )
        .expect("enqueue");

    let listed = backend.list(&[], None, None).expect("list");
    assert!(listed.entries[0].run_at.is_none());
    assert_eq!(backend.lease(1, None, None).expect("lease").leased.len(), 1);
}

#[test]
fn list_pages_default_to_500_and_cap_at_2000() {
    let temp = tempfile::tempdir().expect("tempdir");
    edit_state(temp.path(), |state| {
        for n in 0..2001 {
            state.entries.push(DispatchQueueEntry::from_dispatch(
                task_dispatch(&format!("TASK-{n}"), "standard"),
                None,
                None,
            ));
        }
    });
    let backend = backend(&temp);

    let default_page = backend.list(&[], None, None).expect("list");
    assert_eq!(default_page.entries.len(), 500);
    assert_eq!(default_page.total, 2001);
    assert_eq!(
        backend
            .list(&[], Some(5000), None)
            .expect("list")
            .entries
            .len(),
        2000
    );
    assert_eq!(
        backend
            .list(&[], Some(0), None)
            .expect("list")
            .entries
            .len(),
        1
    );
}

#[test]
fn generic_task_kind_reports_a_task_id() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let dispatch: SubjectDispatch = serde_json::from_value(json!({
        "subject": { "kind": "task", "id": "TASK-7" },
        "workflow_ref": "standard",
        "trigger_source": "test",
        "requested_at": "2026-09-28T00:00:00Z"
    }))
    .expect("dispatch");

    let enqueued = backend.enqueue(dispatch, None, None).expect("enqueue");

    assert_eq!(enqueued.subject_id, "TASK-7");
    let listed = backend.list(&[], None, None).expect("list");
    assert_eq!(listed.entries[0].subject_id, "TASK-7");
    assert_eq!(listed.entries[0].task_id.as_deref(), Some("TASK-7"));
}

#[test]
fn old_style_hand_out_mark_and_put_back_leave_ticketed_entries_alone() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let waiting = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue 1");
    let running = backend
        .enqueue(task_dispatch("TASK-2", "standard"), None, None)
        .expect("enqueue 2");
    make_ticketed(&temp, &waiting.entry_id, "TASK-1");
    make_ticketed(&temp, &running.entry_id, "TASK-2");
    edit_state(temp.path(), |state| {
        let entry = entry_mut(state, &running.entry_id);
        entry.status = DispatchQueueEntryStatus::Assigned;
        entry.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(60));
    });

    // Neither the waiting entry nor the expired running one is handed out.
    assert!(backend
        .lease(5, None, None)
        .expect("lease")
        .leased
        .is_empty());
    assert!(matches!(
        backend.mark_assigned(&waiting.entry_id, None),
        Err(QueueMutationError::Fenced { .. })
    ));
    assert!(matches!(
        backend.release_pending(&running.entry_id, "operator"),
        Err(QueueReleasePendingError::Fenced { .. })
    ));
    assert_eq!(
        read_entry(temp.path(), &running.entry_id).status,
        DispatchQueueEntryStatus::Assigned
    );
}

#[test]
fn hold_release_drop_and_completion_still_work_on_ticketed_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let backend = backend(&temp);
    let waiting = backend
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue 1");
    let running = backend
        .enqueue(task_dispatch("TASK-2", "standard"), None, None)
        .expect("enqueue 2");
    make_ticketed(&temp, &waiting.entry_id, "TASK-1");
    make_ticketed(&temp, &running.entry_id, "TASK-2");
    edit_state(temp.path(), |state| {
        let entry = entry_mut(state, &running.entry_id);
        entry.status = DispatchQueueEntryStatus::Assigned;
        entry.workflow_id = Some("wf-2".to_string());
    });

    assert!(backend.hold(&waiting.entry_id).expect("hold").changed);
    assert!(backend.release(&waiting.entry_id).expect("release").changed);
    // v0.2.9: old-style "done" is accepted for any entry (spec §7.3).
    assert!(
        backend
            .completion(&running.entry_id, "completed", None, None)
            .expect("completion")
            .changed
    );
    assert!(backend.drop_entry(&waiting.entry_id).expect("drop").changed);
    assert_eq!(backend.stats().expect("stats").total, 0);
}

#[test]
fn ticketed_refusal_uses_the_stale_fence_error_code() {
    let temp = tempfile::tempdir().expect("tempdir");
    let waiting = backend(&temp)
        .enqueue(task_dispatch("TASK-1", "standard"), None, None)
        .expect("enqueue");
    make_ticketed(&temp, &waiting.entry_id, "TASK-1");
    let mut plugin = PluginProcess::spawn(&[]);
    plugin.initialize(temp.path(), "1.2.0");

    let refused = plugin.request(
        "queue/mark_assigned",
        json!({ "entry_id": waiting.entry_id }),
    );

    assert_eq!(refused["error"]["code"], -32209);
    assert!(refused["error"]["message"]
        .as_str()
        .unwrap()
        .contains("use queue/v2/*"));
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test old_style`

Expected: most tests fail: expired leases aren't reclaimed, `list` isn't capped, ticketed entries are leased.

- [ ] **Step 3: Implement**

Apply to `src/dispatch_queue_state.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/dispatch_queue_state.rs b/src/dispatch_queue_state.rs
index b728afe..89ecc64 100644
--- a/src/dispatch_queue_state.rs
+++ b/src/dispatch_queue_state.rs
@@ -15,6 +15,8 @@ use animus_subject_protocol::SubjectDispatch;
 use chrono::{DateTime, Utc};
 use serde::{Deserialize, Serialize};
 
+use crate::identity::{dispatch_legacy_key, dispatch_task_id};
+
 /// Format version written to `queue.json`. Files without the marker (queue
 /// v0.3.3 and older) load as version 0.
 pub const QUEUE_FORMAT_VERSION: u32 = 2;
@@ -183,8 +185,8 @@ impl DispatchQueueEntry {
     ) -> Self {
         Self {
             entry_id: uuid::Uuid::new_v4().to_string(),
-            subject_id: dispatch.subject_key(),
-            task_id: dispatch.task_id().unwrap_or_default().to_string(),
+            subject_id: dispatch_legacy_key(&dispatch),
+            task_id: dispatch_task_id(&dispatch).unwrap_or_default().to_string(),
             dispatch: Some(dispatch),
             status: DispatchQueueEntryStatus::Pending,
             enqueued_at: Some(chrono::Utc::now().to_rfc3339()),
@@ -290,12 +292,13 @@ impl DispatchQueueEntry {
         self.task_id.as_str()
     }
 
-    /// Effective task id (None when this entry's subject is not a built-in
-    /// task).
+    /// Effective task id: the subject id when the subject's kind is
+    /// `animus.task` or `task` (v0.2.9 `entryView`), else the stored
+    /// `task_id` when non-empty.
     pub fn task_id_ref(&self) -> Option<&str> {
         self.dispatch
             .as_ref()
-            .and_then(SubjectDispatch::task_id)
+            .and_then(dispatch_task_id)
             .or_else(|| (!self.task_id.trim().is_empty()).then_some(self.task_id.as_str()))
     }
 }
```

Apply to `src/plugin.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/plugin.rs b/src/plugin.rs
index c49a689..ba11ae2 100644
--- a/src/plugin.rs
+++ b/src/plugin.rs
@@ -29,7 +29,7 @@ use tokio::sync::{Mutex, RwLock};
 use crate::host_guard::check_host_protocol;
 use crate::lease_ttl::{lease_ttl_from_env, LEASE_TTL_ENV};
 use crate::queue_service::{
-    QueueBackend, QueueCallError, QueueLeaseError, QueueReleasePendingError,
+    QueueBackend, QueueCallError, QueueLeaseError, QueueMutationError, QueueReleasePendingError,
 };
 
 const PLUGIN_NAME: &str = "animus-queue-default";
@@ -526,7 +526,7 @@ async fn handle_hold(
     };
     match backend.hold(&request.entry_id) {
         Ok(response) => to_value_response(id, &response),
-        Err(error) => not_pending_or_internal(id, &error, "queue/hold"),
+        Err(error) => mutation_error_response(id, error, "queue/hold"),
     }
 }
 
@@ -545,7 +545,7 @@ async fn handle_release(
     };
     match backend.release(&request.entry_id) {
         Ok(response) => to_value_response(id, &response),
-        Err(error) => not_pending_or_internal(id, &error, "queue/release"),
+        Err(error) => mutation_error_response(id, error, "queue/release"),
     }
 }
 
@@ -577,6 +577,14 @@ async fn handle_release_pending(
                 data: None,
             },
         ),
+        Err(error @ QueueReleasePendingError::Fenced { .. }) => RpcResponse::err(
+            id,
+            RpcError {
+                code: queue_error_codes::QUEUE_STALE_FENCE,
+                message: error.to_string(),
+                data: None,
+            },
+        ),
         Err(QueueReleasePendingError::NotAssigned {
             entry_id,
             actual_state,
@@ -657,7 +665,7 @@ async fn handle_mark_assigned(
         };
     match backend.mark_assigned(&request.entry_id, request.workflow_id) {
         Ok(response) => to_value_response(id, &response),
-        Err(error) => not_pending_or_internal(id, &error, "queue/mark_assigned"),
+        Err(error) => mutation_error_response(id, error, "queue/mark_assigned"),
     }
 }
 
@@ -742,19 +750,26 @@ fn to_value_response<T: serde::Serialize>(id: Option<Value>, value: &T) -> RpcRe
     }
 }
 
-fn not_pending_or_internal(id: Option<Value>, error: &anyhow::Error, method: &str) -> RpcResponse {
-    let msg = error.to_string();
-    if msg.contains("not in the expected pre-mutation status") {
-        return RpcResponse::err(
-            id,
-            RpcError {
-                code: queue_error_codes::QUEUE_ENTRY_NOT_PENDING,
-                message: msg,
-                data: None,
-            },
-        );
-    }
-    internal_error_response(id, format!("{method} failed: {error:#}"))
+fn mutation_error_response(
+    id: Option<Value>,
+    error: QueueMutationError,
+    method: &str,
+) -> RpcResponse {
+    let code = match &error {
+        QueueMutationError::NotPending { .. } => queue_error_codes::QUEUE_ENTRY_NOT_PENDING,
+        QueueMutationError::Fenced { .. } => queue_error_codes::QUEUE_STALE_FENCE,
+        QueueMutationError::Backend(error) => {
+            return internal_error_response(id, format!("{method} failed: {error:#}"));
+        }
+    };
+    RpcResponse::err(
+        id,
+        RpcError {
+            code,
+            message: error.to_string(),
+            data: None,
+        },
+    )
 }
 
 fn call_error_response(id: Option<Value>, error: QueueCallError, method: &str) -> RpcResponse {
```

Apply to `src/queue_service.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/queue_service.rs b/src/queue_service.rs
index 89d3169..24b5e86 100644
--- a/src/queue_service.rs
+++ b/src/queue_service.rs
@@ -4,6 +4,7 @@
 //! mutations from the in-tree code were replaced as part of the v0.5
 //! plugin extraction (see `docs/architecture/v0.5-protocol-specs.md` §2).
 
+use std::collections::HashSet;
 use std::path::{Path, PathBuf};
 
 use animus_queue_protocol::{
@@ -18,9 +19,15 @@ use crate::dispatch_queue_state::{
     DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
 };
 use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state, save_queue_state};
+use crate::identity::{dispatch_legacy_key, MISSING_SUBJECT_IDENTITY};
 use crate::lease_ttl::DEFAULT_LEASE_TTL_SECS;
 use crate::queue_history::{append_history, HistoryOutcome, HistoryRecord};
 
+/// `queue/list` page size when the caller gives none (v0.2.9).
+const DEFAULT_LIST_LIMIT: usize = 500;
+/// Largest `queue/list` page (v0.2.9).
+const MAX_LIST_LIMIT: usize = 2000;
+
 /// File-locked backend wrapping a single project root's queue state.
 #[derive(Debug, Clone)]
 pub struct QueueBackend {
@@ -90,11 +97,22 @@ impl QueueBackend {
         // A subjectless entry would make the whole file unreadable for queue
         // v0.3.3 (its dispatch type requires a subject), which breaks the
         // documented rollback path. Reject it before touching state.
-        let Some(subject_id) = dispatch.subject_key() else {
+        let Some(subject_id) = dispatch_legacy_key(&dispatch) else {
             return Err(QueueCallError::InvalidParams(
-                "queue subject identity is missing".to_string(),
+                MISSING_SUBJECT_IDENTITY.to_string(),
             ));
         };
+        // v0.2.9 `parseRunAt`: an unreadable run_at means "dispatch now".
+        let run_at = run_at.filter(|raw| {
+            let readable = chrono::DateTime::parse_from_rfc3339(raw).is_ok();
+            if !readable {
+                tracing::warn!(
+                    run_at = raw.as_str(),
+                    "queue/enqueue: ignoring unparseable run_at; the entry dispatches now"
+                );
+            }
+            readable
+        });
 
         let _lock = acquire_queue_lock(&self.project_root)?;
         let mut state = load_queue_state(&self.project_root)?.unwrap_or_default();
@@ -176,9 +194,7 @@ impl QueueBackend {
         } else {
             filtered.drain(0..offset);
         }
-        if let Some(limit) = limit {
-            filtered.truncate(limit);
-        }
+        filtered.truncate(limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT));
 
         let entries: Vec<QueueEntry> = filtered.into_iter().filter_map(entry_to_protocol).collect();
         Ok(QueueListResponse {
@@ -227,21 +243,21 @@ impl QueueBackend {
     // queue/lease (NEW atomic dispatch path)
     // ============================================================
 
-    /// Atomic dispatch: claim up to `max` pending entries, attach
-    /// workflow ids, transition each Pending → Assigned, persist, and
-    /// return the leased entries.
+    /// Old-style atomic dispatch: claim up to `max` entries, attach workflow
+    /// ids, transition each to Assigned with a lease expiry, persist, and
+    /// return them.
     ///
-    /// If `workflow_ids` is `Some` its length MUST equal `max` (return
-    /// [`QueueLeaseError::WorkflowIdCountMismatch`]). When `None`, synthetic
-    /// UUIDs are generated.
+    /// Follows animus-postgres v0.2.9's `lease`, except that ticketed entries
+    /// are never touched (difference 4):
     ///
-    /// If `exclude_subjects` is `Some`, pending entries whose
-    /// `subject_dispatch.subject_key()` matches any id in the list are
-    /// skipped over without state transition. Daemons pass the set of
-    /// subjects that already have in-flight workflows so the queue can
-    /// advance past a head-of-line entry instead of returning it for
-    /// immediate `queue/release_pending` back to Pending. Backward-
-    /// compatible: `None` matches v0.2.0 behavior.
+    /// - Candidates are due Pending entries and old-style Assigned entries
+    ///   whose lease expired; an expired entry is handed out again.
+    /// - A subject is handed out at most once per call. `exclude_subjects`
+    ///   (legacy subject keys) adds subjects to skip.
+    /// - An entry keeps a workflow id it already has; otherwise the i-th
+    ///   chosen entry gets `workflow_ids[i]`, or a fresh UUID when the caller
+    ///   sent none. If `workflow_ids` is `Some`, its length MUST equal `max`
+    ///   ([`QueueLeaseError::WorkflowIdCountMismatch`]).
     pub fn lease(
         &self,
         max: usize,
@@ -269,23 +285,23 @@ impl QueueBackend {
         // Drop deferred entries that blew past their expiry window while the
         // daemon was unavailable, instead of dispatching them late.
         let finished = sweep_expired_entries(&mut state, now);
-        let now_rfc3339 = now.to_rfc3339();
-        let mut leased: Vec<QueueEntry> = Vec::new();
-        let mut assigned_index = 0usize;
-        let mut exclude_set: Option<std::collections::HashSet<String>> =
-            exclude_subjects.map(|ids| ids.into_iter().collect());
-
-        // FIFO within Pending — first-eligible-wins, in current order.
-        for entry in state.entries.iter_mut() {
-            if leased.len() == max {
+        let mut exclude: HashSet<String> =
+            exclude_subjects.unwrap_or_default().into_iter().collect();
+        let mut chosen: Vec<usize> = Vec::new();
+        for (index, entry) in state.entries.iter().enumerate() {
+            if chosen.len() == max {
                 break;
             }
-            if entry.status != DispatchQueueEntryStatus::Pending {
+            if entry.is_ticketed() {
                 continue;
             }
-            // Deferred entry whose run_at has not yet arrived — leave Pending,
-            // not leasable until the instant passes.
-            if entry.is_deferred_until_future(now) {
+            let due = entry.status == DispatchQueueEntryStatus::Pending
+                && !entry.is_deferred_until_future(now);
+            let expired = entry.status == DispatchQueueEntryStatus::Assigned
+                && entry
+                    .lease_expires_at
+                    .is_some_and(|expires_at| expires_at < now);
+            if !due && !expired {
                 continue;
             }
             // Corrupt legacy state — an entry with no dispatch envelope can't
@@ -294,36 +310,33 @@ impl QueueBackend {
             if entry.dispatch.is_none() {
                 tracing::warn!(
                     entry_id = %entry.entry_id,
-                    "queue/lease: skipping pending entry with no SubjectDispatch envelope"
+                    "queue/lease: skipping entry with no SubjectDispatch envelope"
                 );
                 continue;
             }
-            if let Some(set) = exclude_set.as_mut() {
-                // Prefer the dispatch's canonical subject_key (matches the
-                // host's active-subject tracking); fall back to the stored
-                // subject_id for entries that migrated without a dispatch.
-                let key_owned = entry
-                    .dispatch
-                    .as_ref()
-                    .and_then(SubjectDispatch::subject_key)
-                    .unwrap_or_else(|| entry.subject_id_ref().to_string());
-                if set.contains(&key_owned) {
-                    continue;
-                }
-                // Leasing this entry makes its subject in-flight for the rest
-                // of the batch — otherwise two pending entries for the same
-                // subject can be leased together, defeating the exclusivity
-                // the caller asked for via `exclude_subjects`.
-                set.insert(key_owned);
+            if !exclude.insert(entry.subject_id_ref().to_string()) {
+                continue;
             }
-            let workflow_id = match workflow_ids.as_ref() {
-                Some(ids) => ids[assigned_index].clone(),
-                None => uuid::Uuid::new_v4().to_string(),
-            };
-            assigned_index += 1;
+            chosen.push(index);
+        }
 
+        let expires_at = now + chrono::Duration::seconds(self.lease_ttl_secs);
+        let now_rfc3339 = now.to_rfc3339();
+        let mut leased: Vec<QueueEntry> = Vec::with_capacity(chosen.len());
+        for (slot, index) in chosen.into_iter().enumerate() {
+            let entry = &mut state.entries[index];
+            // v0.2.9: keep a lease's workflow id across expiry and reclaim.
+            // The daemon may already have created that workflow.
+            let workflow_id = entry
+                .workflow_id
+                .clone()
+                .filter(|id| !id.is_empty())
+                .or_else(|| workflow_ids.as_ref().map(|ids| ids[slot].clone()))
+                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
             entry.status = DispatchQueueEntryStatus::Assigned;
-            entry.workflow_id = Some(workflow_id);
+            entry.workflow_id = Some(workflow_id.clone());
+            entry.lease_owner = Some(workflow_id);
+            entry.lease_expires_at = Some(expires_at);
             entry.assigned_at = Some(now_rfc3339.clone());
             if let Some(protocol_entry) = entry_to_protocol(entry) {
                 leased.push(protocol_entry);
@@ -342,7 +355,10 @@ impl QueueBackend {
     // ============================================================
 
     /// Hold a Pending entry. Idempotent on already-held.
-    pub fn hold(&self, entry_id: &str) -> Result<QueueMutationResponse> {
+    pub fn hold(
+        &self,
+        entry_id: &str,
+    ) -> std::result::Result<QueueMutationResponse, QueueMutationError> {
         self.mutate_entry(entry_id, |entry| {
             match entry.status {
                 DispatchQueueEntryStatus::Held => Ok(false), // idempotent no-op
@@ -358,7 +374,10 @@ impl QueueBackend {
     }
 
     /// Release a Held entry back to Pending. Idempotent on already-pending.
-    pub fn release(&self, entry_id: &str) -> Result<QueueMutationResponse> {
+    pub fn release(
+        &self,
+        entry_id: &str,
+    ) -> std::result::Result<QueueMutationResponse, QueueMutationError> {
         self.mutate_entry(entry_id, |entry| match entry.status {
             DispatchQueueEntryStatus::Pending => Ok(false),
             DispatchQueueEntryStatus::Held => {
@@ -463,27 +482,36 @@ impl QueueBackend {
     // queue/mark_assigned + queue/completion
     // ============================================================
 
-    /// Transition a single Pending entry to Assigned. Used by callers that
-    /// prefer list+mark over atomic [`Self::lease`].
+    /// Transition a single old-style Pending entry to Assigned (v0.2.9
+    /// `markAssigned`). The entry gets `workflow_id`, or a fresh UUID, even
+    /// if it had an id before, plus a lease expiry. Ticketed entries are
+    /// refused with [`QueueMutationError::Fenced`] (difference 4).
     pub fn mark_assigned(
         &self,
         entry_id: &str,
         workflow_id: Option<String>,
-    ) -> Result<QueueMutationResponse> {
-        self.mutate_entry(entry_id, |entry| match entry.status {
-            DispatchQueueEntryStatus::Assigned => Ok(false),
-            DispatchQueueEntryStatus::Pending => {
-                entry.status = DispatchQueueEntryStatus::Assigned;
-                if let Some(wid) = workflow_id {
-                    entry.workflow_id = Some(wid);
-                } else if entry.workflow_id.is_none() {
-                    entry.workflow_id = Some(uuid::Uuid::new_v4().to_string());
+    ) -> std::result::Result<QueueMutationResponse, QueueMutationError> {
+        let now = Utc::now();
+        let expires_at = now + chrono::Duration::seconds(self.lease_ttl_secs);
+        self.mutate_entry(entry_id, |entry| {
+            if entry.is_ticketed() {
+                return Err(MutationError::Fenced);
+            }
+            match entry.status {
+                DispatchQueueEntryStatus::Assigned => Ok(false),
+                DispatchQueueEntryStatus::Pending => {
+                    let workflow_id =
+                        workflow_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
+                    entry.status = DispatchQueueEntryStatus::Assigned;
+                    entry.workflow_id = Some(workflow_id.clone());
+                    entry.lease_owner = Some(workflow_id);
+                    entry.lease_expires_at = Some(expires_at);
+                    entry.assigned_at = Some(now.to_rfc3339());
+                    Ok(true)
                 }
-                entry.assigned_at = Some(Utc::now().to_rfc3339());
-                Ok(true)
+                DispatchQueueEntryStatus::Held => Err(MutationError::NotPending),
+                DispatchQueueEntryStatus::Unknown => Err(MutationError::NotPending),
             }
-            DispatchQueueEntryStatus::Held => Err(MutationError::NotPending),
-            DispatchQueueEntryStatus::Unknown => Err(MutationError::NotPending),
         })
     }
 
@@ -547,11 +575,13 @@ impl QueueBackend {
     // queue/release_pending
     // ============================================================
 
-    /// Atomically return an Assigned entry to Pending. Clears the workflow
-    /// lease fields and appends an audit entry describing why.
+    /// Atomically return an old-style Assigned entry to Pending. Clears the
+    /// workflow and lease fields and appends an audit entry describing why.
     ///
     /// Errors:
     /// - [`QueueReleasePendingError::NotFound`] when `entry_id` is unknown.
+    /// - [`QueueReleasePendingError::Fenced`] when the entry is ticketed
+    ///   (difference 4; use `queue/v2/release_pending`).
     /// - [`QueueReleasePendingError::NotAssigned`] when the entry exists but
     ///   is in a state other than Assigned. The error carries the entry's
     ///   actual wire status so callers can surface it as the `-32208`
@@ -577,6 +607,11 @@ impl QueueBackend {
                 entry_id: entry_id.to_string(),
             })?;
 
+        if entry.is_ticketed() {
+            return Err(QueueReleasePendingError::Fenced {
+                entry_id: entry_id.to_string(),
+            });
+        }
         if entry.status != DispatchQueueEntryStatus::Assigned {
             return Err(QueueReleasePendingError::NotAssigned {
                 entry_id: entry_id.to_string(),
@@ -589,9 +624,12 @@ impl QueueBackend {
         entry.status = DispatchQueueEntryStatus::Pending;
         entry.assigned_at = None;
         entry.workflow_id = None;
-        // TODO(codex-p2): fence late completions from the released workflow so
-        // they cannot prune the replacement lease on entry id reuse. Requires
-        // touching the completion path (see queue_service.rs completion()).
+        entry.lease_owner = None;
+        entry.lease_expires_at = None;
+        // A late old-style completion from the released workflow can still
+        // finish the entry's next run. That is v0.2.9's behaviour and an
+        // accepted risk for old-style calls (spec §7.3); ticketed work uses
+        // queue/v2/*, which is fenced.
         entry.audit_log.push(DispatchQueueAuditEntry {
             at: now,
             method: "queue/release_pending".to_string(),
@@ -623,7 +661,11 @@ impl QueueBackend {
         save_queue_state(&self.project_root, state)
     }
 
-    fn mutate_entry<F>(&self, entry_id: &str, mutate: F) -> Result<QueueMutationResponse>
+    fn mutate_entry<F>(
+        &self,
+        entry_id: &str,
+        mutate: F,
+    ) -> std::result::Result<QueueMutationResponse, QueueMutationError>
     where
         F: FnOnce(&mut DispatchQueueEntry) -> std::result::Result<bool, MutationError>,
     {
@@ -649,9 +691,14 @@ impl QueueBackend {
         let changed = match mutate(entry) {
             Ok(changed) => changed,
             Err(MutationError::NotPending) => {
-                return Err(anyhow::anyhow!(
-                    "queue entry {entry_id} is not in the expected pre-mutation status"
-                ));
+                return Err(QueueMutationError::NotPending {
+                    entry_id: entry_id.to_string(),
+                });
+            }
+            Err(MutationError::Fenced) => {
+                return Err(QueueMutationError::Fenced {
+                    entry_id: entry_id.to_string(),
+                });
             }
         };
 
@@ -665,11 +712,33 @@ impl QueueBackend {
     }
 }
 
-/// Internal mutation error surfaced to RPC handlers as
-/// `QUEUE_ENTRY_NOT_PENDING`.
+/// Internal outcome of a [`QueueBackend::mutate_entry`] closure.
 #[derive(Debug)]
 enum MutationError {
     NotPending,
+    Fenced,
+}
+
+/// Typed errors for `queue/hold`, `queue/release` and `queue/mark_assigned`.
+#[derive(Debug, thiserror::Error)]
+pub enum QueueMutationError {
+    /// The entry is not in the status the call expects. Surfaced as
+    /// [`animus_queue_protocol::error_codes::QUEUE_ENTRY_NOT_PENDING`].
+    #[error("queue entry {entry_id} is not in the expected pre-mutation status")]
+    NotPending {
+        /// Entry id from the request.
+        entry_id: String,
+    },
+    /// The entry is ticketed; old-style calls may not change it (difference
+    /// 4). Surfaced as [`animus_queue_protocol::error_codes::QUEUE_STALE_FENCE`].
+    #[error("queue entry {entry_id} is owned by a generation-fenced lease; use queue/v2/*")]
+    Fenced {
+        /// Entry id from the request.
+        entry_id: String,
+    },
+    /// Wrapped backend error (I/O, lock acquisition, persistence).
+    #[error(transparent)]
+    Backend(#[from] anyhow::Error),
 }
 
 /// Errors from calls that validate caller input before touching state.
@@ -710,6 +779,13 @@ pub enum QueueReleasePendingError {
         /// Entry id from the request.
         entry_id: String,
     },
+    /// The entry is ticketed; old-style calls may not change it (difference
+    /// 4). Surfaced as [`animus_queue_protocol::error_codes::QUEUE_STALE_FENCE`].
+    #[error("queue entry {entry_id} is owned by a generation-fenced lease; use queue/v2/*")]
+    Fenced {
+        /// Entry id from the request.
+        entry_id: String,
+    },
     /// Entry exists but is not in the Assigned state. Surfaced as
     /// [`animus_queue_protocol::error_codes::QUEUE_ENTRY_NOT_ASSIGNED`]
     /// with `data.actual_state` populated.
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --all-features`

Expected: all pass.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: old-style calls follow animus-postgres v0.2.9; leave ticketed entries alone"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 8: Ticketed add (`queue/v2/enqueue`)

v0.2.9's `enqueueV2` (`queue.ts:394-497`) plus differences 2, 3 and 7.

- **Validation:** protocol validation, then:
  - the id must normalise to a canonical `<kind>:<id>`
  - `run_at` must be RFC 3339 (difference 7)
  - the idempotency key is trimmed and at most 256 characters
  - repository fields are trimmed
- **Idempotency:** the same key with the same content returns the original receipt (`enqueued: false`), from the live file or else from the history. The same key with different content is invalid params.
- **Difference 3:** if the task already has a waiting, running or held entry, that entry comes back with the warning `subject <id> already has an active generation; enqueue rejected`. If it is an old-style entry, it gets ticket identity then.
- **Otherwise:** a new entry with the task's next generation. The next generation is one more than the higher of the stored counter and any live entry's generation, and it is recorded.

**Files:**

- Create: `src/fenced_queue.rs`
- Modify: `src/lib.rs`
- Modify: `tests/common/mod.rs`
- Create: `tests/fenced_enqueue.rs`

**Interfaces:**

- Consumes: `QueueCallError` (Task 1), identity and request hash (Task 2), history (Task 6), `sweep_expired_entries` and `commit` (Task 6).
- Produces (`src/fenced_queue.rs`, `impl QueueBackend`): `pub fn enqueue_v2(&self, request: QueueEnqueueV2Request) -> Result<QueueEnqueueV2Response, QueueCallError>`
- Produces (module-private in `src/fenced_queue.rs`, used again by Tasks 9 and 10):
  - `pub(crate) fn ensure_ticket_identity(state: &mut DispatchQueueState, index: usize) -> Result<SubjectGeneration, String>`
  - `fn next_subject_generation(state: &mut DispatchQueueState, qualified_id: &str) -> u64`
- Produces (`tests/common/mod.rs`): `reservation(task_id)` and `enqueue_request(task_id)`.

- [ ] **Step 1: Write the failing tests**

Apply to `tests/common/mod.rs` (`git apply` accepts this patch):

```diff
diff --git a/tests/common/mod.rs b/tests/common/mod.rs
index e3e73c9..7e59017 100644
--- a/tests/common/mod.rs
+++ b/tests/common/mod.rs
@@ -5,13 +5,36 @@ use std::io::{BufRead, BufReader, Write};
 use std::path::Path;
 use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
 
+use animus_execution_protocol::RepositoryReservation;
 use animus_queue_default::{
     load_queue_state, save_queue_state, DispatchQueueEntry, DispatchQueueState,
 };
+use animus_queue_protocol::QueueEnqueueV2Request;
 use animus_subject_protocol::{SubjectDispatch, SubjectRef};
 use chrono::Utc;
 use serde_json::{json, Value};
 
+/// The repository/branch reservation used for `task_id` in these tests.
+pub fn reservation(task_id: &str) -> RepositoryReservation {
+    RepositoryReservation {
+        repository: "https://github.com/launchapp-dev/animus-cli.git".to_string(),
+        base_ref: "refs/heads/main".to_string(),
+        head_ref: format!("refs/heads/animus/{task_id}"),
+    }
+}
+
+/// A ticketed add for `task_id`, shaped like the ones in
+/// animus-queue-postgres v0.2.0's tests.
+pub fn enqueue_request(task_id: &str) -> QueueEnqueueV2Request {
+    QueueEnqueueV2Request {
+        subject_dispatch: task_dispatch(task_id, "coding"),
+        idempotency_key: Some(format!("delivery-{task_id}")),
+        repository: Some(reservation(task_id)),
+        run_at: None,
+        expire_after_secs: None,
+    }
+}
+
 /// Load the queue file, apply `edit`, and save it. For setting up states the
 /// public API can't reach directly (expired leases, old files).
 pub fn edit_state(project_root: &Path, edit: impl FnOnce(&mut DispatchQueueState)) {
```

Create `tests/fenced_enqueue.rs`:

```rust
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
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test fenced_enqueue`

Expected: compile error: no method named `enqueue_v2` on `QueueBackend`.

- [ ] **Step 3: Implement**

Create `src/fenced_queue.rs`:

```rust
//! Generation-fenced ("ticketed") queue calls: `queue/v2/*`.
//!
//! Behaviour follows animus-postgres v0.2.9 (`src/queue.ts`) except for the
//! seven differences listed in the design spec (§7.2). Each one is marked
//! where it applies.

use animus_execution_protocol::{RepositoryReservation, SubjectGeneration};
use animus_queue_protocol::{QueueEnqueueV2Request, QueueEnqueueV2Response};
use chrono::Utc;

use crate::dispatch_queue_state::{
    DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
};
use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state};
use crate::identity::{dispatch_canonical_id, dispatch_task_id};
use crate::queue_history::find_history_by_idempotency_key;
use crate::queue_service::{sweep_expired_entries, QueueBackend, QueueCallError};
use crate::request_hash::enqueue_request_hash;

/// Longest accepted idempotency key after trimming (v0.2.9).
const MAX_IDEMPOTENCY_KEY_CHARS: usize = 256;

impl QueueBackend {
    /// `queue/v2/enqueue`: add a ticketed entry with the subject's next
    /// generation.
    ///
    /// - The same idempotency key with the same content returns the original
    ///   receipt (`enqueued: false`); with different content it is an error.
    ///   The content hash leaves out `dispatch.requested_at` (difference 2).
    /// - A task that already has a live entry gets that entry back, with a
    ///   warning, instead of a second one (difference 3).
    /// - A malformed `run_at` is an error (difference 7).
    pub fn enqueue_v2(
        &self,
        request: QueueEnqueueV2Request,
    ) -> Result<QueueEnqueueV2Response, QueueCallError> {
        request.validate().map_err(QueueCallError::InvalidParams)?;
        let QueueEnqueueV2Request {
            subject_dispatch: dispatch,
            idempotency_key,
            repository,
            run_at,
            expire_after_secs,
        } = request;
        let qualified_id =
            dispatch_canonical_id(&dispatch).map_err(QueueCallError::InvalidParams)?;
        if let Some(raw) = run_at.as_deref() {
            chrono::DateTime::parse_from_rfc3339(raw).map_err(|error| {
                QueueCallError::InvalidParams(format!("run_at must be RFC 3339 ({raw}): {error}"))
            })?;
        }
        let idempotency_key = idempotency_key.map(|key| key.trim().to_string());
        if idempotency_key
            .as_ref()
            .is_some_and(|key| key.chars().count() > MAX_IDEMPOTENCY_KEY_CHARS)
        {
            return Err(QueueCallError::InvalidParams(
                "idempotency_key must be a non-empty string of at most 256 characters".to_string(),
            ));
        }
        let repository = repository.map(|reservation| RepositoryReservation {
            repository: reservation.repository.trim().to_string(),
            base_ref: reservation.base_ref.trim().to_string(),
            head_ref: reservation.head_ref.trim().to_string(),
        });
        let request_hash = enqueue_request_hash(
            &dispatch,
            repository.as_ref(),
            run_at.as_deref(),
            expire_after_secs,
        );

        let _lock = acquire_queue_lock(self.project_root())?;
        let mut state = load_queue_state(self.project_root())?.unwrap_or_default();
        let finished = sweep_expired_entries(&mut state, Utc::now());

        if let Some(key) = idempotency_key.as_deref() {
            if let Some(receipt) = self.idempotent_receipt(&state, key, &request_hash)? {
                if !finished.is_empty() {
                    self.commit(&state, &finished)?;
                }
                return Ok(receipt);
            }
        }

        if let Some(index) = live_entry_for_subject(&state, &qualified_id) {
            let subject =
                ensure_ticket_identity(&mut state, index).map_err(QueueCallError::InvalidParams)?;
            let entry_id = state.entries[index].entry_id.clone();
            self.commit(&state, &finished)?;
            return Ok(QueueEnqueueV2Response {
                enqueued: false,
                entry_id,
                subject,
                warning: Some(format!(
                    "subject {qualified_id} already has an active generation; enqueue rejected"
                )),
            });
        }

        let generation = next_subject_generation(&mut state, &qualified_id);
        let entry = DispatchQueueEntry {
            entry_id: uuid::Uuid::new_v4().to_string(),
            subject_id: Some(qualified_id.clone()),
            task_id: dispatch_task_id(&dispatch).unwrap_or_default().to_string(),
            dispatch: Some(dispatch),
            status: DispatchQueueEntryStatus::Pending,
            enqueued_at: Some(Utc::now().to_rfc3339()),
            run_at,
            expire_after_secs,
            subject_generation: Some(generation),
            repository,
            idempotency_key,
            request_hash: Some(request_hash),
            ..DispatchQueueEntry::default()
        };
        let entry_id = entry.entry_id.clone();
        state.entries.push(entry);
        self.commit(&state, &finished)?;
        Ok(QueueEnqueueV2Response {
            enqueued: true,
            entry_id,
            subject: SubjectGeneration {
                qualified_id,
                generation,
            },
            warning: None,
        })
    }

    /// The original receipt for `key`, from the live file or, if the entry
    /// already finished, from the history. Errors when the key is bound to
    /// different content.
    fn idempotent_receipt(
        &self,
        state: &DispatchQueueState,
        key: &str,
        request_hash: &str,
    ) -> Result<Option<QueueEnqueueV2Response>, QueueCallError> {
        let live = state
            .entries
            .iter()
            .find(|entry| entry.idempotency_key.as_deref() == Some(key))
            .cloned();
        let found = match live {
            Some(entry) => Some(entry),
            None => find_history_by_idempotency_key(self.project_root(), key)?
                .map(|record| record.entry),
        };
        let Some(entry) = found else {
            return Ok(None);
        };
        if entry.request_hash.as_deref() != Some(request_hash) {
            return Err(QueueCallError::InvalidParams(
                "idempotency_key is already bound to a different queue request".to_string(),
            ));
        }
        let (Some(qualified_id), Some(generation)) =
            (entry.subject_id.clone(), entry.subject_generation)
        else {
            return Err(QueueCallError::Backend(anyhow::anyhow!(
                "queue entry {} holds idempotency key {key} but no subject generation",
                entry.entry_id
            )));
        };
        Ok(Some(QueueEnqueueV2Response {
            enqueued: false,
            entry_id: entry.entry_id,
            subject: SubjectGeneration {
                qualified_id,
                generation,
            },
            warning: None,
        }))
    }
}

/// The live entry that stops a new ticketed add for `qualified_id`
/// (difference 3): the ticketed entry with the highest generation, as
/// animus-queue-postgres v0.2.0 picks, else the first old-style entry for the
/// same subject in queue order.
fn live_entry_for_subject(state: &DispatchQueueState, qualified_id: &str) -> Option<usize> {
    let is_live = |entry: &DispatchQueueEntry| entry.status != DispatchQueueEntryStatus::Unknown;
    state
        .entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            is_live(entry)
                && entry.is_ticketed()
                && entry.subject_id.as_deref() == Some(qualified_id)
        })
        .max_by_key(|(_, entry)| entry.subject_generation)
        .map(|(index, _)| index)
        .or_else(|| {
            state.entries.iter().position(|entry| {
                is_live(entry)
                    && !entry.is_ticketed()
                    && entry
                        .dispatch
                        .as_ref()
                        .and_then(|dispatch| dispatch_canonical_id(dispatch).ok())
                        .as_deref()
                        == Some(qualified_id)
            })
        })
}

/// Make sure the entry at `index` has ticket identity and return it. An
/// old-style entry gets its canonical id and the subject's next generation
/// (difference 5: this happens when a ticketed call first needs it, not at
/// plugin start as in v0.2.9). Errors when the entry's subject can't be
/// identified.
pub(crate) fn ensure_ticket_identity(
    state: &mut DispatchQueueState,
    index: usize,
) -> Result<SubjectGeneration, String> {
    let entry = &state.entries[index];
    if let (Some(generation), Some(qualified_id)) =
        (entry.subject_generation, entry.subject_id.clone())
    {
        return Ok(SubjectGeneration {
            qualified_id,
            generation,
        });
    }
    let qualified_id = match entry.dispatch.as_ref() {
        Some(dispatch) => dispatch_canonical_id(dispatch)?,
        None => {
            return Err(format!(
                "queue entry {} has no dispatch to identify its subject",
                entry.entry_id
            ))
        }
    };
    let generation = next_subject_generation(state, &qualified_id);
    let entry = &mut state.entries[index];
    entry.subject_id = Some(qualified_id.clone());
    entry.subject_generation = Some(generation);
    Ok(SubjectGeneration {
        qualified_id,
        generation,
    })
}

/// Next generation for `qualified_id`: one more than the highest recorded or
/// live generation. Recorded, so it is never handed out again while the
/// counters exist.
fn next_subject_generation(state: &mut DispatchQueueState, qualified_id: &str) -> u64 {
    let recorded = state
        .subject_generations
        .get(qualified_id)
        .copied()
        .unwrap_or(0);
    let live = state
        .entries
        .iter()
        .filter(|entry| entry.subject_id.as_deref() == Some(qualified_id))
        .filter_map(|entry| entry.subject_generation)
        .max()
        .unwrap_or(0);
    let generation = recorded.max(live) + 1;
    state
        .subject_generations
        .insert(qualified_id.to_string(), generation);
    generation
}
```

Apply to `src/lib.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/lib.rs b/src/lib.rs
index 0bf2d5c..ba64b2b 100644
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -20,6 +20,7 @@
 
 pub mod dispatch_queue_state;
 pub mod dispatch_queue_store;
+pub mod fenced_queue;
 pub mod host_guard;
 pub mod identity;
 pub mod lease_ttl;
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --test fenced_enqueue`

Expected: 14 tests pass.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: queue/v2/enqueue with subject generations and idempotency keys"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 9: Ticketed hand-out (`queue/v2/lease`)

v0.2.9's `leaseV2` (`queue.ts:926-1023`) plus difference 5.

- **Limits:** up to `max` (1 to 5) entries.
- **Candidates:** the first `max(50, max * 10)` due waiting entries in queue order. Held entries, entries that aren't due yet and running entries are never candidates.
- **Kept waiting, and reported in `blocked`:**
  - an entry with no usable subject (`missing_execution_identity`)
  - an entry whose subject generation matches a fence in `exclude` (`subject_generation_active`)
  - an entry whose branch is held by a fence in `exclude`, or by a running entry, including one handed out earlier in the same call (`repository_ref_collision`, with the holder's fence)
- **Difference 5:** old-style entries get ticket identity when the scan reaches them.
- **Handing out an entry:**
  - It keeps a workflow id it already has; otherwise it takes the next unused proposed id.
  - Its workflow generation is set to 1 if it has none.
  - Its lease owner is the caller, and its lease generation rises by one.
  - Its expiry is now plus the ticket length.

**Files:**

- Modify: `src/fenced_queue.rs`
- Modify: `src/queue_service.rs`
- Create: `tests/fenced_lease.rs`

**Interfaces:**

- Consumes: `ensure_ticket_identity` (Task 8), `DispatchQueueEntry::execution_fence` (Task 5), `entry_to_protocol`, now `pub(crate)` in `src/queue_service.rs`.
- Produces (`src/fenced_queue.rs`):
  - `pub const MAX_LEASE_BATCH: usize = 5`
  - `pub fn lease_v2(&self, request: QueueLeaseV2Request) -> Result<QueueLeaseV2Response, QueueCallError>`

**Spec interpretation (flag in your report):** spec §7.1 mentions `expired_lease_recovery_required` for expired tickets. v0.2.9 never emits it from `leaseV2`: only waiting rows are candidates, so an expired ticket is neither handed out nor listed as blocked. The 0.7 daemon takes such tasks over with `queue/v2/lease/recover` from its own records. This plan follows v0.2.9, and `expired_ticket_is_not_handed_out_again` pins it.

**Repository comparison:** collisions use the protocol's `RepositoryReservation::collision_key()`, the key the 0.7 daemon itself uses: `trim().to_ascii_lowercase()` repository plus `head_ref`. v0.2.9 lowercases without trimming. The two agree for every reservation this queue stores, because Task 8 trims them.

- [ ] **Step 1: Write the failing tests**

Create `tests/fenced_lease.rs`:

```rust
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
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test fenced_lease`

Expected: compile error: no method named `lease_v2` on `QueueBackend`.

- [ ] **Step 3: Implement**

Apply to `src/fenced_queue.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/fenced_queue.rs b/src/fenced_queue.rs
index d969904..4f64f33 100644
--- a/src/fenced_queue.rs
+++ b/src/fenced_queue.rs
@@ -4,8 +4,11 @@
 //! seven differences listed in the design spec (§7.2). Each one is marked
 //! where it applies.
 
-use animus_execution_protocol::{RepositoryReservation, SubjectGeneration};
-use animus_queue_protocol::{QueueEnqueueV2Request, QueueEnqueueV2Response};
+use animus_execution_protocol::{ExecutionFence, RepositoryReservation, SubjectGeneration};
+use animus_queue_protocol::{
+    FencedQueueEntry, QueueEnqueueV2Request, QueueEnqueueV2Response, QueueLeaseBlock,
+    QueueLeaseBlockReason, QueueLeaseV2Request, QueueLeaseV2Response,
+};
 use chrono::Utc;
 
 use crate::dispatch_queue_state::{
@@ -14,9 +17,15 @@ use crate::dispatch_queue_state::{
 use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state};
 use crate::identity::{dispatch_canonical_id, dispatch_task_id};
 use crate::queue_history::find_history_by_idempotency_key;
-use crate::queue_service::{sweep_expired_entries, QueueBackend, QueueCallError};
+use crate::queue_service::{
+    entry_to_protocol, sweep_expired_entries, QueueBackend, QueueCallError,
+};
 use crate::request_hash::enqueue_request_hash;
 
+/// Most entries one `queue/v2/lease` call hands out. Advertised to the host as
+/// `max_lease_batch`; the 0.7 daemon requires at least 5.
+pub const MAX_LEASE_BATCH: usize = 5;
+
 /// Longest accepted idempotency key after trimming (v0.2.9).
 const MAX_IDEMPOTENCY_KEY_CHARS: usize = 256;
 
@@ -128,6 +137,156 @@ impl QueueBackend {
         })
     }
 
+    /// `queue/v2/lease`: hand out up to `max` (1 to 5) due Pending entries in
+    /// queue order, each with its full execution fence.
+    ///
+    /// Candidates are the first `max(50, max * 10)` due Pending entries.
+    /// Held, deferred and running entries are never handed out, so an entry
+    /// whose ticket expired must be taken over with `queue/v2/lease/recover`.
+    /// A candidate stays where it is and is reported in `blocked` when:
+    ///
+    /// - its subject can't be identified (`missing_execution_identity`);
+    /// - a fence in `exclude` holds the same subject generation
+    ///   (`subject_generation_active`);
+    /// - a fence in `exclude`, or a running entry, holds the same repository
+    ///   and branch (`repository_ref_collision`).
+    ///
+    /// Old-style entries get ticket identity here (difference 5). A handed-out
+    /// entry keeps a workflow id it already has, otherwise it takes the next
+    /// unused id from `workflow_ids`. Its lease generation rises by one.
+    pub fn lease_v2(
+        &self,
+        request: QueueLeaseV2Request,
+    ) -> Result<QueueLeaseV2Response, QueueCallError> {
+        request.validate().map_err(QueueCallError::InvalidParams)?;
+        if request.max > MAX_LEASE_BATCH {
+            return Err(QueueCallError::InvalidParams(
+                "queue v2 lease requires max 1..5, owner_id, and max unique workflow_ids"
+                    .to_string(),
+            ));
+        }
+        let owner_id = request.owner_id.trim().to_string();
+
+        let _lock = acquire_queue_lock(self.project_root())?;
+        let mut state = load_queue_state(self.project_root())?.unwrap_or_default();
+        let now = Utc::now();
+        let finished = sweep_expired_entries(&mut state, now);
+        let candidates: Vec<usize> = state
+            .entries
+            .iter()
+            .enumerate()
+            .filter(|(_, entry)| {
+                entry.status == DispatchQueueEntryStatus::Pending
+                    && !entry.is_deferred_until_future(now)
+            })
+            .map(|(index, _)| index)
+            .take((request.max * 10).max(50))
+            .collect();
+
+        let expires_at = now + chrono::Duration::seconds(self.lease_ttl_secs());
+        let mut changed = !finished.is_empty();
+        let mut leased = Vec::new();
+        let mut blocked = Vec::new();
+        let mut unused_workflow_ids = request.workflow_ids.iter();
+        for index in candidates {
+            if leased.len() >= request.max {
+                break;
+            }
+            let entry_id = state.entries[index].entry_id.clone();
+            let was_ticketed = state.entries[index].is_ticketed();
+            let identity = if state.entries[index].dispatch.is_some() {
+                ensure_ticket_identity(&mut state, index)
+            } else {
+                Err(format!("queue entry {entry_id} has no dispatch envelope"))
+            };
+            let subject = match identity {
+                Ok(subject) => subject,
+                Err(error) => {
+                    tracing::warn!(
+                        entry_id = %entry_id,
+                        %error,
+                        "queue/v2/lease: entry has no usable subject identity"
+                    );
+                    blocked.push(lease_block(
+                        entry_id,
+                        QueueLeaseBlockReason::MissingExecutionIdentity,
+                        None,
+                    ));
+                    continue;
+                }
+            };
+            changed |= !was_ticketed;
+
+            if let Some(conflict) = request
+                .exclude
+                .iter()
+                .find(|fence| fence.subject.as_ref() == Some(&subject))
+            {
+                blocked.push(lease_block(
+                    entry_id,
+                    QueueLeaseBlockReason::SubjectGenerationActive,
+                    Some(conflict.clone()),
+                ));
+                continue;
+            }
+            if let Some(repository) = state.entries[index].repository.clone() {
+                let key = repository.collision_key();
+                if let Some(conflict) = request.exclude.iter().find(|fence| {
+                    fence
+                        .repository
+                        .as_ref()
+                        .is_some_and(|held| held.collision_key() == key)
+                }) {
+                    blocked.push(lease_block(
+                        entry_id,
+                        QueueLeaseBlockReason::RepositoryRefCollision,
+                        Some(conflict.clone()),
+                    ));
+                    continue;
+                }
+                if let Some(running) = running_entry_on_branch(&state, &key) {
+                    blocked.push(lease_block(
+                        entry_id,
+                        QueueLeaseBlockReason::RepositoryRefCollision,
+                        state.entries[running].execution_fence(),
+                    ));
+                    continue;
+                }
+            }
+
+            let existing_workflow_id = state.entries[index]
+                .workflow_id
+                .clone()
+                .filter(|id| !id.is_empty());
+            let Some(workflow_id) =
+                existing_workflow_id.or_else(|| unused_workflow_ids.next().cloned())
+            else {
+                break;
+            };
+            let entry = &mut state.entries[index];
+            entry.status = DispatchQueueEntryStatus::Assigned;
+            entry.workflow_id = Some(workflow_id);
+            entry.workflow_generation = Some(entry.workflow_generation.unwrap_or(1));
+            entry.lease_owner = Some(owner_id.clone());
+            entry.lease_generation += 1;
+            entry.lease_expires_at = Some(expires_at);
+            entry.assigned_at = Some(now.to_rfc3339());
+            entry.held_at = None;
+            changed = true;
+            leased.push(FencedQueueEntry {
+                entry: entry_to_protocol(entry).expect("checked above: entry has a dispatch"),
+                execution: entry
+                    .execution_fence()
+                    .expect("a just-leased entry has complete ticket identity"),
+            });
+        }
+
+        if changed {
+            self.commit(&state, &finished)?;
+        }
+        Ok(QueueLeaseV2Response { leased, blocked })
+    }
+
     /// The original receipt for `key`, from the live file or, if the entry
     /// already finished, from the history. Errors when the key is bound to
     /// different content.
@@ -175,6 +334,36 @@ impl QueueBackend {
     }
 }
 
+fn lease_block(
+    entry_id: String,
+    reason: QueueLeaseBlockReason,
+    conflicts_with: Option<ExecutionFence>,
+) -> QueueLeaseBlock {
+    QueueLeaseBlock {
+        entry_id,
+        reason,
+        conflicts_with,
+    }
+}
+
+/// The running entry holding the branch with `collision_key`, earliest
+/// assigned first (v0.2.9 orders by `assigned_at`).
+fn running_entry_on_branch(state: &DispatchQueueState, collision_key: &str) -> Option<usize> {
+    state
+        .entries
+        .iter()
+        .enumerate()
+        .filter(|(_, entry)| {
+            entry.status == DispatchQueueEntryStatus::Assigned
+                && entry
+                    .repository
+                    .as_ref()
+                    .is_some_and(|held| held.collision_key() == collision_key)
+        })
+        .min_by(|(_, left), (_, right)| left.assigned_at.cmp(&right.assigned_at))
+        .map(|(index, _)| index)
+}
+
 /// The live entry that stops a new ticketed add for `qualified_id`
 /// (difference 3): the ticketed entry with the highest generation, as
 /// animus-queue-postgres v0.2.0 picks, else the first old-style entry for the
```

Apply to `src/queue_service.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/queue_service.rs b/src/queue_service.rs
index 24b5e86..c03fb8e 100644
--- a/src/queue_service.rs
+++ b/src/queue_service.rs
@@ -801,7 +801,7 @@ pub enum QueueReleasePendingError {
     Backend(anyhow::Error),
 }
 
-fn entry_to_protocol(entry: &DispatchQueueEntry) -> Option<QueueEntry> {
+pub(crate) fn entry_to_protocol(entry: &DispatchQueueEntry) -> Option<QueueEntry> {
     // The wire-level `QueueEntry.subject_dispatch` is required. Entries with
     // no persisted envelope are corrupt legacy state — log + skip them
     // instead of panicking, so callers see a healthy queue minus the
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --test fenced_lease`

Expected: 16 tests pass.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: queue/v2/lease with typed blocks and branch collisions"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 10: Renew, take over, done and put back

v0.2.9's `fencedMutation` (`queue.ts:1025-1132`) plus difference 1. Ticket problems are outcomes, never errors.

**Order of checks for a live entry:**

1. A put-back of a waiting entry with an exact match gives `already_applied`.
2. Anything not running gives `not_assigned`, with reason `queue entry is pending` or `queue entry is held`.
3. A ticket that isn't an exact match gives `stale_fence`, with reason `execution fence does not own this queue lease`.

An exact match compares the owner, entry id, lease/workflow/subject generations, workflow id and repository, ignoring case in the repository name. It never compares the expiry.

**Per call:**

- **Renew:**
  - An expired ticket gives `stale_fence` with reason `queue lease has expired and requires recovery`.
  - Otherwise the expiry becomes the later of the current expiry and now + ttl (spec §7.1: never earlier).
- **Take over (recover):**
  - A new owner that is empty or equal to the holder is invalid params.
  - A live ticket gives `lease_still_live` with the current fence.
  - Otherwise the new owner gets the ticket, the lease generation rises by one, and the expiry becomes now + ttl.
- **Done:** removes the entry and records history. There is no expiry check (difference 1).
- **Put back:**
  - The entry returns to waiting and keeps its workflow id and ticket fields, which is how retries are recognised. An audit line is added. There is no expiry check (difference 1).
  - The next hand-out raises the lease generation.
- **Requested ttl:** `min(ttl_secs, setting)`, or the setting when not given (v0.2.9).

**Entry no longer live:** the queue answers from the history.

- A "done" with an exact match on a completed, failed or cancelled entry gives `already_applied`, with the fence from the history.
- Anything else gives `not_assigned`, with reason `queue entry is done` or `queue entry is dropped`.
- No history line gives `not_found`.

**Files:**

- Modify: `src/fenced_queue.rs`
- Create: `tests/fenced_tickets.rs`

**Interfaces:**

- Consumes: history lookups (Task 6), `lease_is_live` and `execution_fence` (Task 5).
- Produces (`src/fenced_queue.rs`, `impl QueueBackend`, all returning `Result<QueueLeaseMutationResponse, QueueCallError>`):
  - `pub fn renew_lease(&self, request: QueueLeaseRenewRequest)`
  - `pub fn recover_lease(&self, request: QueueLeaseRecoverRequest)`
  - `pub fn completion_v2(&self, request: QueueCompletionV2Request)`
  - `pub fn release_pending_v2(&self, request: QueueReleasePendingV2Request)`

**Where "never earlier" differs from v0.2.9:** v0.2.9 sets the expiry to exactly now + ttl, so a caller asking for a shorter `ttl_secs` than remains would pull it earlier. The 0.7 daemon rejects that (`validate_queue_transition`: "renew shortened lease expiry"). With the daemon's normal calls, which pass no `ttl_secs`, both give the same result.

- [ ] **Step 1: Write the failing tests**

Create `tests/fenced_tickets.rs`:

```rust
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
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test fenced_tickets`

Expected: compile error: no method named `renew_lease` on `QueueBackend`.

- [ ] **Step 3: Implement**

Apply to `src/fenced_queue.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/fenced_queue.rs b/src/fenced_queue.rs
index 4f64f33..f82386e 100644
--- a/src/fenced_queue.rs
+++ b/src/fenced_queue.rs
@@ -4,19 +4,26 @@
 //! seven differences listed in the design spec (§7.2). Each one is marked
 //! where it applies.
 
-use animus_execution_protocol::{ExecutionFence, RepositoryReservation, SubjectGeneration};
+use animus_execution_protocol::{
+    ExecutionFence, RepositoryReservation, SubjectGeneration, EXECUTION_FENCE_SCHEMA_ID,
+    EXECUTION_FENCE_VERSION,
+};
 use animus_queue_protocol::{
-    FencedQueueEntry, QueueEnqueueV2Request, QueueEnqueueV2Response, QueueLeaseBlock,
-    QueueLeaseBlockReason, QueueLeaseV2Request, QueueLeaseV2Response,
+    status, FencedQueueEntry, QueueCompletionV2Request, QueueEnqueueV2Request,
+    QueueEnqueueV2Response, QueueLeaseBlock, QueueLeaseBlockReason, QueueLeaseMutationOutcome,
+    QueueLeaseMutationResponse, QueueLeaseRecoverRequest, QueueLeaseRenewRequest,
+    QueueLeaseV2Request, QueueLeaseV2Response, QueueReleasePendingV2Request,
 };
-use chrono::Utc;
+use chrono::{Duration, Utc};
 
 use crate::dispatch_queue_state::{
-    DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
+    DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
 };
 use crate::dispatch_queue_store::{acquire_queue_lock, load_queue_state};
 use crate::identity::{dispatch_canonical_id, dispatch_task_id};
-use crate::queue_history::find_history_by_idempotency_key;
+use crate::queue_history::{
+    find_history_by_entry_id, find_history_by_idempotency_key, HistoryOutcome, HistoryRecord,
+};
 use crate::queue_service::{
     entry_to_protocol, sweep_expired_entries, QueueBackend, QueueCallError,
 };
@@ -287,6 +294,240 @@ impl QueueBackend {
         Ok(QueueLeaseV2Response { leased, blocked })
     }
 
+    /// `queue/v2/lease/renew`: extend a live ticket. The expiry moves to
+    /// `now + ttl` but never earlier than it already is (spec §7.1: the 0.7
+    /// daemon rejects a renewal whose expiry goes backwards). The generation
+    /// stays the same. An expired ticket must be taken over instead.
+    pub fn renew_lease(
+        &self,
+        request: QueueLeaseRenewRequest,
+    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
+        request.validate().map_err(QueueCallError::InvalidParams)?;
+        let ttl_secs = self.requested_ttl(request.ttl_secs);
+        self.fenced_mutation(&request.execution, FencedOperation::Renew { ttl_secs })
+    }
+
+    /// `queue/v2/lease/recover`: hand an expired ticket to a different owner.
+    /// The lease generation rises by exactly one; the workflow id, workflow
+    /// generation and subject generation stay.
+    pub fn recover_lease(
+        &self,
+        request: QueueLeaseRecoverRequest,
+    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
+        request.validate().map_err(QueueCallError::InvalidParams)?;
+        let ttl_secs = self.requested_ttl(request.ttl_secs);
+        self.fenced_mutation(
+            &request.execution,
+            FencedOperation::Recover {
+                new_owner_id: request.new_owner_id.trim().to_string(),
+                ttl_secs,
+            },
+        )
+    }
+
+    /// `queue/v2/completion`: finish the ticket's entry and move it to the
+    /// history. Accepted with an expired ticket as long as nobody took the
+    /// task over (difference 1). A repeated "done" is `already_applied` and
+    /// the first outcome is kept.
+    pub fn completion_v2(
+        &self,
+        request: QueueCompletionV2Request,
+    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
+        request.validate().map_err(QueueCallError::InvalidParams)?;
+        let outcome = HistoryOutcome::from_completion_status(&request.status)
+            .expect("validate() accepts only terminal statuses");
+        self.fenced_mutation(&request.execution, FencedOperation::Complete { outcome })
+    }
+
+    /// `queue/v2/release_pending`: put the ticket's entry back to waiting. It
+    /// keeps its workflow id and ticket fields, so a retry is recognised and
+    /// the next hand-out raises the lease generation. Accepted with an
+    /// expired ticket as long as nobody took the task over (difference 1).
+    pub fn release_pending_v2(
+        &self,
+        request: QueueReleasePendingV2Request,
+    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
+        request.validate().map_err(QueueCallError::InvalidParams)?;
+        self.fenced_mutation(
+            &request.execution,
+            FencedOperation::ReleasePending {
+                reason: request.reason.trim().to_string(),
+            },
+        )
+    }
+
+    /// A caller's `ttl_secs`, capped at the configured ticket length (v0.2.9).
+    fn requested_ttl(&self, ttl_secs: Option<u64>) -> i64 {
+        let limit = self.lease_ttl_secs();
+        ttl_secs.map_or(limit, |secs| {
+            i64::try_from(secs).unwrap_or(i64::MAX).min(limit)
+        })
+    }
+
+    /// The body shared by the four ticket calls (v0.2.9 `fencedMutation`,
+    /// plus difference 1). Ticket problems are outcomes, not errors.
+    fn fenced_mutation(
+        &self,
+        execution: &ExecutionFence,
+        operation: FencedOperation,
+    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
+        let entry_id = execution
+            .queue_lease
+            .as_ref()
+            .expect("validate_queue_backed() requires queue_lease")
+            .entry_id
+            .clone();
+        let _lock = acquire_queue_lock(self.project_root())?;
+        let mut state = load_queue_state(self.project_root())?.unwrap_or_default();
+        let Some(index) = state
+            .entries
+            .iter()
+            .position(|entry| entry.entry_id == entry_id)
+        else {
+            return self.finished_entry_outcome(&entry_id, execution, &operation);
+        };
+
+        let now = Utc::now();
+        let entry = &mut state.entries[index];
+        if matches!(operation, FencedOperation::ReleasePending { .. })
+            && entry.status == DispatchQueueEntryStatus::Pending
+            && exact_fence_matches(entry, execution)
+        {
+            return Ok(mutation_response(
+                QueueLeaseMutationOutcome::AlreadyApplied,
+                None,
+                None,
+            ));
+        }
+        if entry.status != DispatchQueueEntryStatus::Assigned {
+            return Ok(mutation_response(
+                QueueLeaseMutationOutcome::NotAssigned,
+                None,
+                Some(format!("queue entry is {}", entry.status.as_wire())),
+            ));
+        }
+        if !exact_fence_matches(entry, execution) {
+            return Ok(mutation_response(
+                QueueLeaseMutationOutcome::StaleFence,
+                None,
+                Some("execution fence does not own this queue lease".to_string()),
+            ));
+        }
+
+        match operation {
+            FencedOperation::Recover {
+                new_owner_id,
+                ttl_secs,
+            } => {
+                if new_owner_id.is_empty()
+                    || entry.lease_owner.as_deref() == Some(new_owner_id.as_str())
+                {
+                    return Err(QueueCallError::InvalidParams(
+                        "lease recovery requires a different non-empty owner".to_string(),
+                    ));
+                }
+                if entry.lease_is_live(now) {
+                    return Ok(mutation_response(
+                        QueueLeaseMutationOutcome::LeaseStillLive,
+                        entry.execution_fence(),
+                        None,
+                    ));
+                }
+                entry.lease_owner = Some(new_owner_id);
+                entry.lease_generation += 1;
+                entry.lease_expires_at = Some(now + Duration::seconds(ttl_secs));
+                let fence = entry.execution_fence();
+                self.commit(&state, &[])?;
+                Ok(mutation_response(
+                    QueueLeaseMutationOutcome::Applied,
+                    fence,
+                    None,
+                ))
+            }
+            FencedOperation::Renew { ttl_secs } => {
+                if !entry.lease_is_live(now) {
+                    return Ok(mutation_response(
+                        QueueLeaseMutationOutcome::StaleFence,
+                        None,
+                        Some("queue lease has expired and requires recovery".to_string()),
+                    ));
+                }
+                let renewed = now + Duration::seconds(ttl_secs);
+                entry.lease_expires_at = entry.lease_expires_at.max(Some(renewed));
+                let fence = entry.execution_fence();
+                self.commit(&state, &[])?;
+                Ok(mutation_response(
+                    QueueLeaseMutationOutcome::Applied,
+                    fence,
+                    None,
+                ))
+            }
+            // Difference 1: no expiry check. The exact match above already
+            // proves nobody took the task over.
+            FencedOperation::Complete { outcome } => {
+                let done = state.entries.remove(index);
+                let fence = done.execution_fence();
+                let record = HistoryRecord::finished(&done, outcome, "queue/v2/completion", None);
+                self.commit(&state, &[record])?;
+                Ok(mutation_response(
+                    QueueLeaseMutationOutcome::Applied,
+                    fence,
+                    None,
+                ))
+            }
+            FencedOperation::ReleasePending { reason } => {
+                entry.status = DispatchQueueEntryStatus::Pending;
+                entry.assigned_at = None;
+                entry.audit_log.push(DispatchQueueAuditEntry {
+                    at: now.to_rfc3339(),
+                    method: "queue/v2/release_pending".to_string(),
+                    from_status: status::ASSIGNED.to_string(),
+                    to_status: status::PENDING.to_string(),
+                    reason,
+                });
+                self.commit(&state, &[])?;
+                Ok(mutation_response(
+                    QueueLeaseMutationOutcome::Applied,
+                    None,
+                    None,
+                ))
+            }
+        }
+    }
+
+    /// The outcome for an entry that is no longer live. v0.2.9 keeps
+    /// finished rows and answers from them; this queue answers from the
+    /// history, taking the first record for the entry.
+    fn finished_entry_outcome(
+        &self,
+        entry_id: &str,
+        execution: &ExecutionFence,
+        operation: &FencedOperation,
+    ) -> Result<QueueLeaseMutationResponse, QueueCallError> {
+        let Some(record) = find_history_by_entry_id(self.project_root(), entry_id)? else {
+            return Ok(mutation_response(
+                QueueLeaseMutationOutcome::NotFound,
+                None,
+                None,
+            ));
+        };
+        if matches!(operation, FencedOperation::Complete { .. })
+            && record.outcome != HistoryOutcome::Dropped
+            && exact_fence_matches(&record.entry, execution)
+        {
+            return Ok(mutation_response(
+                QueueLeaseMutationOutcome::AlreadyApplied,
+                record.entry.execution_fence(),
+                None,
+            ));
+        }
+        Ok(mutation_response(
+            QueueLeaseMutationOutcome::NotAssigned,
+            None,
+            Some(format!("queue entry is {}", record.outcome.state_word())),
+        ))
+    }
+
     /// The original receipt for `key`, from the live file or, if the entry
     /// already finished, from the history. Errors when the key is bound to
     /// different content.
@@ -334,6 +575,62 @@ impl QueueBackend {
     }
 }
 
+/// One of the four ticket calls, with its validated inputs.
+enum FencedOperation {
+    Renew { ttl_secs: i64 },
+    Recover { new_owner_id: String, ttl_secs: i64 },
+    Complete { outcome: HistoryOutcome },
+    ReleasePending { reason: String },
+}
+
+fn mutation_response(
+    outcome: QueueLeaseMutationOutcome,
+    execution: Option<ExecutionFence>,
+    reason: Option<String>,
+) -> QueueLeaseMutationResponse {
+    QueueLeaseMutationResponse {
+        outcome,
+        execution,
+        reason,
+    }
+}
+
+/// `true` when `execution` is the entry's current ticket (v0.2.9
+/// `exactFenceMatches`). Matches the owner and every id and generation, and
+/// the repository, but not the expiry time.
+fn exact_fence_matches(entry: &DispatchQueueEntry, execution: &ExecutionFence) -> bool {
+    let (Some(subject), Some(lease)) = (&execution.subject, &execution.queue_lease) else {
+        return false;
+    };
+    execution.schema == EXECUTION_FENCE_SCHEMA_ID
+        && execution.version == EXECUTION_FENCE_VERSION
+        && entry.workflow_id.as_deref() == Some(execution.workflow_id.as_str())
+        && entry.workflow_generation == Some(execution.workflow_generation)
+        && entry.subject_id.as_deref() == Some(subject.qualified_id.as_str())
+        && entry.subject_generation == Some(subject.generation)
+        && entry.entry_id == lease.entry_id
+        && entry.lease_owner.as_deref() == Some(lease.owner_id.as_str())
+        && entry.lease_generation == lease.generation
+        && same_repository(execution.repository.as_ref(), entry.repository.as_ref())
+}
+
+/// v0.2.9 `sameRepository`: both absent, or the same repository (ignoring
+/// case) with the same base and head refs.
+fn same_repository(
+    left: Option<&RepositoryReservation>,
+    right: Option<&RepositoryReservation>,
+) -> bool {
+    match (left, right) {
+        (None, None) => true,
+        (Some(left), Some(right)) => {
+            left.repository.to_lowercase() == right.repository.to_lowercase()
+                && left.base_ref == right.base_ref
+                && left.head_ref == right.head_ref
+        }
+        _ => false,
+    }
+}
+
 fn lease_block(
     entry_id: String,
     reason: QueueLeaseBlockReason,
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --test fenced_tickets`

Expected: 21 tests pass.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: fenced renew, recover, completion and release_pending"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 11: Wire the ticketed methods and announce the capability

Adds the six `queue/v2/*` methods to the plugin, using the protocol's strict request types (unknown fields become `-32602`). Advertises `generation_fenced_leases_v1: true` and `max_lease_batch: 5`, the same as both Postgres queues, and lists the six methods in `capabilities.methods`.

The contract test (spec §8.2) runs the real binary. It checks the handshake with a copy of the daemon's `require_generation_fenced_queue`, decodes every reply strictly, and checks each ticket change with a copy of the daemon's `validate_queue_transition` (`animus-cli` `coding_scheduler.rs`).

**Files:**

- Modify: `src/plugin.rs`
- Create: `tests/stdio_contract.rs`

**Interfaces:**

- Consumes: every `QueueBackend` v2 method from Tasks 8–10; `MAX_LEASE_BATCH` (Task 9).
- Produces (`src/plugin.rs`): `async fn handle_v2<Req, Resp>(id, params, backend, method: &str, call: fn(&QueueBackend, Req) -> Result<Resp, QueueCallError>) -> RpcResponse`

- [ ] **Step 1: Write the failing tests**

Create `tests/stdio_contract.rs`:

```rust
//! Contract test against the 0.7 daemon's rules (spec §8.2): the real binary
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
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test stdio_contract`

Expected: `require_generation_fenced_queue` assertion fails: `generation_fenced_leases_v1` is false.

- [ ] **Step 3: Implement**

Apply to `src/plugin.rs` (`git apply` accepts this patch):

```diff
diff --git a/src/plugin.rs b/src/plugin.rs
index ba11ae2..b2f18e9 100644
--- a/src/plugin.rs
+++ b/src/plugin.rs
@@ -1,7 +1,8 @@
 //! Stdio JSON-RPC loop for the `animus-queue-default` plugin.
 //!
 //! Handles `initialize`, `$/ping`, `health/check`, `shutdown`, `exit`,
-//! `--manifest` / `--help` CLI shortcuts, and the 10 `queue/*` methods.
+//! `--manifest` / `--help` CLI shortcuts, the old-style `queue/*` methods and
+//! the six ticketed `queue/v2/*` methods.
 
 use std::io::{self, IsTerminal, Write};
 use std::path::PathBuf;
@@ -21,11 +22,16 @@ use animus_queue_protocol::{
     METHOD_QUEUE_NEXT_DEADLINE, METHOD_QUEUE_RELEASE, METHOD_QUEUE_RELEASE_PENDING,
     METHOD_QUEUE_REORDER, METHOD_QUEUE_STATS, PROTOCOL_VERSION as QUEUE_PROTOCOL_VERSION,
 };
+use animus_queue_protocol::{
+    METHOD_QUEUE_COMPLETION_V2, METHOD_QUEUE_ENQUEUE_V2, METHOD_QUEUE_LEASE_RECOVER,
+    METHOD_QUEUE_LEASE_RENEW, METHOD_QUEUE_LEASE_V2, METHOD_QUEUE_RELEASE_PENDING_V2,
+};
 use anyhow::Result;
 use serde_json::{json, Value};
 use tokio::io::{AsyncReadExt, AsyncWriteExt};
 use tokio::sync::{Mutex, RwLock};
 
+use crate::fenced_queue::MAX_LEASE_BATCH;
 use crate::host_guard::check_host_protocol;
 use crate::lease_ttl::{lease_ttl_from_env, LEASE_TTL_ENV};
 use crate::queue_service::{
@@ -35,7 +41,7 @@ use crate::queue_service::{
 const PLUGIN_NAME: &str = "animus-queue-default";
 const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");
 const PLUGIN_DESCRIPTION: &str =
-    "Reference queue plugin for Animus v0.5 (file-backed dispatch queue with atomic lease).";
+    "Reference queue plugin for Animus 0.7 (file-backed dispatch queue with generation-fenced leases).";
 
 /// Stable entrypoint for the plugin process. Call from `#[tokio::main]` in
 /// `main.rs`.
@@ -180,6 +186,12 @@ fn queue_methods() -> Vec<&'static str> {
         METHOD_QUEUE_REORDER,
         METHOD_QUEUE_MARK_ASSIGNED,
         METHOD_QUEUE_COMPLETION,
+        METHOD_QUEUE_ENQUEUE_V2,
+        METHOD_QUEUE_LEASE_V2,
+        METHOD_QUEUE_LEASE_RENEW,
+        METHOD_QUEUE_LEASE_RECOVER,
+        METHOD_QUEUE_COMPLETION_V2,
+        METHOD_QUEUE_RELEASE_PENDING_V2,
         "health/check",
     ]
 }
@@ -214,6 +226,66 @@ async fn handle_request(
             Some(handle_mark_assigned(id, request.params, &backend).await)
         }
         METHOD_QUEUE_COMPLETION => Some(handle_completion(id, request.params, &backend).await),
+        METHOD_QUEUE_ENQUEUE_V2 => Some(
+            handle_v2(
+                id,
+                request.params,
+                &backend,
+                METHOD_QUEUE_ENQUEUE_V2,
+                QueueBackend::enqueue_v2,
+            )
+            .await,
+        ),
+        METHOD_QUEUE_LEASE_V2 => Some(
+            handle_v2(
+                id,
+                request.params,
+                &backend,
+                METHOD_QUEUE_LEASE_V2,
+                QueueBackend::lease_v2,
+            )
+            .await,
+        ),
+        METHOD_QUEUE_LEASE_RENEW => Some(
+            handle_v2(
+                id,
+                request.params,
+                &backend,
+                METHOD_QUEUE_LEASE_RENEW,
+                QueueBackend::renew_lease,
+            )
+            .await,
+        ),
+        METHOD_QUEUE_LEASE_RECOVER => Some(
+            handle_v2(
+                id,
+                request.params,
+                &backend,
+                METHOD_QUEUE_LEASE_RECOVER,
+                QueueBackend::recover_lease,
+            )
+            .await,
+        ),
+        METHOD_QUEUE_COMPLETION_V2 => Some(
+            handle_v2(
+                id,
+                request.params,
+                &backend,
+                METHOD_QUEUE_COMPLETION_V2,
+                QueueBackend::completion_v2,
+            )
+            .await,
+        ),
+        METHOD_QUEUE_RELEASE_PENDING_V2 => Some(
+            handle_v2(
+                id,
+                request.params,
+                &backend,
+                METHOD_QUEUE_RELEASE_PENDING_V2,
+                QueueBackend::release_pending_v2,
+            )
+            .await,
+        ),
         other => Some(RpcResponse::err(
             id,
             RpcError {
@@ -288,14 +360,12 @@ async fn handle_initialize(
     *backend.write().await =
         Some(QueueBackend::new(project_root).with_lease_ttl(lease_ttl_from_env()));
 
+    // Identical to animus-postgres v0.2.9 and animus-queue-postgres v0.2.0.
+    // The 0.7 daemon requires the flag and a batch of at least 5.
     let capabilities = QueueCapabilities {
         priority_weighted: false,
-        // No backend-side cap on lease batch size — file-locked state happily
-        // handles batches of any size the daemon's capacity budgeter requests.
-        // Hosts clamp `queue/lease.max` to this value; advertising `u32::MAX`
-        // is the "effectively unlimited" sentinel for the reference plugin.
-        max_lease_batch: u32::MAX,
-        generation_fenced_leases_v1: false,
+        max_lease_batch: MAX_LEASE_BATCH as u32,
+        generation_fenced_leases_v1: true,
     };
     let extra = serde_json::to_value(capabilities).unwrap_or(Value::Null);
     let mut kind_capabilities = std::collections::HashMap::new();
@@ -701,6 +771,38 @@ async fn handle_completion(
     }
 }
 
+// ============================================================
+// queue/v2/*
+// ============================================================
+
+/// Shared handler for the ticketed methods. Params are the protocol's strict
+/// request types (unknown fields are rejected); bad input is `-32602`, and
+/// ticket problems come back as normal outcomes inside the result.
+async fn handle_v2<Req, Resp>(
+    id: Option<Value>,
+    params: Option<Value>,
+    backend: &Arc<RwLock<Option<QueueBackend>>>,
+    method: &str,
+    call: fn(&QueueBackend, Req) -> std::result::Result<Resp, QueueCallError>,
+) -> RpcResponse
+where
+    Req: serde::de::DeserializeOwned,
+    Resp: serde::Serialize,
+{
+    let backend = match require_backend(id.clone(), backend).await {
+        Ok(b) => b,
+        Err(response) => return response,
+    };
+    let request: Req = match parse_params(id.clone(), params, method) {
+        Ok(req) => req,
+        Err(response) => return response,
+    };
+    match call(&backend, request) {
+        Ok(response) => to_value_response(id, &response),
+        Err(error) => call_error_response(id, error, method),
+    }
+}
+
 // ============================================================
 // helpers
 // ============================================================
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --test stdio_contract`

Expected: 3 tests pass (the takeover test sleeps about 3 seconds).

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: advertise generation-fenced leases and wire queue/v2/*"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 12: Upgrade, rollback, crash and concurrency tests

Spec §8.1's cross-cutting tests. They use the real v0.3.3 release as a renamed dev-dependency, `queue_v033`; cargo accepts a git dependency on the package's own earlier tag.

- **Upgrade:** a file written by v0.3.3 (waiting, held and running entries) loads as it is. The waiting entry gets identity at its first ticketed hand-out. The running entry is finished by the old-style "done". The held entry runs once released.
- **Rollback:** v0.3.3 lists a v0.4.0 file with every status intact, and can write it. v0.4.0 then reads it back.
- **Crash:** a history line appended without the `queue.json` replace. The retry is `applied`, a later retry is `already_applied`, and the first line wins.
- **Concurrency:** 8 threads and 4 plugin processes lease from one project; no entry is handed out twice. 8 threads adding the same task make one entry.

These tests should pass straight away. If one fails, the defect is in Tasks 5–11, not in this task.

**Files:**

- Modify: `Cargo.toml`
- Create: `tests/crash_and_concurrency.rs`
- Create: `tests/upgrade_rollback.rs`
- Modify: `Cargo.lock` (cargo updates it; commit the result)

**Interfaces:**

- Consumes: everything above.
- Produces: nothing new.

- [ ] **Step 1: Add the v0.3.3 dev-dependency and the tests**

Apply to `Cargo.toml` (`git apply` accepts this patch):

```diff
diff --git a/Cargo.toml b/Cargo.toml
index a908f05..3e0f4b6 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -35,3 +35,5 @@ semver = "1"
 
 [dev-dependencies]
 tempfile = "3"
+# The previous release, for the upgrade and rollback tests (spec §3.3, §6.4).
+queue_v033 = { package = "animus-queue-default", git = "https://github.com/launchapp-dev/animus-queue-default", tag = "v0.3.3" }
```

Create `tests/crash_and_concurrency.rs`:

```rust
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
    QueueCompletionV2Request, QueueLeaseMutationOutcome, QueueLeaseV2Request, QueueLeaseV2Response,
    METHOD_QUEUE_LEASE_V2,
};
use common::{enqueue_request, read_entry, PluginProcess};

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

    // Readers take the first line, and later retries are acknowledged.
    let record = find_history_by_entry_id(temp.path(), &entry_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.outcome, HistoryOutcome::Completed);
    let again = backend.completion_v2(completion).unwrap();
    assert_eq!(again.outcome, QueueLeaseMutationOutcome::AlreadyApplied);
    assert_eq!(history_lines_for(temp.path(), &entry_id), 2);
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
```

Create `tests/upgrade_rollback.rs`:

```rust
//! Moving between queue v0.3.3 and v0.4.0 on the same `queue.json`
//! (spec §3.3 and §6.4), using the real v0.3.3 crate.

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
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo test --test upgrade_rollback --test crash_and_concurrency`

Expected: they pass. If one fails, stop and debug the task it points at.

- [ ] **Step 3: Implement**

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test --all-features`

Expected: all pass.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: upgrade from v0.3.3, rollback, crash safety and concurrency"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 13: README, release notes and crate docs

Spec §9:

- **README:** the compatibility table, the guard and its message, rollback, the ticket-length setting, both storage files and the `.gitignore` note.
- **Release notes:** the same points, plus the seven differences and the deliberate matches.
- **Crate docs:** the lib.rs doc comment is corrected to match.

**Files:**

- Modify: `README.md`
- Create: `docs/releases/v0.4.0.md`
- Modify: `src/lib.rs`

**Interfaces:**

- Consumes: none. Documentation only.

- [ ] **Step 1: Check the source of truth**

No tests: this task is documentation. Before writing, re-read spec §3 and §9 and the seven differences in §7.2; the text below must say the same things.

- [ ] **Step 2: Run the tests to see them fail**

Run: `cargo doc --no-deps`

Expected: builds (docs have no failing test; review the text instead).

- [ ] **Step 3: Implement**

Replace the whole of `README.md` with:

````markdown
# animus-queue-default

The default `queue` plugin for [Animus](https://github.com/launchapp-dev/animus-cli): a file-backed dispatch queue with generation-fenced ("ticketed") leases.

When the daemon takes a task, the queue gives it a ticket. The ticket names the daemon holding the task, carries counters (generations), and expires. Every later call about the task ("still working", "done", "put it back") must show the ticket. If a daemon crashes and another takes the task over, the counters move on, so the first daemon's late "done" is refused.

## Which Animus uses which queue

| Animus CLI | Queue | Why |
|---|---|---|
| 0.6.x and older (v0.4.0 – v0.6.33) | `animus-queue-default` v0.3.3 or older | Old daemons don't use tickets |
| 0.7 and newer | `animus-queue-default` v0.4.0 or newer | The 0.7 daemon requires tickets |

A 0.6.x CLI already installs v0.3.3 through its built-in pin (`animus plugin install-defaults`, `animus daemon start --auto-install`, `animus plugin update`). Installing this plugin with no tag, or with `@v0.4.0`, bypasses that pin.

### Old CLIs are refused

This queue refuses any host that announces plugin protocol below `1.1.0` at `initialize`, which covers every 0.6.x CLI. The refusal happens before the queue files are opened, so they are left untouched. The error says what to do:

> animus-queue-default v0.4.0 requires Animus 0.7 or newer. This Animus is 0.6 or older (plugin protocol 1.0.0). Install the queue version made for it: `animus plugin install launchapp-dev/animus-queue-default@v0.3.3`

`--manifest` still works for any caller.

### Going back from 0.7 to 0.6.x

1. Let running work finish.
2. Stop the daemon.
3. Install the 0.6.x CLI and queue v0.3.3.

Waiting and held tasks carry over: v0.4.0 only adds fields to `queue.json`, and v0.3.3 ignores fields it doesn't know. When v0.3.3 next writes the file, tickets and per-task counters are dropped. Tasks still running at rollback stay `assigned` under v0.3.3, which has no expiry, so finish or drop them. `queue-history.jsonl` is left alone.

Moving forward again needs nothing special: v0.4.0 loads whatever v0.3.3 left. Counters restarting is safe, because a ticket also carries the entry's unique id and the holder's id, which never repeat.

## Methods

- **Ticketed** (used by the 0.7 daemon):
  - `queue/v2/enqueue`, `queue/v2/lease`, `queue/v2/lease/renew`
  - `queue/v2/lease/recover`, `queue/v2/completion`, `queue/v2/release_pending`
- **Old-style:**
  - `queue/enqueue`, `queue/list`, `queue/lease`, `queue/stats`
  - `queue/next_deadline`, `queue/hold`, `queue/release`, `queue/release_pending`
  - `queue/drop`, `queue/reorder`, `queue/mark_assigned`, `queue/completion`
- `health/check`

The plugin advertises `generation_fenced_leases_v1: true` and `max_lease_batch: 5`, the same as the Postgres queues.

Behaviour follows `animus-postgres` v0.2.9 except for seven listed differences; see [the v0.4.0 release notes](docs/releases/v0.4.0.md).

In short:

- **Hand-out:** up to 5 entries per call, in queue order. Held entries and entries that aren't due yet are skipped. An entry whose repository branch is already in use is left waiting.
- **Tickets:** renewing only works while the ticket is valid, and never moves the expiry earlier. A different daemon can take a task over once its ticket has expired. "Done" and "put back" are accepted after expiry as long as nobody took the task over.
- **Ticketed add:** a task that already has a waiting, running or held entry gets that entry back, with a warning. Resending with the same `idempotency_key` returns the original receipt.
- **Old-style calls:** old-style hand-out, mark-assigned and put-back leave ticketed entries alone. `list`, `stats`, `hold`, `release`, `drop`, `reorder` and `next_deadline` work on every entry. `drop` is the manual escape hatch for a stuck running task.

### Ticket length

Tickets last 30 minutes. Set `ANIMUS_QUEUE_LEASE_TTL_SECS` to change that. It takes a whole number of seconds from 1 to 604800 (7 days); any other value falls back to 1800. The plugin declares the variable in its manifest, so the Animus host forwards it. A shorter ticket makes interrupted work resume sooner.

## Deferred dispatch (`run_at`)

An add may carry `run_at` (RFC 3339) and `expire_after_secs`.

- The entry stays `pending` but isn't handed out until `run_at` passes.
- A deferred entry still waiting after `run_at + expire_after_secs` is dropped instead of run late.
- `queue/next_deadline` returns the earliest future `run_at`, so the daemon can wake at exactly that time.
- A malformed `run_at` is an error on the ticketed add. The old-style add treats it as "now".

## Storage

The plugin binds one project root at `initialize` (`init_extensions.project_binding.project_root`) and keeps its state there:

```
<project_root>/.animus/queue.json            waiting, running and held entries, plus per-task counters
<project_root>/.animus/queue.lock            file lock held for each read-modify-write
<project_root>/.animus/queue-history.jsonl   one line per finished entry, kept forever
```

- `queue.json` is replaced atomically (temp file, fsync, rename) and is never deleted, because its counters must survive.
- Finishing an entry appends its history line and fsyncs it before `queue.json` is replaced. A crash in between leaves the entry live; the retry finishes it again, and readers use the first history line per entry.
- The lock covers both files, so any number of queue processes can work on one project at once.

These are runtime state. Add them to your `.gitignore`:

```
.animus/queue.json
.animus/queue.lock
.animus/queue-history.jsonl
```

## Known limitations

- **FIFO only.** Entries are handed out in queue order regardless of `SubjectDispatch::priority` (`priority_weighted: false`).
- **No change stream.** Poll `queue/list`.
- **One project root per process.** Re-binding needs a restart.
- **No Windows build.**

## Build

```bash
cargo build
cargo test
cargo run --release -- --manifest
```
````

Create `docs/releases/v0.4.0.md`:

```markdown
# animus-queue-default v0.4.0

The default queue now supports tickets (generation-fenced leases), which the Animus 0.7 daemon requires. With this release a default 0.7 install can start its daemon.

## Before you upgrade

| Animus CLI | Queue |
|---|---|
| 0.6.x and older | stay on v0.3.3 |
| 0.7 and newer | v0.4.0 or newer |

- v0.4.0 refuses 0.6.x CLIs at `initialize`, before touching any file, with a message giving the install command for v0.3.3: `animus plugin install launchapp-dev/animus-queue-default@v0.3.3`.
- A 0.7 CLI keeps failing its preflight until its own pins move to v0.4.0. That comes in a separate animus-cli release.

## What's new

- The six ticketed methods: `queue/v2/enqueue`, `queue/v2/lease`, `queue/v2/lease/renew`, `queue/v2/lease/recover`, `queue/v2/completion` and `queue/v2/release_pending`.
- Capabilities `generation_fenced_leases_v1: true` and `max_lease_batch: 5`, matching the Postgres queues.
- Ticket length is set with `ANIMUS_QUEUE_LEASE_TTL_SECS` (default 1800, allowed 1 to 604800), declared in the manifest so the host forwards it.
- Finished entries are kept in `.animus/queue-history.jsonl`. It is written before `queue.json`, so a crash between the two loses nothing and duplicates nothing.
- `queue.json` gains a format marker and per-task counters. It is never deleted.
- Old-style `queue/lease` gives entries a 30-minute expiry. Expired old-style entries are handed out again unless the caller lists them in `exclude_subjects`, as in `animus-postgres` v0.2.9.
- `queue/list` defaults to 500 entries and returns at most 2000.
- Old-style adds without a subject are rejected, because they would make the file unreadable for v0.3.3.

## Upgrading and rolling back

- **Upgrading:** v0.4.0 reads a v0.3.3 `queue.json` as it is.
  - Waiting and held entries get ticket identity the first time they are handed out.
  - Running entries stay old-style; the old-style "done", or `drop`, finishes them.
- **Rolling back to 0.6.x:** let running work finish, stop the daemon, then install the 0.6.x CLI and queue v0.3.3.
  - Waiting and held tasks carry over.
  - Tickets and counters are dropped when v0.3.3 next writes the file.
  - `queue-history.jsonl` is left alone.

Add `.animus/queue.json`, `.animus/queue.lock` and `.animus/queue-history.jsonl` to `.gitignore`.

## Behaviour compared with animus-postgres v0.2.9

v0.4.0 behaves like `animus-postgres` v0.2.9 except in these seven places:

1. **"Done" or "put back" with an expired ticket** is accepted if nobody took the task over. v0.2.9 refuses it as stale. The 0.7 daemon renews tickets only while it has a free slot, and after a restart it finishes runs with their stored ticket, so refusing would leave tasks stuck.
2. **A resent ticketed add** with the same `idempotency_key` ignores `dispatch.requested_at` when comparing content. The CLI stamps a new time on every attempt, and it promises that identical retries return the original receipt.
3. **A ticketed add for a task that already has a waiting, running or held entry** returns that entry with a warning, as `animus-queue-postgres` v0.2.0 does. v0.2.9 adds a second entry. So a "run later" add for a task that is still waiting or running doesn't queue a second run.
4. **Old-style hand-out, mark-assigned and put-back** leave ticketed entries alone. A ticketed entry gives mark-assigned and put-back error `-32209`.
5. **Old entries get ticket identity** at their first ticketed hand-out, not at plugin start.
6. **The ticket length setting** is declared in the manifest, so it takes effect. v0.2.9 reads it but never declares it, so it always uses 30 minutes.
7. **A malformed `run_at` on a ticketed add** is an error. v0.2.9 treats it as "now".

These match v0.2.9 on purpose:

- **Expired tickets** are never handed out fresh. They come back only through `queue/v2/lease/recover`, and the hand-out doesn't list them as blocked.
- **Renewing** keeps the generation. It moves the expiry to "now + length" but never earlier than it already is, which the 0.7 daemon requires.
- **Tickets are matched** on their owner and numbers, not their expiry time.
- **A repeated "done"** is acknowledged, and the first outcome is kept.
- **"Done" for a task an operator dropped** returns `not_assigned`.
- **Old-style "done"** is accepted for any running entry.
- **A second copy of a task** can be handed out while one runs, and the daemon hands it back. Difference 3 stops new copies from ticketed adds, so these come only from old files or old-style adds.
- **Each hand-out looks at up to 50 waiting entries** (10 × `max` when that's larger).
```

Replace the whole of `src/lib.rs` with:

````rust
//! Animus default `queue` plugin: a file-backed dispatch queue with
//! generation-fenced ("ticketed") leases for the Animus 0.7 daemon, plus the
//! old-style `queue/*` methods. Behaviour follows animus-postgres v0.2.9
//! except for the seven differences in `docs/releases/v0.4.0.md`.
//!
//! Project root is bound at `initialize` time via the
//! `init_extensions.project_binding` extension; it is NOT a per-request
//! field. RPCs that imply a different project root than the bound one are
//! rejected with [`animus_queue_protocol::error_codes::PROJECT_BINDING_MISMATCH`].
//!
//! State and lock layout under the bound project root:
//!
//! ```text
//! <project_root>/.animus/queue.json            # live entries + counters (atomic replace)
//! <project_root>/.animus/queue.lock            # fs2 exclusive-lock file
//! <project_root>/.animus/queue-history.jsonl   # finished entries (append + fsync)
//! ```
//!
//! The lock is held only across read-modify-write cycles, never across
//! IPC.

#![warn(missing_docs)]

pub mod dispatch_queue_state;
pub mod dispatch_queue_store;
pub mod fenced_queue;
pub mod host_guard;
pub mod identity;
pub mod lease_ttl;
pub mod plugin;
pub mod queue_history;
pub mod queue_service;
pub mod request_hash;

pub use dispatch_queue_state::{
    DispatchQueueAuditEntry, DispatchQueueEntry, DispatchQueueEntryStatus, DispatchQueueState,
};
pub use dispatch_queue_store::{
    load_queue_state, queue_lock_path, queue_state_path, save_queue_state,
};
pub use queue_service::QueueBackend;
````

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo doc --no-deps`

Expected: no warnings.

- [ ] **Step 5: Gates and codex review**

Run the gates (spec §8.5):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-features
```

Expected: no output from `fmt`, no clippy warnings, every test binary reports `ok`.

Then stage and self-review with codex (repo rule):

```bash
git add -A
source ~/.claude/skills/gstack/bin/gstack-codex-probe 2>/dev/null
timeout 540 codex review --uncommitted -c 'model_reasoning_effort="high"' --enable web_search_cached < /dev/null
```

Fix every `[P1]` and re-run until none remain. Fix a `[P2]` inline if it is about 10 lines; otherwise leave `// TODO(codex-p2):` and list it in your report.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "docs: README and v0.4.0 release notes"
```

No `Co-Authored-By`, `Claude-Session` or "Generated with" lines.

---

### Task 14: End-to-end checks, old-version checks, and the PR

Spec §8.3, §8.4 and §4. Nothing here changes code unless a check fails; if one fails, stop, debug it (superpowers:systematic-debugging), fix it in the task it belongs to with a new commit, and re-run.

**Files:**
- None, unless a check fails.

**Interfaces:**
- Consumes: the release binary `target/release/animus-queue-default` from this branch.

- [ ] **Step 1: Build both sides**

```bash
cd ~/Animus_projects/animus-queue-default && cargo build --release
cd ~/Animus_projects/animus-cli && cargo build --release -p orchestrator-cli
```

Expected: both finish. The CLI binary is `~/Animus_projects/animus-cli/target/release/animus` (version `0.7.0-rc.52` or later). Don't commit anything in `animus-cli`, which has the owner's uncommitted work.

- [ ] **Step 2: Isolated environment**

```bash
export E2E=$(mktemp -d)
export HOME=$E2E/home && mkdir -p "$HOME"
export ANIMUS=~/Animus_projects/animus-cli/target/release/animus
export P=$E2E/project && mkdir -p "$P" && cd "$P" && git init -q && git commit -q --allow-empty -m init
"$ANIMUS" plugin install-defaults --include-subjects
"$ANIMUS" plugin install --path ~/Animus_projects/animus-queue-default/target/release/animus-queue-default --force
"$ANIMUS" plugin install launchapp-dev/animus-workflow-runner-default@v0.4.74 --force
"$ANIMUS" daemon preflight --project-root "$P"
```

Expected: the preflight passes. Before the queue install it would have failed on `generation_fenced_leases_v1`. The runner must be v0.4.69 or newer; the CLI's pin is fixed in part 2. Every later command in this task runs in this shell. Check with `echo $HOME` that you are not using the real home directory.

- [ ] **Step 3: A workflow that runs only a shell command (no AI, no cost)**

Write `$P/.animus/workflows.yaml`:

```yaml
phases:
  stand-in-work:
    mode: command
    directive: Stand-in work for the queue end-to-end test
    command:
      program: sh
      args: ["-c", "sleep ${E2E_SLEEP:-5}; echo done"]
      cwd_mode: task_root
      timeout_secs: 900
      success_exit_codes: [0]

workflows:
  - id: e2e-shell
    name: E2E shell only
    phases:
      - stand-in-work
```

If `animus workflow` validation rejects a field, load the `animus-workflow-authoring` skill and fix the YAML. The only goal is a phase that runs `sh`.

- [ ] **Step 4: The basic path**

```bash
"$ANIMUS" subject create --kind task --title "e2e one" --project-root "$P"      # note the TASK id
"$ANIMUS" queue enqueue --task-id <TASK-ID> --workflow-ref e2e-shell --project-root "$P"
"$ANIMUS" daemon start --project-root "$P"
"$ANIMUS" queue list --project-root "$P"
cat "$P/.animus/queue.json"; cat "$P/.animus/queue-history.jsonl"
```

Expected:
- The entry goes `pending` to `assigned`, then disappears from `queue.json`.
- `queue.json` shows `format_version: 2` and the counter `task:<TASK-ID>: 1`.
- One `completed` line appears in `queue-history.jsonl`.
- `animus daemon stream --pretty` shows no queue errors.

If the CLI's enqueue flags differ, load `animus-queue-management` before retrying.

- [ ] **Step 5: The hard cases**

Run each case and write down what happened for the PR description:

1. **Restart mid-run:** `E2E_SLEEP=60`, enqueue, then `daemon stop` while it runs, then `daemon start`. The run finishes and the entry closes. No `stale_fence` for a run nobody took over.
2. **Ticket expiry:** stop the daemon, restart it with `ANIMUS_QUEUE_LEASE_TTL_SECS=20` and `E2E_SLEEP=90`. The run completes. The entry ends in history as `completed`, not stuck `assigned`.
3. **Dropping a running task:** `animus queue drop <entry-id>` during a run. The daemon logs a `not_assigned` warning when the run ends. Nothing crashes, and the next task still runs.
4. **Queuing the same task twice:** the second enqueue returns the same entry with the warning `subject task:<id> already has an active generation; enqueue rejected`.
5. **A run that outlives its ticket while every slot is busy** (difference 1): `ANIMUS_QUEUE_LEASE_TTL_SECS=20`, enqueue at least as many tasks as the daemon's pool size with `E2E_SLEEP=90`. Every run's "done" is `applied`, and nothing is left `assigned`.

- [ ] **Step 6: Old CLI refused (installed 0.6.33, isolated HOME)**

```bash
export HOME=$E2E/old-home && mkdir -p "$HOME"
export OLD=~/.local/bin/animus && "$OLD" --version          # 0.6.33
export Q=$E2E/old-project && mkdir -p "$Q" && cd "$Q" && git init -q && git commit -q --allow-empty -m init
"$OLD" plugin install-defaults
"$OLD" queue enqueue --task-id TASK-1 --project-root "$Q"    # writes a v0.3.3 queue.json
shasum "$Q/.animus/queue.json" > "$E2E/before.sha"
"$OLD" plugin install --path ~/Animus_projects/animus-queue-default/target/release/animus-queue-default --force
"$OLD" queue list --project-root "$Q"
shasum -c "$E2E/before.sha"
```

Expected:
- `queue list` fails with the Global Constraints message, showing `(plugin protocol 1.0.0)`.
- `shasum -c` prints `OK`: the file is untouched.

Then run the command from the message:

```bash
"$OLD" plugin install launchapp-dev/animus-queue-default@v0.3.3 --force
"$OLD" queue list --project-root "$Q"
```

Expected: the list works again. Use the exact working command in the README and release notes. If it needs `--force` (or anything else) to replace the installed plugin, update the message in `src/host_guard.rs`, both docs and the tests that pin it, in one commit.

- [ ] **Step 7: Rollback with the real 0.6.33**

Copy a `queue.json` written by v0.4.0 in Steps 4–5 (with a held entry: `animus queue hold <entry-id>` first) into `$Q/.animus/`, then run `"$OLD" queue list --project-root "$Q"`.

Expected: waiting and held entries are listed with their statuses.

- [ ] **Step 8: Final gates**

```bash
cd ~/Animus_projects/animus-queue-default
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --all-features && cargo build --release
git status --short   # must be empty
```

- [ ] **Step 9: Push the branch and open the PR**

```bash
git push -u origin feat/fenced-leases
gh pr create --base main --head feat/fenced-leases \
  --title "v0.4.0: generation-fenced (ticketed) leases for Animus 0.7" \
  --body-file <path to a body you write>
```

The body covers:
- what changed
- the compatibility table
- the seven differences
- the five notes from "Notes on reading the spec"
- the Step 4–7 results
- that `main` is one commit behind `v0.3.3`, and this PR carries that commit

No attribution lines of any kind. Do not tag. Report the PR URL to the owner, who reviews and merges. After the merge, the owner has approved switching the GitHub default branch from `v0.1.0-dev` to `main`.
