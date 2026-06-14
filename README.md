# animus-queue-default

Reference `queue` plugin for [Animus](https://github.com/launchapp-dev/animus-protocol) v0.5.

This crate is a lift-and-shift of the in-tree dispatch queue from
`ao-cli/crates/orchestrator-daemon-runtime/src/queue/`, ported to the
`animus-queue-protocol` stdio plugin contract. It adds the v0.5
**`queue/lease`** atomic dispatch path (new — not present in the in-tree
code).

## Scope

The queue plugin owns:

- per-project FIFO state for `SubjectDispatch` envelopes
- file-locked state persistence under `<project_root>/.animus/`
- the 10 `queue/*` methods (enqueue, list, lease, stats, hold, release, drop,
  reorder, mark_assigned, completion)

Capacity / dispatch headroom / active-workflow filtering stays in the
daemon (kernel concern). The plugin just provides ordered access.

## Deferred dispatch (`run_at`)

`queue/enqueue` accepts optional `run_at` (RFC 3339) and
`expire_after_secs` (`animus-queue-protocol` 0.3.1+). When `run_at` is set
and in the future the entry is enqueued as **deferred**: it stays
`pending` but is excluded from `queue/lease` until the instant passes, then
dispatches on the next lease. `expire_after_secs` is a grace window — a
still-pending deferred entry past `run_at + expire_after_secs` is dropped
on the next lease/enqueue sweep instead of dispatched late (`None` = always
fire late). `QueueStats::deferred` counts the not-yet-leasable subset of
`pending`.

Enqueue is **never deduped** (queue-protocol 0.3.2+): both immediate and
deferred enqueues always create a new entry, and a collision with an
existing entry for the same subject is surfaced via
`QueueEnqueueResponse::warning` for the caller to act on. Lease-side
`exclude_subjects` still prevents two entries for the same subject from
running concurrently.

`queue/next_deadline` returns the earliest future `run_at` across pending
deferred entries (or `None`), so the daemon can sleep until exactly that
instant instead of waiting for its heartbeat.

## State / lock layout

The plugin binds a project root at `initialize` time via
`init_extensions.project_binding.project_root`. State and lock files live
under that root:

```
<project_root>/.animus/queue.json
<project_root>/.animus/queue.lock
```

The plugin uses `fs2::FileExt::lock_exclusive()` during state mutations.
The lock is held only across read-modify-write cycles, never across IPC.
Running multiple plugin instances against the same project root produces
undefined behavior; the daemon SHOULD enforce single-plugin-per-project.

## v0.5 known limitations

- **FIFO only.** Pending entries are leased in insertion order regardless
  of `SubjectDispatch::priority`. The protocol carries a
  `QueueCapabilities::priority_weighted: false` flag advertising this.
  Priority-weighted backends are a future v0.6+ concern.
- **No watch/streaming.** Queue change events are not currently published.
  Hosts that need them can poll `queue/list` or rely on `queue/lease`
  return values.
- **Single project root per process.** The plugin process is bound to one
  project root for its lifetime; re-binding would require a restart.

## Build

```bash
cargo build
cargo test
```

## Wire smoke

```bash
cargo run --release -- --manifest
```
