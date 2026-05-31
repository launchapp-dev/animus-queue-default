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
