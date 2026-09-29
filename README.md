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

At `initialize`, this queue checks the Animus version the host sends (`host_info.version`) and refuses anything older than 0.7.0. Release candidates such as `0.7.0-rc.52` count as 0.7. The plugin protocol version can't be used for this: every CLI since v0.5.0 announces `1.1.0` on its queue calls, 0.6.x included. One exception is accepted whatever version it sends: plugin protocol `1.2.0` or newer, which only 0.7's generic plugin handshake announces. A missing or unreadable version is refused.

The refusal happens before the queue files are opened, so they are left untouched. The error says what to do:

> animus-queue-default v0.4.0 requires Animus 0.7 or newer. This Animus is 0.6.33 (plugin protocol 1.1.0). Install the queue version made for it: `animus plugin install launchapp-dev/animus-queue-default@v0.3.3 --force`

`--force` is needed because a queue plugin is already installed.

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
- **Ticketed add:** a task that already has a waiting, running or held entry gets that entry back, with a warning. Resending with the same `idempotency_key` returns the original receipt. A key is remembered forever, including when its add got an existing entry back. A running entry left over from Animus 0.6 doesn't count, and the add creates a new entry.
- **Old-style calls:** old-style hand-out, mark-assigned and put-back leave ticketed entries alone. `list`, `stats`, `hold`, `release`, `drop`, `reorder` and `next_deadline` work on every entry. `drop` is the manual escape hatch for a stuck running task.

### Ticket length

Tickets last 30 minutes. Set `ANIMUS_QUEUE_LEASE_TTL_SECS` to change that. It takes a whole number of seconds from 1 to 604800 (7 days); any other value falls back to 1800. The plugin declares the variable in its manifest, so the Animus host forwards it. A shorter ticket makes interrupted work resume sooner.

## Deferred dispatch (`run_at`)

An add may carry `run_at` (RFC 3339) and `expire_after_secs`.

- The entry stays `pending` but isn't handed out until `run_at` passes.
- A deferred entry still waiting after `run_at + expire_after_secs` is dropped instead of run late.
- `queue/next_deadline` returns the earliest future `run_at`, so the daemon can wake at exactly that time.
- A malformed `run_at` is an error on the ticketed add. The old-style add treats it as "now".
- An `expire_after_secs` too large for `run_at + expire_after_secs` to be a valid time is an error on the ticketed add. The old-style add stores it, and the entry never expires.

## Storage

The plugin binds one project root at `initialize` (`init_extensions.project_binding.project_root`) and keeps its state there:

```
<project_root>/.animus/queue.json            waiting, running and held entries, plus per-task counters
<project_root>/.animus/queue.lock            file lock held for each read-modify-write
<project_root>/.animus/queue-history.jsonl   one line per finished entry, kept forever
```

- `queue.json` is replaced atomically (temp file, fsync, rename, then fsync of `.animus/`) and is never deleted, because its counters must survive. Changes survive a machine crash, not just a process crash.
- Finishing an entry appends its history line and fsyncs it before `queue.json` is replaced. A crash in between leaves the entry live, as if the finish never happened: it can still be finished, dropped or taken over. Readers use the last history line per entry, which is always the one that took effect.
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
- **A task can stay "running" if a hand-out reply is lost.** The queue records the hand-out, and then the queue process is killed, or the daemon crashes before saving the ticket. The daemon never learns the ticket, so it can't renew or take over the task. `animus-postgres` has the same gap. The fix belongs in the daemon. Until then, find the task with `animus queue list` (status `assigned`, with no run for it in `animus status`). Remove it with `animus queue drop <task-id>`, which drops every queue entry for that task, then add it again with `animus queue enqueue --subject-id <task-id>`.

## Build

```bash
cargo build
cargo test
cargo run --release -- --manifest
```
