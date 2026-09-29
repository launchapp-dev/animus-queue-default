# animus-queue-default v0.4.0: ticketed queue (generation-fenced leases)

- **Date:** 2026-09-28
- **Status:** design agreed in discussion; this document awaits review
- **Repo:** `launchapp-dev/animus-queue-default`, branch `feat/fenced-leases` (from tag `v0.3.3`)
- **Part of:** a three-part fix for "a default install can't start the 0.7 daemon".
  - This spec is part 1, the queue.
  - Part 2 (animus-cli pins, preflight, CI, docs) and part 3 (dashboard) get their own specs.

## 1. Problem

From Animus 0.7.0-rc.34, the daemon only works with a queue that supports tickets ("generation-fenced leases").

- **When it checks:** at startup, and again on every ticketed call (`plugin_clients.rs`), so `--skip-preflight` doesn't get round it.
- **What it needs:**
  - the `generation_fenced_leases_v1` flag
  - a batch size of at least 5
  - six ticketed methods
- **Why a default install fails:** the queue it installs, `animus-queue-default` v0.3.3, has none of these, so a fresh 0.7 install refuses to start.
- **Who already supports tickets:** both Postgres queues.
  - `animus-postgres` v0.2.9 is the reference for this work.
  - `animus-queue-postgres` v0.2.0 is the other.

**What tickets are for.** When the daemon takes a task, the queue gives it a ticket. The ticket says three things:

- which daemon holds the task (owner)
- a counter (generation)
- when the ticket expires

Every later call about that task must show the ticket: "still working", "done" or "put it back". If a daemon crashes and another daemon takes the task over, the counter moves on. The first daemon's late "done" is then refused, so it can't close a task someone else is now running.

Tickets last 30 minutes. The holder renews its ticket while it works. After the ticket expires, a different daemon may take the task over.

## 2. Goal and scope

**Goal:** the default queue gets the same capabilities as `animus-postgres` v0.2.9 and behaves like it everywhere, except in the seven places listed in §7.2.

**In scope:** everything in this repo needed to release v0.4.0:

- ticket support
- the storage changes that support it
- the old-CLI guard (§3)
- tests
- the README and release notes

**Out of scope, with owners:**

- **Part 2, animus-cli:**
  - Raise the install pins to queue v0.4.0 and a fenced runner.
  - Make the two pin lists agree.
  - Check capabilities in `daemon preflight`.
  - Add a CI compatibility check.
  - Fix stale docs.
  - Publish a new CLI release marked Latest.
- **Part 3:** the web dashboard.
- **Others:** §10 lists the remaining follow-ups.

## 3. Which CLI uses which queue

| Animus CLI | Queue | Why |
|---|---|---|
| 0.6.x and older (v0.4.0 – v0.6.33) | `animus-queue-default` v0.3.3 or older | Old daemons don't use tickets |
| 0.7 and newer | `animus-queue-default` v0.4.0 or newer | The 0.7 daemon requires tickets |

### 3.1 How 0.6.x CLIs stay on v0.3.3

**Already safe, with no changes.** A 0.6.x CLI has v0.3.3 built in as its pin (v0.6.33 `plugin_registry.rs:37` and `default-install.json`). Every install route below uses that pin:

- `plugin install-defaults`
- `daemon start --auto-install`
- flavor install
- `plugin outdated`
- `plugin update`, which never downgrades a newer install unless forced

**The gap:** some routes skip the pin.

- `animus plugin install launchapp-dev/animus-queue-default` with no tag downloads GitHub's "Latest" release (v0.6.33 `ops_plugin.rs:4047-4052`). Once v0.4.0 is published, that is v0.4.0.
- The same happens with an explicit `@v0.4.0`, or anything else that downloads "latest" directly.

0.6.x CLIs don't reject newer plugins themselves. They accept any plugin whose protocol major version is 1 (`check_protocol_compat` in v0.6.33 `host.rs`).

### 3.2 The guard

**How the queue tells hosts apart.** At `initialize`, the queue reads the host's `protocol_version`:

| CLI releases | `protocol_version` sent |
|---|---|
| v0.4.0 – v0.6.33 | `1.0.0` |
| First 0.7 pre-releases (rc.1 – rc.8) | `1.1.0` |
| rc.9 – rc.52 | `1.2.0` |

Each tag was checked individually.

**Rule:** if the host's `protocol_version` is missing, unparseable, or below `1.1.0` (compared as semantic versions), `initialize` fails with an error.

- The refusal happens before any file access, so a refused host never reads or changes the queue files.
- `--manifest` still works, because printing the manifest doesn't go through `initialize`.

**Message.** The final recovery command is confirmed in testing (§8.4):

> animus-queue-default v0.4.0 requires Animus 0.7 or newer. This Animus is 0.6 or older (plugin protocol 1.0.0). Install the queue version made for it: `animus plugin install launchapp-dev/animus-queue-default@v0.3.3`

**Where it shows:** the daemon starts a fresh queue process for every call. So the message appears wherever that Animus talks to the queue: `animus queue …` commands, plugin checks and daemon logs.

**Why not use the host's version string:** `host_info.version` is the plugin-host crate's own version, `0.1.0`, in both 0.6.33 and 0.7. It can't tell them apart.

### 3.3 Going back from 0.7 to 0.6.x

**Why the file stays readable.** v0.4.0 keeps v0.3.3's file layout:

- the same top-level `entries` list
- the same entry fields
- the same status words: `pending`, `assigned`, `held`

It only adds fields. v0.3.3 ignores fields it doesn't know. An unknown status word, however, would become a stuck `unknown` entry, so status words must not change.

The stored task request stays compatible too. The newer format only adds the optional `actor` field. Every stored entry has a subject, because the queue rejects entries without one.

**Procedure:**

1. Let running work finish.
2. Stop the daemon.
3. Install the 0.6.x CLI and queue v0.3.3.

Waiting and held tasks carry over.

**What's lost:**

- Tickets, per-task counters and the version marker. v0.3.3 drops them the next time it writes the file.
- Any tasks still running at rollback. They stay `assigned` under v0.3.3, which has no expiry, so finish or drop them.

`queue-history.jsonl` is left alone.

**Moving forward again later** needs nothing special: v0.4.0 loads whatever v0.3.3 left behind (§6.4). Counters restarting from zero is safe, because every ticket also carries two IDs that can't repeat:

- the entry's unique ID
- the holder's ID, which is new each time a daemon starts

So an old ticket can never match a new entry.

This replaces the earlier plan to call downgrades unsupported.

### 3.4 A 0.7 CLI with queue v0.3.3

This still fails preflight until the CLI's pins move to v0.4.0 in part 2. Release order:

1. queue v0.4.0
2. a CLI release with the new pins

## 4. Branch, release, repository

- **Branch:** `feat/fenced-leases`, cut from tag `v0.3.3`, the tip of `v0.3.0-exclude-subjects`.
  - The PR goes into `main`.
  - `main` is one commit behind `v0.3.3`, and the PR carries that commit.
- **Version:** `0.4.0`.
  - The `v0.4.0` tag is pushed only on the owner's go.
  - The existing release workflow then builds and cosign-signs four targets: Linux and macOS, each on x86_64 and arm64.
- **After the merge:** switch the GitHub default branch from `v0.1.0-dev` to `main` (approved).

## 5. Protocol and handshake

**Dependencies.** `animus-plugin-protocol`, `animus-queue-protocol` and `animus-subject-protocol` move from animus-protocol tag `v0.5.10` to `v0.7.0-rc.43`.

- That is the version the CLI uses.
- The queue types are unchanged from rc.14 to rc.44.
- Add `animus-execution-protocol` at the same tag if the fence types are needed directly.

**`initialize` reply.** It is identical to both Postgres queues: `animus-postgres` v0.2.9 `index.ts:133-140`, and `animus-queue-postgres` v0.2.0 `plugin.rs:327-336`.

```json
"kind_capabilities": { "queue": { "extra": {
  "priority_weighted": false,
  "max_lease_batch": 5,
  "generation_fenced_leases_v1": true
} } }
```

**Methods:**

- **Old-style:**
  - `queue/enqueue`, `queue/list`, `queue/lease`, `queue/stats`
  - `queue/next_deadline`, `queue/hold`, `queue/release`, `queue/release_pending`
  - `queue/drop`, `queue/reorder`, `queue/mark_assigned`, `queue/completion`
- **Health:** `health/check`.
- **Ticketed:**
  - `queue/v2/enqueue`, `queue/v2/lease`, `queue/v2/lease/renew`
  - `queue/v2/lease/recover`, `queue/v2/completion`, `queue/v2/release_pending`

**Why the old methods stay.** Animus 0.7 still calls:

- `list`, `stats`, `hold`, `release`, `drop`, `reorder` and `next_deadline`
- the old-style `completion`, from two call sites

No 0.7 code calls the old-style `enqueue`, `lease`, `mark_assigned` or `release_pending` (zero call sites). They stay for parity with v0.2.9 and for callers outside Animus.

**Manifest.** `env_required` declares `ANIMUS_QUEUE_LEASE_TTL_SECS` with `required: false` and `sensitive: false`. The host forwards only declared variables, so this is what lets the setting reach the plugin (difference 6).

## 6. Storage

### 6.1 Files

**`<project>/.animus/queue.json`** holds live work only: waiting, running and held entries.

- **Also stored:**
  - a format version marker
  - each task's highest generation so far, which never decreases
- **Writing:** atomically under `.animus/queue.lock`, as today: temp file, fsync, rename, then fsync of the `.animus` directory so the rename survives a machine crash. Creating `.animus` fsyncs the project root; creating the history file fsyncs `.animus`.
- **Not deleted when empty,** because the counters must survive.
- **Ticket fields** on new-style entries:
  - subject and subject generation
  - repository and branch
  - workflow generation
  - lease owner, lease generation and lease expiry

**`<project>/.animus/queue-history.jsonl`** holds one line per finished entry: completed, failed, cancelled or dropped.

- It is kept forever, like the Postgres queues' rows.
- It is read only when a retry doesn't find its entry in the live file.
- It grows by about 400 bytes per finished run.

### 6.2 Crash safety

When a task finishes:

1. Append the history line and flush it (fsync).
2. Atomically replace `queue.json`.

If the process crashes between the two, the entry is still live, as if the finish never happened. It behaves like any live entry: the daemon's retry finishes it again, an operator can drop it, and once its ticket expires another daemon can take it over and finish it.

That can leave more than one history line for an entry. Readers take the **last** line per `entry_id`. An entry never returns to `queue.json` once it leaves, so the last line is always the one that took effect; earlier lines belong to finishes that never completed.

The idempotency check of a ticketed add also looks at entries the expiry sweep removed in the same call, because their history lines aren't readable until the call commits.

### 6.3 Concurrency

The daemon starts a fresh queue process for every call. The single file lock around every read-modify-write stays, and it covers both files.

### 6.4 Upgrading a v0.3.3 file

- The file loads as it is. A file without the version marker is treated as the v0.3.3 format, and its per-task counters start at zero.
- Waiting and held entries stay old-style until their first ticketed hand-out gives them ticket identity (difference 5).
- Running (`assigned`) entries stay old-style. The old-style "done" finishes them, or an operator can drop them. They don't block a ticketed add for the same task (§7.2, difference 3).

### 6.5 Task IDs

IDs are normalised to `<kind>:<id>`, following v0.2.9's `canonicalSubjectId` (`queue.ts:166`):

- `animus.task` becomes `task`, and `animus.requirement` becomes `requirement`.
- Both parts are trimmed.
- The kind prefix is added only once: an ID that already starts with `<kind>:` (in any case) isn't prefixed again, so `task:task:X` never appears.
- An empty kind or ID is an invalid-params error.

## 7. Behaviour

### 7.1 Baseline: v0.2.9

Anything not listed in §7.2 behaves like `animus-postgres` v0.2.9's `src/queue.ts`.

**Add, ticketed (`queue/v2/enqueue`):**

- **Same idempotency key and same content:** returns the original receipt and adds nothing, however long ago the key was first sent. A key stays bound to its entry for good, including a key whose add got an existing entry back (difference 3).
- **Same key, different content:** error.
- **Task already waiting, running or held:** returns the existing entry with a warning (difference 3). A running old-style entry doesn't count.
- **Malformed `run_at`:** error (difference 7).
- **`expire_after_secs` too large:** error when `run_at + expire_after_secs` isn't a valid time. v0.2.9 can't store such a value either; its add fails in the database.
- **Otherwise:** a new entry with the task's next generation and a normalised ID.

**Add, old-style:** as v0.2.9, and as v0.3.3 does today. It always adds a new entry and warns about duplicates. Like v0.2.9, it looks for duplicates by the old-style key (`TASK-1`), so it doesn't notice a ticketed copy (`task:TASK-1`). An expiry too large to hold is stored and means the entry never expires; it must never break later calls.

**Hand out, ticketed (`queue/v2/lease`):**

- **Batch:** up to 5 per call, in queue order, skipping held entries and entries that aren't due yet.
- **Skips an entry whose git branch collides** with a running entry or with one of the caller's active tickets. The collision key is `lowercase(trim(repo)) + "\n" + head_ref`.
- **Expired tickets:** never handed out fresh, and not listed in the hand-out's `blocked` list. They come back only through a takeover (`queue/v2/lease/recover`). v0.2.9 defines the reason `expired_lease_recovery_required`, but its hand-out only looks at waiting entries, so it never reports one; see §7.3.
- **Old-style waiting entries** get ticket identity at this point (difference 5).
- **Second copies:** a second copy of a task can be handed out while another copy is running; the daemon hands it back.

**Tickets:**

- **Renew:** only while the ticket is valid. The generation stays the same and the expiry moves later, never earlier.
- **Take over (recover):** only after expiry, and only by a different owner. The generation goes up by exactly one.
- **Matching:** a ticket is matched on its owner and numbers, not its exact expiry time.
- **Length:** 30 minutes by default, set with `ANIMUS_QUEUE_LEASE_TTL_SECS` (difference 6). A per-call `ttl_secs` is honoured as in v0.2.9.

**Finish and put back, ticketed:**

- **Expired ticket:** accepted if nobody took the task over (difference 1).
- **Repeated "done":** acknowledged (`AlreadyApplied`), and the first outcome is kept.
- **Put back:** the task returns to waiting and keeps its run ID. The next hand-out raises the generation.
- **"Done" for a task an operator dropped:** `NotAssigned`.

**Old-style calls:**

- **`list`, `stats`, `hold`, `release`, `drop`, `reorder`, `next_deadline`:** work on every entry, including ticketed ones. `drop` also works on running tasks, as the manual escape hatch.
- **Old-style hand-out, mark-assigned and put-back:** touch only old-style entries (difference 4).
- **Old-style hand-out:**
  - It gives entries a 30-minute expiry.
  - Expired old-style entries are handed out again, unless the caller lists the task in `exclude_subjects`.
- **Old-style "done":** accepted for any entry.

**Errors:**

- Bad input gets a JSON-RPC error with a clear message.
- Ticket problems come back as normal outcomes, never as failures: `Applied`, `AlreadyApplied`, `NotFound`, `StaleFence`, `LeaseStillLive` or `NotAssigned`.

### 7.2 The seven differences from v0.2.9

**1. "Done" or "put back" with an expired ticket**

- **v0.2.9:** refused as stale, with "queue lease has expired and requires recovery" (`queue.ts:1089`; its test is at `chat.test.ts:690`).
- **v0.4.0:** accepted if the ticket's owner and numbers still match, meaning nobody took the task over.
- **Why:**
  - The daemon renews tickets only while it has a free slot. Renewal lives in `dispatch_ready_tasks`, which is skipped when the pool is full, draining or spend-latched (`run_project_tick.rs:101-104`, `project_tick_plan.rs:19`). So long runs routinely outlive their ticket.
  - After a restart, the daemon also finishes runs with their stored ticket without taking them over first (`daemon_run.rs:1015-1068`).
  - Refusing in either case leaves tasks stuck.

**2. A resent add with the same idempotency key**

- **v0.2.9:** the content hash includes `dispatch.requested_at` (`queue.ts:407`), so a resend with a new timestamp is rejected.
- **v0.4.0:** the hash ignores `dispatch.requested_at`.
- **Why:** the CLI stamps `requested_at` with the current time on every attempt (`ops_queue.rs:80-85`). Its help text promises that identical retries return the original receipt (`queue_types.rs:55-61`).

**3. Adding a task that already has a live entry**

- **v0.2.9:** adds another entry and warns (`queue.ts:483`).
- **v0.4.0:** returns the existing entry with a warning.
- **Why:** the owner's choice, and it matches `animus-queue-postgres` v0.2.0 (`store/mod.rs:370-450`).
- **Consequence:** a "run later" add for a task that is still waiting or running doesn't queue a second run.
- **The add's idempotency key is bound to the entry it gets back,** with the add's own content hash. v0.2.9 binds every key it accepts to the entry it created, so retrying a key never starts a second run. queue-postgres v0.2.0 forgets such a key; the owner chose to keep v0.2.9's promise.
- **A running old-style entry doesn't count.** v0.2.9 gives old entries identity only while they wait or are held (`queue.ts:247`). After the upgrade, the 0.6 daemon that ran such an entry is refused by the guard, so returning it would swallow the add for good. The add creates a new entry, as v0.2.9 and queue-postgres v0.2.0 do.

**4. Old-style hand-out, mark-assigned or put-back on ticketed entries**

- **v0.2.9:** allowed. Its old-style hand-out can also pick up expired ticketed rows again.
- **v0.4.0:** these calls touch only old-style entries.
- **Why:** callers without tickets can't disturb ticketed work. No 0.7 code makes these calls, and with the guard, only callers outside Animus could.

**5. When old entries get ticket identity**

- **v0.2.9:** at plugin start (`migrateGenerationIdentity`, `queue.ts:230`).
- **v0.4.0:** at their first ticketed hand-out.
- **Why:** follows from difference 4.

**6. Changing the ticket length**

- **v0.2.9:** reads the setting (`leaseTtlSecs`) but never declares it, so the host never forwards it and tickets always last 30 minutes.
- **v0.4.0:** declared in the manifest, with a default of 1800 seconds.
- **Why:** gives a knob for how fast interrupted work resumes.

**7. A malformed `run_at` on a ticketed add**

- **v0.2.9:** treated as "now" (`parseRunAt` returns null, `queue.ts:335`).
- **v0.4.0:** an error.
- **Why:** failing is safer than running a task early. It matches `animus-queue-postgres` v0.2.0.

### 7.3 Where v0.4.0 deliberately matches v0.2.9

These were each considered and kept as v0.2.9 does them:

- **Old-style hand-outs expire after 30 minutes** and are handed out again, unless the caller excludes them. 0.6.x daemons send `exclude_subjects` for their running tasks, which made this crash recovery for them. Now that the guard refuses 0.6.x hosts, it rarely applies.
- **Old-style "done" is accepted for any entry,** whether or not it names the run. Accepted risk: a late "done" that names no run can close a task that has since been restarted.
- **"Done" for a task an operator dropped gets `NotAssigned`.** The 0.7 daemon then keeps a leftover local record and warns at each restart. The record no longer takes up a slot (TASK-1332).
- **A second copy of a task can be handed out while one runs,** and the daemon hands it back each cycle. Difference 3 stops new duplicates from the ticketed add, so these copies come only from old files or old-style adds.
- **A repeated "done" with a different outcome** is acknowledged, and the first outcome is kept.
- **The old-style add doesn't notice a ticketed copy of the task** (see §7.1). Nothing in Animus 0.7 calls the old-style add.
- **Expired tickets aren't reported by the hand-out.** The 0.7 daemon takes over tasks it has records for. Accepted risk, shared with v0.2.9: if a hand-out reply never reaches the daemon (the queue process is killed after saving, or the daemon crashes before storing the ticket), nobody holds a record, so the entry stays `assigned` until an operator drops it. The README documents the workaround (`animus queue drop <task-id>`, which drops every entry for that task, then enqueue again), and §10 moves the real fix into the daemon.

## 8. Testing and verification

1. **Automated tests in this repo:**
   - The scenarios from both Postgres queues, ported over:
     - adding tasks, resends and conflicts
     - five tasks at once
     - tickets being renewed, expiring and taken over
     - old tickets rejected
     - done, repeated done and put back
     - branch collisions
   - One test for each difference in §7.2, plus checks that §7.1 and §7.3 match v0.2.9.
   - Upgrading from a real v0.3.3 `queue.json`.
   - A crash between the history append and the file replace: nothing is lost or duplicated.
   - A second add's idempotency key replays the entry it got back, before and after that entry finishes.
   - A running old-style entry doesn't block a ticketed add, and keeps its old-style put-back.
   - An `expire_after_secs` too large to hold: an error on the ticketed add; on the old-style add the entry never expires and later calls still work.
   - The same crash followed by a takeover: the new owner's finish stands, its retry is `already_applied` with its own ticket, and the first owner's late "done" is refused.
   - A resent ticketed add whose original entry expired in the same call still returns the original receipt, and a different add with that key is still refused.
   - Many queue processes working on one project at once: no task is ever handed out twice.
   - The guard:
     - `initialize` with protocol `1.0.0`, a missing version or an unparseable one is refused, and the queue files aren't touched.
     - `1.1.0`, `1.2.0` and later `1.x` versions are accepted.
2. **Contract test against the daemon's rules:**
   - Run the real binary and do the handshake.
   - Check every reply with the protocol's strict types (`deny_unknown_fields`).
   - Check the kernel's sanity rules:
     - renewing keeps the generation and never shortens the expiry
     - a takeover raises the generation by exactly one
     - ticket identity comes back byte-identical
     - `FencedQueueEntry::validate` passes
3. **End-to-end test with an isolated `HOME`,** never the real `~/.animus`:
   - Build the 0.7 CLI from source. Install this queue, a fenced runner (v0.4.69 or newer; part 2 fixes the pin) and the other default plugins.
   - Use workflows that run only a shell command, so there's no AI and no cost. Write the sleep lengths into the workflows: the host filters the runner's environment, so a variable set in the shell won't reach the command.
   - **Basics:** preflight passes, the daemon starts, and a queued task is handed out, runs and is marked done.
   - **Hard cases:**
     - a restart mid-run
     - short tickets being renewed (check the expiry moves forward, which also proves the ticket-length setting reached the queue)
     - dropping a running task (the CLI's `queue drop` takes a task id)
     - queuing the same task twice
     - a run that outlives its ticket while all slots are busy, which exercises difference 1. Check the ticket really had expired before the run finished; otherwise the case wasn't exercised
4. **Old-version checks with the installed `animus` 0.6.33,** also with an isolated `HOME`:
   - Queue v0.4.0 refuses with the §3.2 message, and the queue file is unchanged.
   - The recovery command in the message restores v0.3.3, and the queue works again. This fixes the message's final wording.
   - **Rollback:** v0.3.3 lists a queue file written by v0.4.0, with waiting and held tasks intact.
5. **Quality gates:**
   - `cargo fmt`, `clippy` and the repo's CI.
6. **After release (on the owner's go):**
   - All four platforms' binaries and signatures are present.
   - `animus plugin install launchapp-dev/animus-queue-default@v0.4.0` works.

**Not covered:**

- Windows: the queue has never been built for it.
- The Portal: it uses a different queue.

## 9. Documentation

- **README:**
  - the §3 compatibility table
  - the guard and its message
  - the rollback procedure
  - the ticket-length setting
  - both storage files, with a note to add `.animus/queue.json`, `.animus/queue.lock` and `.animus/queue-history.jsonl` to `.gitignore`, since they are runtime state
  - a known limitation: a task can stay running after a lost hand-out reply, with the workaround `animus queue drop <task-id>` and enqueue again
- **v0.4.0 release notes:** the same points, plus the seven differences and the known gap.
- **animus-cli docs:** the same compatibility note, done in part 2.

## 10. Follow-ups outside this spec

- **Part 2 (animus-cli):**
  - Raise the pins, check capabilities in preflight, add the CI check, fix the docs, and publish a Latest release.
  - Page through `queue/list`. The CLI reads one page with the default limit, so with more than 500 matching entries `hold`, `release`, `drop` and `--all` miss the rest. The same happens with `animus-postgres` today. The queue already honours `offset`.
  - Match IDs the same way everywhere. `queue reorder` compares IDs exactly, so `TASK-1` doesn't match the stored `task:TASK-1`; `hold`, `release` and `drop` already accept both forms.
  - Consider renewing tickets even when the pool is full. That is the root cause of difference 1's everyday case.
  - Recover tickets the daemon never received. After a lost hand-out reply the queue has a running entry the daemon has no record of. Two options: the daemon records its intent before asking for a hand-out and reconciles afterwards, or the queue lists expired tickets (for example as `expired_lease_recovery_required`) and the daemon takes them over. Either needs a daemon change, and would apply to `animus-postgres` too.
- **Runner:** optionally add the same guard to `animus-workflow-runner-default`.
  - Newer runners are already public; v0.4.74 has been GitHub's "Latest" since 2026-09-07.
  - A quick read suggests they tolerate 0.6.x hosts, since the fence is optional, but this isn't verified end to end.
- **`animus-postgres` (the Portal):** the expired-ticket refusal (difference 1) and the timestamp in the idempotency hash (difference 2) probably affect it too.
- **`animus-protocol`:** optionally document the lenient completion rule, so future queues follow it.
- **The owner's projects:** `animus-media-team` and `degree-sight` pin queue v0.3.3 and older runners. Raise their pins when they move to 0.7.

## Appendix: reference points

**Kernel (animus-cli `main`):**

- `plugin_clients.rs`: the capability gates, re-checked on every ticketed call.
- `daemon_run.rs:981-1068`: startup reconciliation, which finishes runs with their stored ticket.
- `daemon_tick_executor.rs:452-472`: the renewal heartbeat. Line 276: ticketed completion.
- `run_project_tick.rs:101-104` and `project_tick_plan.rs:19`: dispatch, and with it renewal, is skipped at zero free capacity.
- `coding_scheduler.rs`: five slots, and TASK-1332 (expired local leases don't use slots).
- `ops_queue.rs:80-85` and `queue_types.rs:55-61`: the retry timestamp and the retry promise.
- Old-style completion call sites:
  - `project_terminal_workflow_result.rs:97`
  - `reconcile_completed_processes.rs:207`

**`animus-postgres` v0.2.9, `src/queue.ts`:**

| Line | What |
|---|---|
| 162 | `requestHash` |
| 166 | `canonicalSubjectId` |
| 230 | `migrateGenerationIdentity` |
| 335 | `parseRunAt` |
| 407 | hash input |
| 483 | duplicate warning |
| 561, 585 | old-style lease expiry |
| 702 | `markAssigned` |
| 1057 | `not_assigned` |
| 1089 | expired ticket refused as stale |

Also `index.ts:133-140` (capabilities) and `chat.test.ts:690`.

**`animus-queue-postgres` v0.2.0 (`7bc9211`):**

- `store/mod.rs`:
  - line 57: live states
  - lines 370-450: idempotency and the live-duplicate rule
  - line 505: ownership collision
- `plugin.rs`: lines 195-217 and 327-336, the methods and capabilities.

**Protocol (animus-protocol `v0.7.0-rc.43`):**

- `animus-queue-protocol` 0.4.0.
- `ExecutionFence` (`animus.execution-fence.v1`) carries `workflow_id`, `workflow_generation`, `subject`, `queue_lease` and `repository`.
- `QueueLeaseFence` carries `entry_id`, `owner_id`, `generation` and `expires_at`.

**Animus v0.6.33:**

- `plugin_registry.rs:37`: the queue pin.
- `ops_plugin.rs:4047-4052`: an install with no tag gets GitHub's latest release.
- `host.rs`: `check_protocol_compat` checks the major version only.
- `daemon_task_dispatch.rs:17-30`: `exclude_subjects`.
