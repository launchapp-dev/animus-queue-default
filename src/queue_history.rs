//! Finished-entry history: `<project_root>/.animus/queue-history.jsonl`.
//!
//! One JSON line per finished entry: completed, failed, cancelled or dropped.
//! Kept forever, like the Postgres queues' finished rows. It is read only
//! when a call doesn't find its entry in `queue.json`: a repeated ticketed
//! "done", or an idempotency key whose entry already finished.
//!
//! Crash safety: finishing an entry appends and fsyncs its history line
//! *before* `queue.json` is replaced. A crash in between leaves the entry
//! live: that finish never happened, as in a rolled-back Postgres
//! transaction, and the entry can still be finished, dropped or taken over.
//! Whatever finishes it later appends another line.
//!
//! Readers therefore take the **last** line per entry. An entry never
//! returns to `queue.json` once it leaves, so its last line is always the one
//! whose `queue.json` replace succeeded; earlier lines are interrupted
//! attempts. Readers also skip lines that don't parse, such as a torn final
//! write.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use animus_queue_protocol::completion_status;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::dispatch_queue_state::DispatchQueueEntry;
use crate::dispatch_queue_store::{ensure_state_dir, sync_dir};

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
    let dir = ensure_state_dir(project_root)?;
    let created = !path.exists();
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
    append().with_context(|| format!("failed to append queue history at {}", path.display()))?;
    if created {
        // Make the new file's directory entry durable too.
        sync_dir(&dir)?;
    }
    Ok(())
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

/// The last (authoritative) history record for `entry_id`.
pub fn find_history_by_entry_id(
    project_root: &Path,
    entry_id: &str,
) -> Result<Option<HistoryRecord>> {
    find_last(project_root, |record| record.entry.entry_id == entry_id)
}

/// The last history record whose entry held idempotency key `key`, as its
/// own key or a later add's.
pub fn find_history_by_idempotency_key(
    project_root: &Path,
    key: &str,
) -> Result<Option<HistoryRecord>> {
    find_last(project_root, |record| {
        record.entry.bound_request_hash(key).is_some()
    })
}

fn find_last(
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
    let mut found = None;
    // Split on raw bytes: a torn line may end inside a UTF-8 sequence.
    for (index, line) in BufReader::new(file).split(b'\n').enumerate() {
        let line =
            line.with_context(|| format!("failed to read queue history at {}", path.display()))?;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<HistoryRecord>(&line) {
            Ok(record) if matches(&record) => found = Some(record),
            Ok(_) => {}
            Err(error) => tracing::warn!(
                path = %path.display(),
                line = index + 1,
                %error,
                "skipping unreadable queue history line"
            ),
        }
    }
    Ok(found)
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
    fn last_record_per_entry_wins() {
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

        // The first line is an interrupted attempt; the last one committed.
        let found = find_history_by_entry_id(temp.path(), "e1")
            .unwrap()
            .unwrap();
        assert_eq!(found.outcome, HistoryOutcome::Failed);
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
