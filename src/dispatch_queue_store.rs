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
//! over `queue.json`, so readers see either the old or the new state. The
//! `.animus` directory is flushed after the rename (and the project root when
//! `.animus` is first created), so the replace also survives a machine crash,
//! not just a process crash.
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

/// Flush a directory's entries to disk (fsync), making a rename or file
/// creation inside it durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .with_context(|| format!("failed to flush directory {}", dir.display()))
}

/// Create `<project_root>/.animus` if it is missing, flushing the project
/// root so the new directory is durable. Returns the directory's path.
pub(crate) fn ensure_state_dir(project_root: &Path) -> Result<PathBuf> {
    let dir = project_root.join(ANIMUS_DIR);
    if !dir.is_dir() {
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create animus state dir at {}", dir.display()))?;
        sync_dir(project_root)?;
    }
    Ok(dir)
}

/// Acquire an exclusive lock on `queue.lock`. Returned guard releases the
/// lock on drop.
///
/// Held only across read-modify-write cycles, never across IPC.
pub(crate) fn acquire_queue_lock(project_root: &Path) -> Result<File> {
    let lock_path = queue_lock_path(project_root);
    ensure_state_dir(project_root)?;
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
    let dir = ensure_state_dir(project_root)?;

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
    sync_dir(&dir)
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

    #[test]
    fn state_dir_is_created_once_and_directories_can_be_flushed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let dir = ensure_state_dir(temp.path()).expect("create");
        assert_eq!(dir, temp.path().join(".animus"));
        assert!(dir.is_dir());
        ensure_state_dir(temp.path()).expect("second call is a no-op");
        sync_dir(&dir).expect("flush");
        assert!(sync_dir(&temp.path().join("missing")).is_err());
    }
}
