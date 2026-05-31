//! File-locked persistence for the queue state.
//!
//! Layout under the bound project root:
//!
//! ```text
//! <project_root>/.animus/queue.json
//! <project_root>/.animus/queue.lock
//! ```
//!
//! Writes use a tempfile + rename to make state updates atomic with respect
//! to readers. Mutations hold an exclusive `fs2` lock across the
//! read-modify-write cycle.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use fs2::FileExt;
use uuid::Uuid;

use crate::dispatch_queue_state::{DispatchQueueEntry, DispatchQueueState};

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
        // Treat both "file never existed" and "file disappeared between an
        // existence check and the read" as `Ok(None)`. The stdio loop handles
        // requests concurrently, so a `queue/list` racing with the
        // `save_queue_state` path that removes the file when the queue is
        // empty can otherwise produce a spurious internal error for a
        // perfectly valid empty queue.
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
            serde_json::from_str::<Vec<DispatchQueueEntry>>(&content)
                .map(|entries| DispatchQueueState { entries })
        })
        .with_context(|| format!("failed to parse queue state file at {}", path.display()))?;

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

/// Persist the queue state to disk. Removes the file entirely when the state
/// is empty.
pub fn save_queue_state(project_root: &Path, state: &DispatchQueueState) -> Result<()> {
    let path = queue_state_path(project_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create animus state dir at {}", parent.display())
        })?;
    }

    if state.entries.is_empty() {
        if path.exists() {
            fs::remove_file(&path).with_context(|| {
                format!("failed to remove empty queue state at {}", path.display())
            })?;
        }
        return Ok(());
    }

    let payload = serde_json::to_string_pretty(state).context("failed to serialize queue state")?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(QUEUE_STATE_FILE);
    let tmp_path = path.with_file_name(format!("{}.{}.tmp", file_name, Uuid::new_v4()));
    fs::write(&tmp_path, payload).with_context(|| {
        format!(
            "failed to write temporary queue state at {}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, &path)
        .with_context(|| format!("failed to publish queue state to {}", path.display()))?;
    Ok(())
}
