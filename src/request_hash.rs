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
