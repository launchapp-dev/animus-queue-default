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
