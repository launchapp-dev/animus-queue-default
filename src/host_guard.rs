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
