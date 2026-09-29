//! Refuse hosts that predate generation-fenced queue leases.
//!
//! Animus 0.6.x and older never use tickets, so they must stay on
//! animus-queue-default v0.3.3. The plugin protocol version can't tell them
//! apart from 0.7: every CLI since v0.5.0 announces `1.1.0` on its queue
//! calls (`plugin_clients.rs` hard-codes it). What differs is
//! `host_info.version`, which those calls fill with the CLI's own version:
//! `0.6.33` against `0.7.0-rc.52`.
//!
//! 0.7 also starts plugins through its generic plugin-host handshake, which
//! sends the host crate's version (`0.1.0`) with protocol `1.2.0`. No 0.6.x
//! host announces `1.2.0`, so that protocol is accepted on its own.

use animus_plugin_protocol::{error_codes, RpcError};
use semver::Version;
use serde_json::json;

/// Oldest Animus version this queue accepts. Its release candidates
/// (`0.7.0-rc.N`) count as 0.7 too: the check compares against `0.7.0-0`.
pub const MIN_HOST_VERSION: Version = Version::new(0, 7, 0);

/// Oldest host plugin protocol this queue accepts at all.
pub const MIN_HOST_PROTOCOL_VERSION: Version = Version::new(1, 1, 0);

/// Host protocol that is accepted whatever `host_info.version` says. Only
/// 0.7's generic plugin-host handshake sends it, with `host_info.version`
/// `0.1.0`.
pub const HOST_PROTOCOL_WITHOUT_VERSION_CHECK: Version = Version::new(1, 2, 0);

/// Queue release that 0.6.x and older hosts should install instead.
pub const LEGACY_QUEUE_VERSION: &str = "v0.3.3";

fn parse(raw: Option<&str>) -> Option<Version> {
    raw.and_then(|value| Version::parse(value.trim()).ok())
}

/// [`MIN_HOST_VERSION`] with the lowest pre-release, `0.7.0-0`, so every
/// 0.7.0 pre-release compares as new enough.
fn minimum_host_version() -> Version {
    let mut minimum = MIN_HOST_VERSION;
    minimum.pre = semver::Prerelease::new("0").expect("`0` is a valid pre-release");
    minimum
}

/// Accept a host when its plugin protocol is at least 1.2.0, or when its
/// protocol is at least 1.1.0 and its Animus version is at least 0.7.0
/// (release candidates included). A missing or unparseable value counts as
/// too old.
pub fn check_host(protocol: Option<&str>, host_version: Option<&str>) -> Result<(), RpcError> {
    let protocol_version = parse(protocol);
    let accepted = match protocol_version {
        Some(ref version) if *version >= HOST_PROTOCOL_WITHOUT_VERSION_CHECK => true,
        Some(ref version) if *version >= MIN_HOST_PROTOCOL_VERSION => {
            parse(host_version).is_some_and(|version| version >= minimum_host_version())
        }
        _ => false,
    };
    if accepted {
        return Ok(());
    }
    let install_command = format!(
        "animus plugin install launchapp-dev/animus-queue-default@{LEGACY_QUEUE_VERSION} --force"
    );
    let host_display = match host_version
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => value.to_string(),
        None => "an unknown version".to_string(),
    };
    Err(RpcError {
        code: error_codes::INVALID_PARAMS,
        message: format!(
            "animus-queue-default v{} requires Animus 0.7 or newer. This Animus is {host_display} \
             (plugin protocol {}). Install the queue version made for it: `{install_command}`",
            env!("CARGO_PKG_VERSION"),
            protocol.unwrap_or("not sent"),
        ),
        data: Some(json!({
            "host_version": host_version,
            "host_protocol_version": protocol,
            "minimum_host_version": MIN_HOST_VERSION.to_string(),
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
    fn refuses_animus_0_6_with_the_documented_message() {
        // What the installed 0.6.33 sends on every queue call.
        let error = check_host(Some("1.1.0"), Some("0.6.33")).unwrap_err();
        assert_eq!(error.code, error_codes::INVALID_PARAMS);
        assert_eq!(
            error.message,
            format!(
                "animus-queue-default v{} requires Animus 0.7 or newer. This Animus is 0.6.33 \
                 (plugin protocol 1.1.0). Install the queue version made for it: \
                 `animus plugin install launchapp-dev/animus-queue-default@v0.3.3 --force`",
                env!("CARGO_PKG_VERSION")
            )
        );
        let data = error.data.expect("error data");
        assert_eq!(data["host_version"], "0.6.33");
        assert_eq!(data["host_protocol_version"], "1.1.0");
        assert_eq!(data["minimum_host_version"], "0.7.0");
        assert_eq!(data["minimum_host_protocol_version"], "1.1.0");
        assert_eq!(data["compatible_queue_version"], "v0.3.3");
    }

    #[test]
    fn refuses_older_hosts_and_missing_or_unparseable_values() {
        for (protocol, host) in [
            // 0.5.x/0.6.x queue calls.
            (Some("1.1.0"), Some("0.5.0")),
            (Some("1.1.0"), Some("0.6.33")),
            (Some("1.1.0"), Some("0.6.99")),
            // 0.6.x generic plugin-host handshake.
            (Some("1.0.0"), Some("0.1.0")),
            // Protocol 1.1.0 with the host crate's version, missing or garbled.
            (Some("1.1.0"), Some("0.1.0")),
            (Some("1.1.0"), None),
            (Some("1.1.0"), Some("")),
            (Some("1.1.0"), Some("seven")),
            (Some("1.1.0"), Some("0.7")),
            // A 0.7 version never rescues a protocol below 1.1.0.
            (Some("1.0.0"), Some("0.7.0-rc.52")),
            (Some("1.0.9"), Some("0.7.0")),
            (None, Some("0.7.0")),
            (Some(""), Some("0.7.0")),
            (Some("one.two"), Some("0.7.0")),
            (Some("1.1.0-rc.1"), Some("0.7.0")),
        ] {
            assert!(
                check_host(protocol, host).is_err(),
                "{protocol:?} / {host:?} must be refused"
            );
        }
        let missing = check_host(Some("1.1.0"), None).unwrap_err();
        assert!(missing
            .message
            .contains("This Animus is an unknown version (plugin protocol 1.1.0)"));
        let no_protocol = check_host(None, Some("0.6.33")).unwrap_err();
        assert!(no_protocol.message.contains("(plugin protocol not sent)"));
    }

    #[test]
    fn accepts_animus_0_7_and_newer() {
        for (protocol, host) in [
            // What 0.7.0-rc.52 sends on every queue call.
            ("1.1.0", "0.7.0-rc.52"),
            ("1.1.0", "0.7.0-rc.1"),
            ("1.1.0", "0.7.0-alpha"),
            ("1.1.0", "0.7.0"),
            ("1.1.0", " 0.7.3 "),
            ("1.1.0", "0.8.0"),
            ("1.1.0", "1.0.0"),
            ("1.2.0", "0.7.0-rc.52"),
        ] {
            assert!(
                check_host(Some(protocol), Some(host)).is_ok(),
                "{protocol} / {host} must be accepted"
            );
        }
    }

    #[test]
    fn accepts_protocol_1_2_whatever_the_host_version() {
        // 0.7's generic plugin-host handshake sends the host crate's version.
        for host in [Some("0.1.0"), None, Some("garbled")] {
            assert!(
                check_host(Some("1.2.0"), host).is_ok(),
                "{host:?} must be accepted"
            );
        }
        assert!(check_host(Some("2.0.0"), Some("0.1.0")).is_ok());
    }
}
