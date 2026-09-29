//! The old-CLI guard, exercised through the real binary. The `initialize`
//! params are the ones captured from the real CLIs.

mod common;

use common::PluginProcess;
use serde_json::json;

const INVALID_PARAMS: i64 = -32602;
const PLUGIN_NOT_INITIALIZED: i64 = -32000;

#[test]
fn animus_0_6_is_refused_before_any_file_access() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = PluginProcess::spawn(&[]);

    // Animus 0.6.33's queue calls announce protocol 1.1.0, like 0.7's do.
    let refused = plugin.initialize_as(temp.path(), "1.1.0", "0.6.33");
    assert_eq!(refused["error"]["code"], INVALID_PARAMS);
    let message = refused["error"]["message"].as_str().expect("message");
    assert!(
        message.contains("requires Animus 0.7 or newer. This Animus is 0.6.33"),
        "{message}"
    );
    assert!(
        message.contains("animus plugin install launchapp-dev/animus-queue-default@v0.3.3 --force"),
        "{message}"
    );

    // The refused host gets no backend, so queue calls cannot touch files.
    let listed = plugin.request("queue/list", json!({}));
    assert_eq!(listed["error"]["code"], PLUGIN_NOT_INITIALIZED);
    assert!(
        !temp.path().join(".animus").exists(),
        "a refused host must not create or read queue files"
    );
}

#[test]
fn animus_0_6_generic_handshake_is_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = PluginProcess::spawn(&[]);
    let refused = plugin.initialize_as(temp.path(), "1.0.0", "0.1.0");
    assert_eq!(refused["error"]["code"], INVALID_PARAMS);
}

#[test]
fn missing_host_version_is_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = PluginProcess::spawn(&[]);

    let refused = plugin.request(
        "initialize",
        json!({
            "protocol_version": "1.1.0",
            "capabilities": {},
            "init_extensions": {
                "project_binding": { "project_root": temp.path().to_string_lossy() }
            }
        }),
    );

    assert_eq!(refused["error"]["code"], INVALID_PARAMS);
    assert!(refused["error"]["message"]
        .as_str()
        .expect("message")
        .contains("This Animus is an unknown version (plugin protocol 1.1.0)"));
}

#[test]
fn missing_protocol_version_is_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = PluginProcess::spawn(&[]);

    let refused = plugin.request(
        "initialize",
        json!({
            "host_info": { "name": "animus", "version": "0.7.0-rc.52" },
            "capabilities": {},
            "init_extensions": {
                "project_binding": { "project_root": temp.path().to_string_lossy() }
            }
        }),
    );

    assert_eq!(refused["error"]["code"], INVALID_PARAMS);
    assert!(refused["error"]["message"]
        .as_str()
        .expect("message")
        .contains("(plugin protocol not sent)"));
}

#[test]
fn animus_0_7_is_accepted() {
    // 0.7's queue calls (1.1.0 + its CLI version) and its generic
    // plugin-host handshake (1.2.0 + the host crate's 0.1.0).
    for (protocol, host) in [("1.1.0", "0.7.0-rc.52"), ("1.2.0", "0.1.0")] {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut plugin = PluginProcess::spawn(&[]);
        let accepted = plugin.initialize_as(temp.path(), protocol, host);
        assert!(
            accepted.get("error").is_none(),
            "{protocol}/{host}: {accepted}"
        );
        assert_eq!(accepted["result"]["protocol_version"], "1.2.0");
    }
}
