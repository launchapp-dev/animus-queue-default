//! The old-CLI guard, exercised through the real binary.

mod common;

use common::PluginProcess;
use serde_json::json;

const INVALID_PARAMS: i64 = -32602;
const PLUGIN_NOT_INITIALIZED: i64 = -32000;

#[test]
fn protocol_1_0_host_is_refused_before_any_file_access() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = PluginProcess::spawn(&[]);

    let refused = plugin.initialize(temp.path(), "1.0.0");
    assert_eq!(refused["error"]["code"], INVALID_PARAMS);
    let message = refused["error"]["message"].as_str().expect("message");
    assert!(
        message.contains("requires Animus 0.7 or newer"),
        "{message}"
    );
    assert!(
        message.contains("animus plugin install launchapp-dev/animus-queue-default@v0.3.3"),
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
fn missing_protocol_version_is_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut plugin = PluginProcess::spawn(&[]);

    let refused = plugin.request(
        "initialize",
        json!({
            "host_info": { "name": "animus", "version": "0.1.0" },
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
fn protocol_1_1_and_1_2_hosts_are_accepted() {
    for version in ["1.1.0", "1.2.0"] {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut plugin = PluginProcess::spawn(&[]);
        let accepted = plugin.initialize(temp.path(), version);
        assert!(accepted.get("error").is_none(), "{version}: {accepted}");
        assert_eq!(accepted["result"]["protocol_version"], "1.2.0");
    }
}
