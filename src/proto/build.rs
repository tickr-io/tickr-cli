fn compile() -> Result<(), Box<dyn std::error::Error>> {
    // The self-contained families (each owns its package's field-number space).
    let proto_files = vec![
        "../../proto/conductor-relay.proto",
        "../../proto/tickr-api.proto",
        "../../proto/workflow-definition.proto",
        "../../proto/instance-snapshot.proto",
        "../../proto/installation-bootstrap.proto",
    ];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // The snapshot's routing value carries its typed value in a oneof for
        // the binary encode/decode, but serde must render it as the bare JSON
        // scalar the UI already consumes (`{"kind":"int","value":85}`), so the
        // JSON matches the archived read path byte-for-byte. `untagged` emits
        // the inner value directly; `kind` disambiguates on the way back in.
        .type_attribute(
            "tickr.instance.RoutingValueView.value",
            "#[serde(untagged)]",
        )
        // The Replay-only provenance fields are omitted (not null / not []) for
        // the non-replay kinds, mirroring the archived read path's shape.
        .field_attribute(
            "tickr.instance.TriggerProvenanceView.source_instance",
            "#[serde(default, skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            "tickr.instance.TriggerProvenanceView.resume_from",
            "#[serde(default, skip_serializing_if = \"Vec::is_empty\")]",
        )
        // The composite-patch minted map is empty for a primitive-op patch, so
        // it is omitted from the wire in that (dominant) case — a primitive
        // patch's applied-patch JSON stays byte-identical to the pre-field shape.
        .field_attribute(
            "tickr.instance.AppliedPatchView.minted_map",
            "#[serde(default, skip_serializing_if = \"std::collections::HashMap::is_empty\")]",
        )
        .field_attribute(
            "tickr.installation.InstallationBootstrap.tenant_tier",
            "#[serde(with = \"crate::installation::serde_tenant_tier\")]",
        )
        .field_attribute(
            "tickr.installation.InstallationBootstrap.formation_profile",
            "#[serde(with = \"crate::installation::serde_formation_profile\")]",
        )
        .field_attribute(
            "tickr.installation.InstallationBootstrap.authentication",
            "#[serde(with = \"crate::installation::serde_authentication_mode\")]",
        )
        .compile_protos(&proto_files, &["../../proto"])?;

    // The patch (`tickr.patch`) and runnable-projection (`tickr.runnable`)
    // families both reference the workflow-definition family for node content
    // (tasks, gates, edge kinds, node types), so they compile in their own pass
    // with `.tickr.workflow` mapped to the already-generated `crate::workflow`
    // module — the module layout flattens each proto package to a crate-root
    // module, so prost needs the extern path to emit resolvable references.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(".tickr.workflow", "crate::workflow")
        .compile_protos(
            &[
                "../../proto/patch.proto",
                "../../proto/runnable-projection.proto",
                "../../proto/task-coordination.proto",
            ],
            &["../../proto"],
        )?;

    // The union archive-grade projection (`tickr.archive`) composes the runnable
    // and instance families — it embeds the runnable graph/tasks section and
    // reuses the instance render views. Both are already generated, so this pass
    // maps every referenced package to its crate-root module and generates only
    // the union's own field-number space on top.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(".tickr.workflow", "crate::workflow")
        .extern_path(".tickr.runnable", "crate::runnable")
        .extern_path(".tickr.instance", "crate::instance")
        .compile_protos(&["../../proto/archive-union.proto"], &["../../proto"])?;

    // The Signal family (`tickr.signal`) — the conductor-authored live-run
    // control envelopes. Its `Trigger.replay` seed reuses the runnable graph and
    // the workflow task model, so it compiles in its own pass mapping those
    // already-generated packages to their crate-root modules and generates only
    // the signal family's own field-number space on top.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(".tickr.workflow", "crate::workflow")
        .extern_path(".tickr.runnable", "crate::runnable")
        .compile_protos(&["../../proto/signal.proto"], &["../../proto"])?;

    println!("cargo:rerun-if-changed=../../proto/conductor-relay.proto");
    println!("cargo:rerun-if-changed=../../proto/installation-bootstrap.proto");
    println!("cargo:rerun-if-changed=../../proto/tickr-api.proto");
    println!("cargo:rerun-if-changed=../../proto/workflow-definition.proto");
    println!("cargo:rerun-if-changed=../../proto/instance-snapshot.proto");
    println!("cargo:rerun-if-changed=../../proto/patch.proto");
    println!("cargo:rerun-if-changed=../../proto/runnable-projection.proto");
    println!("cargo:rerun-if-changed=../../proto/task-coordination.proto");
    println!("cargo:rerun-if-changed=../../proto/archive-union.proto");
    println!("cargo:rerun-if-changed=../../proto/signal.proto");
    Ok(())
}

#[cfg(not(madsim))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    compile()
}

#[cfg(madsim)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    std::fs::create_dir_all(&out_dir)?;
    compile()
}
