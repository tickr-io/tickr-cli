//! Encode→decode round-trip proof for the published workflow-definition family.
//!
//! What is under test is the wire contract's external behaviour: what a peer
//! decodes equals what was sent. Each test builds a proto message covering a
//! construct the DSL can author today, encodes it with prost, decodes the
//! bytes back, and asserts equality — the golden discipline applied to the new
//! encoding. The final test assembles a full workflow definition exercising a
//! loop back-edge, all three gate declarations, captures, routing declarations,
//! and every input-source shape at once, so the aggregate round-trips whole.

use prost::Message;
use tickr_proto::workflow as wf;

/// Encode a message and decode the bytes back into a fresh value of the same
/// type — the peer's view of what was sent.
fn round_trip<T>(msg: &T) -> T
where
    T: Message + Default,
{
    let bytes = msg.encode_to_vec();
    T::decode(bytes.as_slice()).expect("a peer must decode what was encoded")
}

fn uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[test]
fn routing_value_every_scalar_round_trips() {
    for value in [
        wf::routing_value::Value::StringValue("hello".to_string()),
        wf::routing_value::Value::IntValue(-42),
        wf::routing_value::Value::BoolValue(true),
        wf::routing_value::Value::BytesValue(vec![0x00, 0x01, 0xff]),
    ] {
        let original = wf::RoutingValue { value: Some(value) };
        assert_eq!(original, round_trip(&original));
    }
}

#[test]
fn duration_round_trips_with_sub_second_nanos() {
    let original = wf::Duration {
        secs: 90,
        nanos: 500_000_000,
    };
    assert_eq!(original, round_trip(&original));
}

#[test]
fn capture_declaration_round_trips() {
    let original = wf::CaptureDeclaration {
        name: "amount".to_string(),
        from: Some(wf::CaptureSource {
            source: Some(wf::capture_source::Source::Trigger(
                wf::capture_source::Trigger {
                    jsonpath: "$.payload.amount".to_string(),
                },
            )),
        }),
    };
    assert_eq!(original, round_trip(&original));
}

#[test]
fn input_source_every_variant_round_trips() {
    let variants = [
        wf::input_source::Source::Task(wf::input_source::Task {
            name: "upstream".to_string(),
        }),
        wf::input_source::Source::Trigger(wf::input_source::Trigger {}),
        wf::input_source::Source::Signal(wf::input_source::Signal {
            signal_name: "approval".to_string(),
            gate_edge_id: uuid(),
        }),
    ];
    for variant in variants {
        let original = wf::InputSource {
            source: Some(variant),
        };
        assert_eq!(original, round_trip(&original));
    }
}

#[test]
fn input_source_list_preserves_bare_and_sourced_slots() {
    // Absence of an inner source marks a bare-name slot; presence marks a
    // structured one. Both must survive so the "some bare, some sourced"
    // distinction the internal vector carries is not flattened on the wire.
    let original = wf::InputSourceList {
        sources: vec![
            wf::OptionalInputSource { source: None },
            wf::OptionalInputSource {
                source: Some(wf::InputSource {
                    source: Some(wf::input_source::Source::Trigger(
                        wf::input_source::Trigger {},
                    )),
                }),
            },
        ],
    };
    let decoded = round_trip(&original);
    assert_eq!(original, decoded);
    assert!(decoded.sources[0].source.is_none(), "bare slot stays bare");
    assert!(
        decoded.sources[1].source.is_some(),
        "sourced slot stays sourced"
    );
}

#[test]
fn task_signal_emit_both_variants_round_trip() {
    for emit in [
        wf::task_signal_emit::Emit::OnSuccess(wf::task_signal_emit::OnSuccess {
            signal_name: "shipped".to_string(),
            from_routing_var: "tracking".to_string(),
        }),
        wf::task_signal_emit::Emit::OnFailure(wf::task_signal_emit::OnFailure {
            signal_name: "failed".to_string(),
        }),
    ] {
        let original = wf::TaskSignalEmit { emit: Some(emit) };
        assert_eq!(original, round_trip(&original));
    }
}

#[test]
fn gate_every_declaration_round_trips() {
    let signal_gate = wf::Gate {
        kind: Some(wf::gate::Kind::SignalReceived(wf::gate::SignalReceived {
            signal_name: "approval".to_string(),
            predicate: Some("$[?@.approved]".to_string()),
            captures_spec: vec![wf::CaptureDeclaration {
                name: "who".to_string(),
                from: Some(wf::CaptureSource {
                    source: Some(wf::capture_source::Source::Trigger(
                        wf::capture_source::Trigger {
                            jsonpath: "$.who".to_string(),
                        },
                    )),
                }),
            }],
            timeout: Some(wf::Duration {
                secs: 3600,
                nanos: 0,
            }),
        })),
    };
    let predicate_gate = wf::Gate {
        kind: Some(wf::gate::Kind::PredicateHolds(wf::gate::PredicateHolds {
            routing_var: "count".to_string(),
            op: wf::ComparisonOp::Ge as i32,
            value: Some(wf::RoutingValue {
                value: Some(wf::routing_value::Value::IntValue(10)),
            }),
            timeout: None,
        })),
    };
    let timer_gate = wf::Gate {
        kind: Some(wf::gate::Kind::TimerElapsed(wf::gate::TimerElapsed {
            duration: Some(wf::Duration { secs: 30, nanos: 0 }),
        })),
    };
    for gate in [signal_gate, predicate_gate, timer_gate] {
        assert_eq!(gate, round_trip(&gate));
    }
}

#[test]
fn trigger_every_variant_round_trips() {
    let cron = wf::Trigger {
        kind: Some(wf::trigger::Kind::Cron("0 9 * * *".to_string())),
    };
    let fire_now = wf::Trigger {
        kind: Some(wf::trigger::Kind::FireNow(wf::trigger::FireNow {})),
    };
    let waits = wf::Trigger {
        kind: Some(wf::trigger::Kind::WaitsOnSignal(wf::WaitsOnSignalConfig {
            signal_name: "user-paid".to_string(),
            predicate: Some("$[?@.paid]".to_string()),
            captures: vec![wf::CaptureDeclaration {
                name: "amount".to_string(),
                from: Some(wf::CaptureSource {
                    source: Some(wf::capture_source::Source::Trigger(
                        wf::capture_source::Trigger {
                            jsonpath: "$.amount".to_string(),
                        },
                    )),
                }),
            }],
        })),
    };
    for trigger in [cron, fire_now, waits] {
        assert_eq!(trigger, round_trip(&trigger));
    }
}

#[test]
fn task_definition_round_trips_with_every_field_populated() {
    let original = wf::TaskDefinition {
        id: uuid(),
        workflow_id: uuid(),
        name: "build".to_string(),
        task_type: wf::TaskType::Sensor as i32,
        nix_expression_path: "/nix/store/build".to_string(),
        nix_args: vec!["--flag".to_string()],
        outputs: vec!["digest".to_string()],
        inputs: vec!["src".to_string()],
        secrets: vec!["token".to_string()],
        max_attempts: 5,
        input_sources: Some(wf::InputSourceList {
            sources: vec![wf::OptionalInputSource {
                source: Some(wf::InputSource {
                    source: Some(wf::input_source::Source::Task(wf::input_source::Task {
                        name: "src".to_string(),
                    })),
                }),
            }],
        }),
        timeout_secs: Some(120),
        emits: vec![wf::TaskSignalEmit {
            emit: Some(wf::task_signal_emit::Emit::OnSuccess(
                wf::task_signal_emit::OnSuccess {
                    signal_name: "built".to_string(),
                    from_routing_var: "digest".to_string(),
                },
            )),
        }],
        routing_vars: vec![wf::RoutingVarDecl {
            name: "digest".to_string(),
            var_type: Some("string".to_string()),
        }],
        loop_participant: true,
    };
    assert_eq!(original, round_trip(&original));
}

#[test]
fn task_graph_round_trips_with_a_loop_back_edge() {
    let start = uuid();
    let end = uuid();
    let a = uuid();
    let b = uuid();
    let original = wf::TaskGraph {
        nodes: vec![
            wf::GraphNode {
                id: start.clone(),
                node_type: wf::NodeType::Start as i32,
            },
            wf::GraphNode {
                id: end.clone(),
                node_type: wf::NodeType::End as i32,
            },
            wf::GraphNode {
                id: a.clone(),
                node_type: wf::NodeType::Task as i32,
            },
            wf::GraphNode {
                id: b.clone(),
                node_type: wf::NodeType::Task as i32,
            },
        ],
        edges: vec![
            wf::Edge {
                id: uuid(),
                sources: vec![start.clone()],
                targets: vec![a.clone()],
                kind: wf::EdgeKind::Control as i32,
                gates: vec![],
            },
            // A `loop` back-edge b → a — the construct that distinguishes a
            // back-edge from a forward edge.
            wf::Edge {
                id: uuid(),
                sources: vec![b.clone()],
                targets: vec![a.clone()],
                kind: wf::EdgeKind::Loop as i32,
                gates: vec![],
            },
            // A gated forward `data` edge a → end.
            wf::Edge {
                id: uuid(),
                sources: vec![a.clone()],
                targets: vec![end.clone()],
                kind: wf::EdgeKind::Data as i32,
                gates: vec![wf::Gate {
                    kind: Some(wf::gate::Kind::TimerElapsed(wf::gate::TimerElapsed {
                        duration: Some(wf::Duration { secs: 5, nanos: 0 }),
                    })),
                }],
            },
        ],
        start,
        end,
    };
    assert_eq!(original, round_trip(&original));
}

#[test]
fn full_workflow_definition_round_trips_including_loop_gate_capture_routing() {
    let workflow_id = uuid();
    let start = uuid();
    let end = uuid();
    let looped = uuid();

    let task = wf::TaskDefinition {
        id: looped.clone(),
        workflow_id: workflow_id.clone(),
        name: "poll".to_string(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: "/nix/store/poll".to_string(),
        nix_args: vec![],
        outputs: vec!["status".to_string()],
        inputs: vec!["seed".to_string(), "approver".to_string()],
        secrets: vec![],
        max_attempts: 3,
        input_sources: Some(wf::InputSourceList {
            sources: vec![
                // Bare-name slot preserved alongside a structured Signal slot.
                wf::OptionalInputSource { source: None },
                wf::OptionalInputSource {
                    source: Some(wf::InputSource {
                        source: Some(wf::input_source::Source::Signal(wf::input_source::Signal {
                            signal_name: "approval".to_string(),
                            gate_edge_id: uuid(),
                        })),
                    }),
                },
            ],
        }),
        timeout_secs: Some(300),
        emits: vec![wf::TaskSignalEmit {
            emit: Some(wf::task_signal_emit::Emit::OnFailure(
                wf::task_signal_emit::OnFailure {
                    signal_name: "poll-failed".to_string(),
                },
            )),
        }],
        routing_vars: vec![wf::RoutingVarDecl {
            name: "status".to_string(),
            var_type: Some("string".to_string()),
        }],
        loop_participant: true,
    };

    let mut tags = std::collections::HashMap::new();
    tags.insert("team".to_string(), "payments".to_string());
    tags.insert("env".to_string(), "prod".to_string());

    let original = wf::WorkflowDefinition {
        id: workflow_id.clone(),
        tenant_id: "8f51db61-785a-5bad-b6c9-e92abfcf5ad7".to_string(),
        namespace: "reporting".to_string(),
        slug: "daily-sync".to_string(),
        name: "Daily Sync".to_string(),
        version: 7,
        tasks: vec![task],
        task_graph: Some(wf::TaskGraph {
            nodes: vec![
                wf::GraphNode {
                    id: start.clone(),
                    node_type: wf::NodeType::Start as i32,
                },
                wf::GraphNode {
                    id: end.clone(),
                    node_type: wf::NodeType::End as i32,
                },
                wf::GraphNode {
                    id: looped.clone(),
                    node_type: wf::NodeType::Task as i32,
                },
            ],
            edges: vec![
                wf::Edge {
                    id: uuid(),
                    sources: vec![start.clone()],
                    targets: vec![looped.clone()],
                    kind: wf::EdgeKind::Control as i32,
                    gates: vec![],
                },
                // Self-loop back-edge with a predicate gate: exercises loop +
                // routing-variable gate together.
                wf::Edge {
                    id: uuid(),
                    sources: vec![looped.clone()],
                    targets: vec![looped.clone()],
                    kind: wf::EdgeKind::Loop as i32,
                    gates: vec![wf::Gate {
                        kind: Some(wf::gate::Kind::PredicateHolds(wf::gate::PredicateHolds {
                            routing_var: "status".to_string(),
                            op: wf::ComparisonOp::NotEq as i32,
                            value: Some(wf::RoutingValue {
                                value: Some(wf::routing_value::Value::StringValue(
                                    "done".to_string(),
                                )),
                            }),
                            timeout: Some(wf::Duration {
                                secs: 600,
                                nanos: 0,
                            }),
                        })),
                    }],
                },
                // Gated data exit edge with a signal gate carrying captures.
                wf::Edge {
                    id: uuid(),
                    sources: vec![looped.clone()],
                    targets: vec![end.clone()],
                    kind: wf::EdgeKind::Data as i32,
                    gates: vec![wf::Gate {
                        kind: Some(wf::gate::Kind::SignalReceived(wf::gate::SignalReceived {
                            signal_name: "approval".to_string(),
                            predicate: Some("$[?@.approved]".to_string()),
                            captures_spec: vec![wf::CaptureDeclaration {
                                name: "approver".to_string(),
                                from: Some(wf::CaptureSource {
                                    source: Some(wf::capture_source::Source::Trigger(
                                        wf::capture_source::Trigger {
                                            jsonpath: "$.approver".to_string(),
                                        },
                                    )),
                                }),
                            }],
                            timeout: Some(wf::Duration {
                                secs: 86400,
                                nanos: 0,
                            }),
                        })),
                    }],
                },
            ],
            start,
            end,
        }),
        trigger: Some(wf::Trigger {
            kind: Some(wf::trigger::Kind::WaitsOnSignal(wf::WaitsOnSignalConfig {
                signal_name: "kickoff".to_string(),
                predicate: None,
                captures: vec![wf::CaptureDeclaration {
                    name: "seed".to_string(),
                    from: Some(wf::CaptureSource {
                        source: Some(wf::capture_source::Source::Trigger(
                            wf::capture_source::Trigger {
                                jsonpath: "$.seed".to_string(),
                            },
                        )),
                    }),
                }],
            })),
        }),
        status: wf::WorkflowStatus::Active as i32,
        captures: vec![wf::CaptureDeclaration {
            name: "seed".to_string(),
            from: Some(wf::CaptureSource {
                source: Some(wf::capture_source::Source::Trigger(
                    wf::capture_source::Trigger {
                        jsonpath: "$.seed".to_string(),
                    },
                )),
            }),
        }],
        timeout_secs: Some(3600),
        tags,
    };

    assert_eq!(original, round_trip(&original));
}
