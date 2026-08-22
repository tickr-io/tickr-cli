use prost::Message;
use tickr_proto::{ConductorRelayMessage, EntityType};

#[test]
fn relay_envelope_has_golden_wire_tags() {
    let message = ConductorRelayMessage {
        entity_type: EntityType::CancelTaskAck as i32,
        payload: vec![0xaa, 0xbb],
        tenant_id: Some("4d6d3d1f-7596-548c-a49c-aed687d5fe0b".to_owned()),
        tenant_slug: Some("steady-orbit".to_owned()),
    };

    assert_eq!(
        message.encode_to_vec(),
        vec![
            0x08, 0x11, 0x12, 0x02, 0xaa, 0xbb, 0x1a, 0x24, b'4', b'd', b'6', b'd', b'3', b'd',
            b'1', b'f', b'-', b'7', b'5', b'9', b'6', b'-', b'5', b'4', b'8', b'c', b'-', b'a',
            b'4', b'9', b'c', b'-', b'a', b'e', b'd', b'6', b'8', b'7', b'd', b'5', b'f', b'e',
            b'0', b'b', 0x22, 0x0c, b's', b't', b'e', b'a', b'd', b'y', b'-', b'o', b'r', b'b',
            b'i', b't',
        ]
    );

    let mut future_wire = message.encode_to_vec();
    future_wire.extend_from_slice(&[0xa2, 0x06, 0x03, b'n', b'e', b'w']);
    assert_eq!(
        ConductorRelayMessage::decode(future_wire.as_slice()).unwrap(),
        message
    );

    let decoded = ConductorRelayMessage::decode(&[0x08, 0x07, 0x12, 0x01, 0xff][..]).unwrap();
    assert_eq!(decoded.entity_type, EntityType::Compaction as i32);
    assert_eq!(decoded.payload, vec![0xff]);
    assert_eq!(decoded.tenant_id, None);
    assert_eq!(decoded.tenant_slug, None);
}

#[test]
fn entity_type_numbers_are_stable() {
    let expected = [
        (EntityType::TaskQueueItem, 0),
        (EntityType::TaskEvent, 1),
        (EntityType::TaskLog, 2),
        (EntityType::SubmitWorkflow, 3),
        (EntityType::Compaction, 7),
        (EntityType::CompactionAck, 8),
        (EntityType::Signal, 9),
        (EntityType::SignalApplied, 10),
        (EntityType::DispatchPrecondition, 11),
        (EntityType::GateOutcome, 12),
        (EntityType::CancelPrecondition, 13),
        (EntityType::PatchWorkflowInstance, 14),
        (EntityType::PatchOutcome, 15),
        (EntityType::CancelTask, 16),
        (EntityType::CancelTaskAck, 17),
    ];
    for (value, number) in expected {
        assert_eq!(value as i32, number);
    }
    for reserved in [4, 5, 6] {
        assert!(EntityType::try_from(reserved).is_err());
    }
}

#[test]
fn proto_source_pins_reservations_and_streaming_service_fqn() {
    let source = include_str!("../../../proto/conductor-relay.proto");
    assert!(source.contains("package tickr;"));
    assert!(source.contains("optional string tenant_slug = 4;"));
    assert!(source.contains("reserved 4;"));
    assert!(source.contains("reserved 5, 6;"));
    assert!(source.contains("reserved \"WORKFLOW_BUILD_UPDATE\";"));
    assert!(source.contains("reserved \"QUERY_REQUEST\", \"QUERY_RESPONSE\";"));
    assert!(source.contains("service ConductorRelayService"));
    assert!(source.contains(
        "rpc StreamConductorRelay(stream ConductorRelayMessage) returns (stream ConductorRelayMessage);"
    ));
    // `package tickr` plus these exact declarations pins the route to
    // /tickr.ConductorRelayService/StreamConductorRelay.
}
