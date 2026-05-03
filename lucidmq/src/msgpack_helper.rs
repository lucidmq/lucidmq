use crate::messages::*;
use crate::types::Command;
use crate::topic::SimpleTopic;

pub fn new_topic_response_create(topic_name: &str, is_success: bool) -> Vec<u8> {
    let env = MessageEnvelope::TopicResponse(TopicResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        response_type: TopicActionResponse::Create,
    });
    serialize_and_frame(&env)
}

pub fn new_topic_response_describe(
    topic_name: &str,
    is_success: bool,
    max_retention: u64,
    max_segment: u64,
    consumer_groups: Vec<String>,
) -> Vec<u8> {
    let env = MessageEnvelope::TopicResponse(TopicResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        response_type: TopicActionResponse::Describe {
            max_segment_bytes: max_segment,
            max_retention_bytes: max_retention,
            consumer_groups,
        },
    });
    serialize_and_frame(&env)
}

pub fn new_topic_response_all(is_success: bool, topics_datas: Vec<SimpleTopic>) -> Vec<u8> {
    let topics = topics_datas.into_iter().map(|t| TopicsList {
        topic_name: t.topic_name,
        consumer_groups: t.consumer_groups,
    }).collect();

    let env = MessageEnvelope::TopicResponse(TopicResponse {
        topic_name: "placeholder".to_string(),
        success: is_success,
        response_type: TopicActionResponse::All { topics },
    });
    serialize_and_frame(&env)
}

pub fn new_topic_response_delete(topic_name: &str, is_success: bool) -> Vec<u8> {
    let env = MessageEnvelope::TopicResponse(TopicResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        response_type: TopicActionResponse::Delete,
    });
    serialize_and_frame(&env)
}

pub fn new_produce_response(topic_name: &str, last_offset: u64, is_success: bool) -> Vec<u8> {
    let env = MessageEnvelope::ProduceResponse(ProduceResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        offset: last_offset,
    });
    serialize_and_frame(&env)
}

pub fn new_consume_response(
    topic_name: &str,
    is_success: bool,
    messages: Vec<StoredRecord>,
) -> Vec<u8> {
    let env = MessageEnvelope::ConsumeResponse(ConsumeResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        messages,
    });
    serialize_and_frame(&env)
}

pub fn new_state_response(
    topic_name: &str,
    is_success: bool,
    action: StateAction,
    record: Option<StoredRecord>,
    records: Vec<StoredRecord>,
) -> Vec<u8> {
    let env = MessageEnvelope::StateResponse(StateResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        action,
        record,
        records,
    });
    serialize_and_frame(&env)
}

pub fn new_invalid_response(message_text: &str) -> Vec<u8> {
    let env = MessageEnvelope::InvalidResponse(InvalidResponse {
        error_message: message_text.to_string(),
    });
    serialize_and_frame(&env)
}

fn serialize_and_frame(envelope: &MessageEnvelope) -> Vec<u8> {
    // CHANGE HERE: use to_vec_named to serialize as dictionaries instead of lists
    let mut payload = rmp_serde::to_vec_named(envelope).expect("Failed to serialize message");
    
    let size_u16 = u16::try_from(payload.len()).expect("Message too large");
    let size_bytes = size_u16.to_le_bytes();
    payload.splice(0..0, size_bytes.iter().cloned());
    payload
}

pub fn parse_request(conn_id: String, data: Vec<u8>) -> Command {
    match rmp_serde::from_slice::<MessageEnvelope>(&data) {
        Ok(MessageEnvelope::TopicRequest(req)) => Command::TopicRequest {
            conn_id,
            request: req,
        },
        Ok(MessageEnvelope::ProduceRequest(req)) => Command::ProduceRequest {
            conn_id,
            request: req,
        },
        Ok(MessageEnvelope::ConsumeRequest(req)) => Command::ConsumeRequest {
            conn_id,
            request: req,
        },
        Ok(MessageEnvelope::StateRequest(req)) => Command::StateRequest {
            conn_id,
            request: req,
        },
        Ok(_) => Command::Invalid {
            conn_id,
            error_message: "Received a response message type instead of a request".to_string(),
            capmessage_data: Vec::new(),
        },
        Err(e) => Command::Invalid {
            conn_id,
            error_message: format!("Failed to parse message: {}", e),
            capmessage_data: Vec::new(),
        }
    }
}

#[cfg(test)]
mod msgpack_helper_tests {
    use serde::{Deserialize, Serialize};

    use super::{new_consume_response, new_state_response, parse_request};
    use crate::messages::{MessageEnvelope, StateAction, StateRequest};
    use crate::types::Command;
    use nolan::{RecordOp, StoredRecord};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct CanonicalWireRecord {
        last_updated: u64,
        source_id: Vec<u8>,
        payload: Option<Vec<u8>>,
        parent_source_id: Option<Vec<u8>>,
        op: RecordOp,
    }

    #[derive(Serialize)]
    struct LegacyWireRecord {
        timestamp: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    }

    #[test]
    fn test_stored_record_uses_canonical_wire_shape() {
        let record = StoredRecord::upsert(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            br#"{"hello":"world"}"#.to_vec(),
            1234,
        );

        let encoded = rmp_serde::to_vec_named(&record).expect("unable to encode stored record");
        let decoded: CanonicalWireRecord =
            rmp_serde::from_slice(&encoded).expect("unable to decode canonical wire record");

        assert_eq!(
            decoded,
            CanonicalWireRecord {
                last_updated: 1234,
                source_id: b"source-1".to_vec(),
                payload: Some(br#"{"hello":"world"}"#.to_vec()),
                parent_source_id: Some(b"parent-1".to_vec()),
                op: RecordOp::Upsert,
            }
        );
    }

    #[test]
    fn test_legacy_wire_shape_is_rejected() {
        let legacy = LegacyWireRecord {
            timestamp: 1234,
            key: b"source-1".to_vec(),
            value: b"value".to_vec(),
        };

        let encoded = rmp_serde::to_vec_named(&legacy).expect("unable to encode legacy wire record");
        let decoded = rmp_serde::from_slice::<StoredRecord>(&encoded);

        assert!(decoded.is_err());
    }

    #[test]
    fn test_consume_response_round_trips_canonical_record() {
        let record = StoredRecord::delete(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            7777,
        );

        let response_bytes = new_consume_response("topic-a", true, vec![record.clone()]);
        let payload = &response_bytes[2..];
        let envelope: MessageEnvelope =
            rmp_serde::from_slice(payload).expect("unable to decode consume response");

        match envelope {
            MessageEnvelope::ConsumeResponse(response) => {
                assert!(response.success);
                assert_eq!("topic-a", response.topic_name);
                assert_eq!(vec![record], response.messages);
            }
            _ => panic!("unexpected message envelope"),
        }
    }

    #[test]
    fn test_state_response_round_trips_current_view() {
        let record = StoredRecord::upsert(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            br#"{"hello":"world"}"#.to_vec(),
            1234,
        );

        let response_bytes = new_state_response(
            "topic-a",
            true,
            StateAction::ScanCurrent,
            None,
            vec![record.clone()],
        );
        let payload = &response_bytes[2..];
        let envelope: MessageEnvelope =
            rmp_serde::from_slice(payload).expect("unable to decode state response");

        match envelope {
            MessageEnvelope::StateResponse(response) => {
                assert!(response.success);
                assert_eq!("topic-a", response.topic_name);
                assert_eq!(StateAction::ScanCurrent, response.action);
                assert_eq!(None, response.record);
                assert_eq!(vec![record], response.records);
            }
            _ => panic!("unexpected message envelope"),
        }
    }

    #[test]
    fn test_parse_state_request_get() {
        let request = MessageEnvelope::StateRequest(StateRequest {
            topic_name: "topic-a".to_string(),
            action: StateAction::Get,
            source_id: Some(b"source-1".to_vec()),
            parent_source_id: None,
        });

        let encoded = rmp_serde::to_vec_named(&request).expect("unable to encode state request");
        let parsed = parse_request("conn-1".to_string(), encoded);

        match parsed {
            Command::StateRequest { conn_id, request } => {
                assert_eq!("conn-1", conn_id);
                assert_eq!("topic-a", request.topic_name);
                assert_eq!(StateAction::Get, request.action);
                assert_eq!(Some(b"source-1".to_vec()), request.source_id);
                assert_eq!(None, request.parent_source_id);
            }
            _ => panic!("unexpected command parsed"),
        }
    }
}
