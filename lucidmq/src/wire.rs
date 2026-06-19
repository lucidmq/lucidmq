use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::messages::{
    ConsumeRequest, ProduceMessage, ProduceRequest, RecordOp, StateAction, StateRequest,
    TopicAction, TopicsList,
};
use crate::topic::SimpleTopic;
use crate::types::Command;
use nolan::StoredRecord;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
enum WireMessage {
    TopicRequest {
        topic_name: String,
        request_type: TopicAction,
    },
    TopicResponse {
        topic_name: String,
        success: bool,
        response_type: WireTopicActionResponse,
    },
    ProduceRequest {
        topic_name: String,
        messages: Vec<WireProduceMessage>,
    },
    ProduceResponse {
        success: bool,
        topic_name: String,
        offset: u64,
    },
    ConsumeRequest {
        topic_name: String,
        consumer_group: String,
        timeout: u64,
    },
    ConsumeResponse {
        success: bool,
        topic_name: String,
        messages: Vec<WireStoredRecord>,
    },
    StateRequest {
        topic_name: String,
        action: StateAction,
        #[serde(default)]
        source_id: Option<String>,
        #[serde(default)]
        parent_source_id: Option<String>,
    },
    StateResponse {
        success: bool,
        topic_name: String,
        action: StateAction,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        record: Option<WireStoredRecord>,
        #[serde(default)]
        records: Vec<WireStoredRecord>,
    },
    InvalidResponse {
        error_message: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
enum WireTopicActionResponse {
    Describe {
        max_segment_bytes: u64,
        max_retention_bytes: u64,
        consumer_groups: Vec<String>,
    },
    Create,
    Delete,
    All {
        topics: Vec<TopicsList>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct WireProduceMessage {
    timestamp: u64,
    source_id: String,
    payload: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_source_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct WireStoredRecord {
    timestamp: u64,
    source_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_source_id: Option<String>,
    op: RecordOp,
}

pub fn new_topic_response_create(topic_name: &str, is_success: bool) -> Vec<u8> {
    serialize_and_frame(&WireMessage::TopicResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        response_type: WireTopicActionResponse::Create,
    })
}

pub fn new_topic_response_describe(
    topic_name: &str,
    is_success: bool,
    max_retention: u64,
    max_segment: u64,
    consumer_groups: Vec<String>,
) -> Vec<u8> {
    serialize_and_frame(&WireMessage::TopicResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        response_type: WireTopicActionResponse::Describe {
            max_segment_bytes: max_segment,
            max_retention_bytes: max_retention,
            consumer_groups,
        },
    })
}

pub fn new_topic_response_all(is_success: bool, topics_datas: Vec<SimpleTopic>) -> Vec<u8> {
    let topics = topics_datas
        .into_iter()
        .map(|t| TopicsList {
            topic_name: t.topic_name,
            consumer_groups: t.consumer_groups,
        })
        .collect();

    serialize_and_frame(&WireMessage::TopicResponse {
        topic_name: "placeholder".to_string(),
        success: is_success,
        response_type: WireTopicActionResponse::All { topics },
    })
}

pub fn new_topic_response_delete(topic_name: &str, is_success: bool) -> Vec<u8> {
    serialize_and_frame(&WireMessage::TopicResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        response_type: WireTopicActionResponse::Delete,
    })
}

pub fn new_produce_response(topic_name: &str, last_offset: u64, is_success: bool) -> Vec<u8> {
    serialize_and_frame(&WireMessage::ProduceResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        offset: last_offset,
    })
}

pub fn new_consume_response(
    topic_name: &str,
    is_success: bool,
    messages: Vec<StoredRecord>,
) -> Vec<u8> {
    serialize_and_frame(&WireMessage::ConsumeResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        messages: messages.into_iter().map(WireStoredRecord::from).collect(),
    })
}

pub fn new_state_response(
    topic_name: &str,
    is_success: bool,
    action: StateAction,
    record: Option<StoredRecord>,
    records: Vec<StoredRecord>,
) -> Vec<u8> {
    serialize_and_frame(&WireMessage::StateResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        action,
        record: record.map(WireStoredRecord::from),
        records: records.into_iter().map(WireStoredRecord::from).collect(),
    })
}

pub fn new_invalid_response(message_text: &str) -> Vec<u8> {
    serialize_and_frame(&WireMessage::InvalidResponse {
        error_message: message_text.to_string(),
    })
}

fn serialize_and_frame(message: &WireMessage) -> Vec<u8> {
    let mut payload = serde_json::to_vec(message).expect("Failed to serialize message");
    let size_u16 = u16::try_from(payload.len()).expect("Message too large");
    let size_bytes = size_u16.to_le_bytes();
    payload.splice(0..0, size_bytes.iter().cloned());
    payload
}

pub fn parse_request(conn_id: String, data: Vec<u8>) -> Command {
    match serde_json::from_slice::<WireMessage>(&data) {
        Ok(WireMessage::TopicRequest {
            topic_name,
            request_type,
        }) => Command::TopicRequest {
            conn_id,
            request: crate::messages::TopicRequest {
                topic_name,
                request_type,
            },
        },
        Ok(WireMessage::ProduceRequest {
            topic_name,
            messages,
        }) => match produce_request_from_wire(topic_name, messages) {
            Ok(request) => Command::ProduceRequest { conn_id, request },
            Err(error_message) => Command::Invalid {
                conn_id,
                error_message,
                capmessage_data: Vec::new(),
            },
        },
        Ok(WireMessage::ConsumeRequest {
            topic_name,
            consumer_group,
            timeout,
        }) => Command::ConsumeRequest {
            conn_id,
            request: ConsumeRequest {
                topic_name,
                consumer_group,
                timeout,
            },
        },
        Ok(WireMessage::StateRequest {
            topic_name,
            action,
            source_id,
            parent_source_id,
        }) => Command::StateRequest {
            conn_id,
            request: StateRequest {
                topic_name,
                action,
                source_id: source_id.map(String::into_bytes),
                parent_source_id: parent_source_id.map(String::into_bytes),
            },
        },
        Ok(_) => Command::Invalid {
            conn_id,
            error_message: "Received a response message type instead of a request".to_string(),
            capmessage_data: Vec::new(),
        },
        Err(e) => Command::Invalid {
            conn_id,
            error_message: format!("Failed to parse JSON message: {}", e),
            capmessage_data: Vec::new(),
        },
    }
}

fn produce_request_from_wire(
    topic_name: String,
    messages: Vec<WireProduceMessage>,
) -> Result<ProduceRequest, String> {
    let messages = messages
        .into_iter()
        .map(|message| {
            serde_json::to_vec(&message.payload)
                .map(|payload| ProduceMessage {
                    timestamp: message.timestamp,
                    source_id: message.source_id.into_bytes(),
                    payload,
                    parent_source_id: message.parent_source_id.map(String::into_bytes),
                })
                .map_err(|e| format!("Failed to encode JSON payload: {}", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ProduceRequest {
        topic_name,
        messages,
    })
}

impl From<StoredRecord> for WireStoredRecord {
    fn from(record: StoredRecord) -> Self {
        WireStoredRecord {
            timestamp: record.timestamp,
            source_id: bytes_to_string(&record.source_id),
            payload: record.payload.as_deref().map(bytes_to_json_value),
            parent_source_id: record
                .parent_source_id
                .as_ref()
                .map(|parent_source_id| bytes_to_string(parent_source_id)),
            op: record.op,
        }
    }
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn bytes_to_json_value(bytes: &[u8]) -> JsonValue {
    serde_json::from_slice(bytes).unwrap_or_else(|_| JsonValue::String(bytes_to_string(bytes)))
}

#[cfg(test)]
mod wire_tests {
    use super::{new_consume_response, new_state_response, parse_request, WireMessage};
    use crate::messages::{ProduceMessage, RecordOp, StateAction};
    use crate::types::Command;
    use nolan::StoredRecord;
    use serde_json::{json, Value as JsonValue};

    #[test]
    fn test_consume_response_serializes_as_json_wire_shape() {
        let record = StoredRecord::upsert(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            br#"{"hello":"world"}"#.to_vec(),
            1234,
        );

        let response_bytes = new_consume_response("topic-a", true, vec![record]);
        let payload = &response_bytes[2..];
        let envelope: JsonValue =
            serde_json::from_slice(payload).expect("unable to decode consume response");

        assert_eq!(
            envelope,
            json!({
                "type": "ConsumeResponse",
                "success": true,
                "topic_name": "topic-a",
                "messages": [{
                    "timestamp": 1234,
                    "source_id": "source-1",
                    "payload": {"hello": "world"},
                    "parent_source_id": "parent-1",
                    "op": "Upsert"
                }]
            })
        );
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
            vec![record],
        );
        let payload = &response_bytes[2..];
        let envelope: WireMessage =
            serde_json::from_slice(payload).expect("unable to decode state response");

        match envelope {
            WireMessage::StateResponse {
                success,
                topic_name,
                action,
                record,
                records,
            } => {
                assert!(success);
                assert_eq!("topic-a", topic_name);
                assert_eq!(StateAction::ScanCurrent, action);
                assert_eq!(None, record);
                assert_eq!(1, records.len());
                assert_eq!("source-1", records[0].source_id);
                assert_eq!(Some(json!({"hello": "world"})), records[0].payload);
                assert_eq!(Some("parent-1".to_string()), records[0].parent_source_id);
                assert_eq!(RecordOp::Upsert, records[0].op);
            }
            _ => panic!("unexpected message envelope"),
        }
    }

    #[test]
    fn test_parse_state_request_get() {
        let request = json!({
            "type": "StateRequest",
            "topic_name": "topic-a",
            "action": "Get",
            "source_id": "source-1"
        });

        let parsed = parse_request("conn-1".to_string(), serde_json::to_vec(&request).unwrap());

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

    #[test]
    fn test_parse_produce_request_append_message() {
        let request = json!({
            "type": "ProduceRequest",
            "topic_name": "topic-a",
            "messages": [{
                "timestamp": 1234,
                "source_id": "source-1",
                "payload": {"hello": "world"},
                "parent_source_id": "parent-1"
            }]
        });

        let parsed = parse_request("conn-1".to_string(), serde_json::to_vec(&request).unwrap());

        match parsed {
            Command::ProduceRequest { conn_id, request } => {
                assert_eq!("conn-1", conn_id);
                assert_eq!("topic-a", request.topic_name);
                assert_eq!(1, request.messages.len());
                assert_eq!(
                    ProduceMessage {
                        timestamp: 1234,
                        source_id: b"source-1".to_vec(),
                        payload: br#"{"hello":"world"}"#.to_vec(),
                        parent_source_id: Some(b"parent-1".to_vec()),
                    },
                    request.messages[0]
                );
            }
            _ => panic!("unexpected command parsed"),
        }
    }
}
