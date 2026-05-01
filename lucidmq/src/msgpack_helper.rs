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
    message_data: Vec<Vec<u8>>,
) -> Vec<u8> {
    // Deserialize the stored message bytes back into Message structs
    let messages: Vec<Message> = message_data.into_iter().filter_map(|m_bytes| {
        rmp_serde::from_slice(&m_bytes).ok()
    }).collect();

    let env = MessageEnvelope::ConsumeResponse(ConsumeResponse {
        topic_name: topic_name.to_string(),
        success: is_success,
        messages,
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