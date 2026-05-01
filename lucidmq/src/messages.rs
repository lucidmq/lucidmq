use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MessageEnvelope {
    TopicRequest(TopicRequest),
    TopicResponse(TopicResponse),
    ProduceRequest(ProduceRequest),
    ProduceResponse(ProduceResponse),
    ConsumeRequest(ConsumeRequest),
    ConsumeResponse(ConsumeResponse),
    InvalidResponse(InvalidResponse),
}

// ----- Topic Messages -----

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicRequest {
    pub topic_name: String,
    pub request_type: TopicAction,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TopicAction {
    Describe,
    Create,
    Delete,
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicResponse {
    pub topic_name: String,
    pub success: bool,
    pub response_type: TopicActionResponse,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TopicActionResponse {
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
pub struct TopicsList {
    pub topic_name: String,
    pub consumer_groups: Vec<String>,
}

// ----- Produce Messages -----

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProduceRequest {
    pub topic_name: String,
    pub messages: Vec<Message>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProduceResponse {
    pub success: bool,
    pub topic_name: String,
    pub offset: u64,
}

// ----- Consume Messages -----

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsumeRequest {
    pub topic_name: String,
    pub consumer_group: String,
    pub timeout: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsumeResponse {
    pub success: bool,
    pub topic_name: String,
    pub messages: Vec<Message>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub timestamp: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

// ----- Invalid message -----

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvalidResponse {
    pub error_message: String,
}