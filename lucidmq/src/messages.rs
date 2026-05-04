use serde::{Serialize, Deserialize};
pub use nolan::{RecordOp, StoredRecord};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MessageEnvelope {
    TopicRequest(TopicRequest),
    TopicResponse(TopicResponse),
    ProduceRequest(ProduceRequest),
    ProduceResponse(ProduceResponse),
    ConsumeRequest(ConsumeRequest),
    ConsumeResponse(ConsumeResponse),
    StateRequest(StateRequest),
    StateResponse(StateResponse),
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProduceMessage {
    pub timestamp: u64,
    pub source_id: Vec<u8>,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub parent_source_id: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProduceRequest {
    pub topic_name: String,
    pub messages: Vec<ProduceMessage>,
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
    pub messages: Vec<StoredRecord>,
}

// ----- State Store Messages -----

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StateAction {
    Get,
    GetChildren,
    ScanCurrent,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StateRequest {
    pub topic_name: String,
    pub action: StateAction,
    #[serde(default)]
    pub source_id: Option<Vec<u8>>,
    #[serde(default)]
    pub parent_source_id: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StateResponse {
    pub success: bool,
    pub topic_name: String,
    pub action: StateAction,
    #[serde(default)]
    pub record: Option<StoredRecord>,
    #[serde(default)]
    pub records: Vec<StoredRecord>,
}

// ----- Invalid message -----

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvalidResponse {
    pub error_message: String,
}
