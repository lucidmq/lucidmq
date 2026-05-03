use crate::msgpack_helper::{
    new_consume_response, new_invalid_response, new_produce_response, new_state_response,
    new_topic_response_all, new_topic_response_create, new_topic_response_delete,
    new_topic_response_describe,
};
use crate::messages::{ConsumeRequest, ProduceRequest, StateAction, StateRequest, TopicAction, TopicRequest};
use crate::{
    consumer::Consumer, producer::Producer, topic::Topic, types::Command, types::SenderType,
    types::RecieverType, topic::SimpleTopic
};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::lucidmq_errors::BrokerError;

/// The brain of the operation. It is responsible for data about topics and how to run correspoding commands on them.
#[derive(Serialize, Deserialize, Clone)]
#[serde(from = "DeserBroker")]
pub struct Broker {
    pub base_directory: String,
    topics: Arc<RwLock<Vec<Arc<RwLock<Topic>>>>>,
}

#[derive(Deserialize)]
struct DeserBroker {
    pub base_directory: String,
    topics: Arc<RwLock<Vec<Arc<RwLock<Topic>>>>>,
}

impl From<DeserBroker> for Broker {
    fn from(tmp: DeserBroker) -> Self {
        Self {
            base_directory: tmp.base_directory,
            topics: tmp.topics,
        }
    }
}

impl Broker {
    /// Create a new instance of a broker
    pub fn new(directory: String) -> Result<Broker, BrokerError> {
        debug!("Creating new instance of lucidmq in {}", directory);
        //Try to load from file
        let lucidmq_file_path = Path::new(&directory).join("lucidmq.meta");
        let file_bytes = fs::read(lucidmq_file_path);
        match file_bytes {
            Ok(bytes) => {
                let decoded_lucidmq: Broker = bincode::deserialize(&bytes).map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to deserialize lucidmq.meta file")
                })?;
                Ok(decoded_lucidmq)
            }
            Err(_err) => {
                info!(
                    "Lucid meta data file does not exist in directory {} creating a new file",
                    directory
                );
                let lucidmq_vec = Vec::new();
                let lucidmq = Broker {
                    base_directory: directory.clone(),
                    topics: Arc::new(RwLock::new(lucidmq_vec)),
                };
                fs::create_dir_all(directory).map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to create lucidmq directory")
                })?;
                Ok(lucidmq)
            }
        }
    }

    /// Run starts a logic that loops and monitors the reciever channel which is being fed message commands by the server thread. 
    /// This message is parsred into a rusulting action to do work on a resulting topic. 
    pub async fn run(mut self, mut reciever: RecieverType, sender: SenderType) -> Result<(), BrokerError> {
        info!("Broker is running");
        while let Some(command) = reciever.recv().await {
            info!("message came through {:?}", command);
            let response_command = match command {
                Command::TopicRequest {
                    conn_id,
                    request,
                } => {
                    let result_data = self.handle_topic(request).await;
                    match result_data {
                        Ok(data) => {
                            Command::Response {
                                conn_id: conn_id,
                                capmessagedata: data,
                            }
                        }
                        Err(err) => {
                            let error_string = err.to_string();
                            let data = self.handle_invalid_message(&error_string).await?;
                            Command::Invalid {
                                conn_id: conn_id,
                                error_message: error_string,
                                capmessage_data: data
                            }
                        },
                    }
                }
                Command::ProduceRequest {
                    conn_id,
                    request,
                } => {
                    let result_data = self.handle_producer(request).await;
                    match result_data {
                        Ok(data) => {
                            Command::Response {
                                conn_id: conn_id,
                                capmessagedata: data,
                            }
                        },
                        Err(err) => {
                            let error_string = err.to_string();
                            let data = self.handle_invalid_message(&error_string).await?;
                            Command::Invalid {
                                conn_id: conn_id,
                                error_message: error_string,
                                capmessage_data: data
                            }
                        }
                    }

                }
                Command::ConsumeRequest {
                    conn_id,
                    request,
                } => {
                    let result_data = self.handle_consumer(request).await;
                    match result_data {
                        Ok(data) =>{
                            Command::Response {
                                conn_id: conn_id,
                                capmessagedata: data,
                            }
                        }                     
                        Err(err) => {
                            let error_string = err.to_string();
                            let data = self.handle_invalid_message(&error_string).await?;
                            Command::Invalid {
                                conn_id: conn_id,
                                error_message: error_string,
                                capmessage_data: data
                            }
                        },
                    }

                }
                Command::StateRequest {
                    conn_id,
                    request,
                } => {
                    let result_data = self.handle_state(request).await;
                    match result_data {
                        Ok(data) => {
                            Command::Response {
                                conn_id,
                                capmessagedata: data,
                            }
                        }
                        Err(err) => {
                            let error_string = err.to_string();
                            let data = self.handle_invalid_message(&error_string).await?;
                            Command::Invalid {
                                conn_id,
                                error_message: error_string,
                                capmessage_data: data
                            }
                        }
                    }
                }
                Command::Invalid { conn_id, error_message,  capmessage_data:_} => {
                    let data = self.handle_invalid_message(&error_message).await?;
                    Command::Invalid {
                        conn_id: conn_id,
                        error_message: error_message,
                        capmessage_data: data
                    }
                }
                Command::Response { conn_id, capmessagedata:_ } => {
                    warn!("Response type unexected command");
                    let data = self.handle_invalid_message("Response message is invalid").await?;
                    Command::Invalid {
                        conn_id: conn_id,
                        error_message: "Response message is invalid".to_string(),
                        capmessage_data: data
                    }
                    
                },
            };
            let res = sender.send(response_command).await;
            match res {
                Err(e) => {
                    error!("{}", e);
                    return Err(BrokerError::new("Unable to send message"));
                }
                Ok(_) => {}
            }
        }
        Ok(())
    }

    /// Given a topic command type. Parse that further into topic command actions.
    async fn handle_topic(
        &mut self,
        topic_request: TopicRequest,
    ) -> Result<Vec<u8>, BrokerError> {
        let topic_name = &topic_request.topic_name;
        match topic_request.request_type {
            TopicAction::Create => {
                Ok(self.handle_create_topic(topic_name))?
            }
            TopicAction::Delete => {
                Ok(self.handle_delete_topic(topic_name))?
            }
            TopicAction::Describe => {
                Ok(self.handle_describe_topic(topic_name))?
            },
            TopicAction::All => {
                Ok(self.handle_all_topic())?
            },
        }
    }

    /// Given a topic name, create a new topic
    /// TODO: we need segment size and topic size to be configurable instead of hard coded.
    fn handle_create_topic(&mut self, topic_name: &str) -> Result<Vec<u8>, BrokerError> {
        let found_index = self.check_topics(topic_name);
        match found_index {
            Some(_) => {
                warn!("topic already exisits");
                Ok(new_topic_response_create(topic_name, false))
            }
            None => {
                let topic = Topic::new(
                    topic_name.to_string(),
                    self.base_directory.clone(),
                    100000, //100kb
                    1000000, //1mb
                ).map_err(|err| {
                    error!("{}", err);
                    BrokerError::new("Unable to create topic directory")
                })?;
                fs::create_dir_all(&topic.directory).map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to create topic directory")
                })?;
                {
                    self.topics
                        .write()
                        .map_err(|e| {
                            error!("{}", e);
                            BrokerError::new("Unable to get write lock on topics")
                        })?
                        .push(Arc::new(RwLock::new(topic)));
                }
                self.flush()?;
                Ok(new_topic_response_create(topic_name, true))
            }
        }
    }

    /// Given a topic name, write a descibe topic protocol message and return it's serialized byte representation.
    fn handle_describe_topic(&mut self, topic_name: &str) -> Result<Vec<u8>, BrokerError> {
        let found_index = self.check_topics(topic_name);
        match found_index {
            Some(ind) => {
                let topics = self.topics.read().map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to get read lock on topics")
                })?;
                let topic = topics.get(ind).unwrap().read().map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to get read lock on single topic")
                })?;
                let cgs = topic.get_consumer_groups();
                let max_segment_size = topic.get_max_segment_size();
                info!("{}, {:?}, {}", topic_name, cgs, max_segment_size);
                Ok(new_topic_response_describe(
                    topic_name,
                    true,
                    topic.max_topic_size,
                    topic.max_segment_size,
                    cgs,
                ))
            }
            None => {
                warn!("topic does not exist");
                let dummy_vec = Vec::new();
                Ok(new_topic_response_describe(
                    topic_name, false, 0, 0, dummy_vec,
                ))
            }
        }
    }

    /// Given a topic name, delete a topic if it exists and creat a delete topic protocol message and return it's serialized byte representation.
    fn handle_delete_topic(&mut self, topic_name: &str) -> Result<Vec<u8>, BrokerError> {
        let found_index = self.check_topics(topic_name);
        match found_index {
            Some(ind) => {
                // Get the topic directory
                let topics = self.topics.read().map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to get read lock on topics")
                })?;
                let topic = topics.get(ind).unwrap().read().map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to get read lock on single topic")
                })?;
                let topic_directory = &topic.directory.clone();
                // Clean up access since we dont need them anymore
                drop(topic);
                drop(topics);
                // Remove the topic from the topic vector
                self.topics
                    .write()
                    .map_err(|e| {
                        error!("{}", e);
                        BrokerError::new("Unable to get write lock on topics")
                    })?
                    .remove(ind);
                fs::remove_dir_all(topic_directory).map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to delete diretory of topic")
                })?;
                self.flush()?;
                Ok(new_topic_response_delete(topic_name, true))
            }
            None => {
                warn!("topic does not exist");
                Ok(new_topic_response_delete(topic_name, false))
            }
        }
    }

    /// Write a all topic protocol message and return it's serialized byte representation.
    fn handle_all_topic(&mut self) -> Result<Vec<u8>, BrokerError> {
        if self.topics.read().map_err(|e| {
            error!("{}", e);
            BrokerError::new("Unable to get read lock on topics")
        })?.is_empty(){
            return Err(BrokerError::new("Unable to get topic name from consume request"));
        }
        let mut simple_topics = Vec::new();
        for topic in self.topics.read().expect("unable to get topic lock").iter() {
            let topic_name = &topic.read().unwrap().name;
            let consumer_groups = topic.read().unwrap().get_consumer_groups();
            let st = SimpleTopic {
                topic_name: topic_name.to_string(),
                consumer_groups: consumer_groups
            };
            simple_topics.push(st)
        }
        let response_data = new_topic_response_all(true, simple_topics);
        Ok(response_data)
    }

    async fn handle_consumer(
        &mut self,
        consume_request: ConsumeRequest,
    ) -> Result<Vec<u8>, BrokerError> {
        info!("Handling consumer message");
        let topic_name = &consume_request.topic_name;
        let consumer_group = &consume_request.consumer_group;
        let timeout = consume_request.timeout;

        let found_index = self.check_topics(topic_name);
        match found_index {
            Some(x) => {
                let broker = self.clone();
                let found_topic = &self.topics.read().map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to get read lock on topic")
                })?[x];
                let consumer_group_arc = found_topic
                    .write()
                    .map_err(|e| {
                        error!("{}", e);
                        BrokerError::new("Unable to get wrote lock on topic")
                    })?
                    .load_consumer_group(consumer_group);
                let mut consumer = Consumer::new(
                    found_topic.clone(),
                    consumer_group_arc,
                    Box::new(move || broker.flush()),
                ).map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to create new consumer")
                })?;
                let messages = consumer.poll(timeout).map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to poll consumers commitlog")
                })?;
                let data = new_consume_response(topic_name, true, messages);
                Ok(data)
            }
            None => {
                warn!("topic does not exist");
                let message_data = Vec::new();
                let data = new_consume_response(topic_name, false, message_data);
                Ok(data)
            }
        }
    }

    async fn handle_state(
        &mut self,
        state_request: StateRequest,
    ) -> Result<Vec<u8>, BrokerError> {
        info!("Handling state request");
        let StateRequest {
            topic_name,
            action,
            source_id,
            parent_source_id,
        } = state_request;

        let found_index = self.check_topics(&topic_name);
        match found_index {
            Some(index) => {
                let topics = self.topics.read().map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to get read lock on topic")
                })?;
                let topic = topics[index].read().map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to get read lock on single topic")
                })?;

                let (record, records) = match action {
                    StateAction::Get => {
                        let requested_source_id = source_id.as_deref().ok_or_else(|| {
                            BrokerError::new("source_id is required for Get state requests")
                        })?;
                        (topic.commitlog.get(requested_source_id), Vec::new())
                    }
                    StateAction::GetChildren => {
                        let requested_parent_source_id = parent_source_id.as_deref().ok_or_else(|| {
                            BrokerError::new("parent_source_id is required for GetChildren state requests")
                        })?;
                        (None, topic.commitlog.get_children(requested_parent_source_id))
                    }
                    StateAction::ScanCurrent => (None, topic.commitlog.scan_current()),
                };

                Ok(new_state_response(&topic_name, true, action, record, records))
            }
            None => Ok(new_state_response(
                &topic_name,
                false,
                action,
                None,
                Vec::new(),
            )),
        }
    }

    async fn handle_producer(
        &mut self,
        produce_request: ProduceRequest,
    ) -> Result<Vec<u8>, BrokerError> {
        info!("Handling producer message");
        let topic_name = &produce_request.topic_name;
        let found_index = self.check_topics(topic_name);
        match found_index {
            Some(x) => {
                let found_topic = &self.topics.read().map_err(|e| {
                    error!("{}", e);
                    BrokerError::new("Unable to get reader for topics")
                })?[x];
                let mut producer = Producer::new(found_topic.clone());
                
                let mut last_offset = 0;
                for msg in produce_request.messages {
                    last_offset = producer.produce_record(msg).map_err(|e| {
                        error!("{}", e);
                        BrokerError::new("Unable to produce message to commitlog")
                    })?;
                }
                Ok(new_produce_response(topic_name, last_offset.into(), true))
            }
            None => {
                warn!("Topic {} does not exist", topic_name);
                Ok(new_produce_response(topic_name, 0, false))
            }
        }
    }

    async fn handle_invalid_message(&self, message_text: &str) -> Result<Vec<u8>, BrokerError> {
        let data = new_invalid_response(message_text);
        Ok(data)
    }

    fn check_topics(&mut self, topic_to_find: &str) -> Option<usize> {
        if self
            .topics
            .read()
            .expect("unable to get read lock")
            .is_empty()
        {
            return None;
        }
        let indexed_value = &self
            .topics
            .read()
            .expect("unable to get read lock")
            .iter()
            .position(|topic| {
                topic
                    .read()
                    .expect("Unable to read topic from read write lock")
                    .name
                    == *topic_to_find
            });
        match indexed_value {
            None => None,
            Some(index) => Some(*index),
        }
    }

    fn flush(&self) -> Result<(), BrokerError>{
        let lucidmq_file_path = Path::new(&self.base_directory).join("lucidmq.meta");
        info!(
            "Saving lucidmq state to file {}",
            lucidmq_file_path.to_string_lossy()
        );
        let encoded_data: Vec<u8> =
            bincode::serialize(&self).map_err(|err| {
                error!("{}", err);
                BrokerError::new("Unable to encode lucidmq metadata")
            })?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(false)
            .write(true)
            .append(false)
            .open(lucidmq_file_path)
            .map_err(|err| {
                error!("{}", err);
                BrokerError::new("Unable to open to lucidmq.meta file for writing")
            })?;
        file.write_all(&encoded_data)
            .map_err(|err| {
                error!("{}", err);
                BrokerError::new("Unable to write to file lucidmq.meta file")
            })?;
        Ok(())
    }
}
