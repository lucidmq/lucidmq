use crate::consumer::{Consumer};
use crate::producer::Producer;
use log::{info};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::str;
use std::sync::RwLock;
use std::sync::atomic::Ordering;
use std::{sync::atomic::AtomicU32, sync::Arc};

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsumerGroup {
    name: String,
    pub offset: AtomicU32,
}

impl ConsumerGroup {
    pub fn new(consumer_group_name: String) -> ConsumerGroup {
        let consumer_group = ConsumerGroup {
            name: consumer_group_name,
            offset: 0.into(),
        };
        return consumer_group;
    }

    pub fn new_cg(consumer_group_name: String, offset_in: AtomicU32) -> ConsumerGroup {
        let consumer_group = ConsumerGroup {
            name: consumer_group_name,
            offset: offset_in,
        };
        return consumer_group;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Topic {
    name: String,
    directory: String,
    consumer_groups: Vec<Arc<ConsumerGroup>>,
}

impl Topic {
    pub fn new(topic_name: String, base_directory: String) -> Topic {
        let path = Path::new(&base_directory);
        // Generate a random directory name
        let directory_name: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(5)
            .map(char::from)
            .collect();
        let new_path = &path.join(directory_name);
        let consumer_groups = Vec::new();
        let topic = Topic {
            name: topic_name,
            directory: new_path
                .to_str()
                .expect("unable to convert to string")
                .to_string(),
            consumer_groups: consumer_groups,
        };
        return topic;
    }

    pub fn load_consumer_group(&mut self, consumer_group_name: String) -> Arc<ConsumerGroup> {
        for group in &self.consumer_groups {
            if group.name == consumer_group_name {
                return group.clone();
            }
        }
        let new_gc = Arc::new(ConsumerGroup::new(consumer_group_name));
        self.consumer_groups.push(new_gc.clone());
        return new_gc;
    }

    pub fn new_topic_from_ref(topic_ref: &Topic) -> Topic {
        let mut consumer_groups = Vec::new();
        for cg in &topic_ref.consumer_groups {
            consumer_groups.push(cg.clone());
        }
        let topic = Topic {
            name: topic_ref.name.clone(),
            directory: topic_ref.directory.clone(),
            consumer_groups: consumer_groups
        };
        return topic;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LucidMQ {
    base_directory: String,
    topics: Arc<RwLock<Vec<Topic>>>,
}

impl LucidMQ {
    pub fn new(directory: String) -> LucidMQ {
        //Try to load from file
        let lucidmq_file_path = Path::new(&directory).join("lucidmq.meta");
        let file_bytes = fs::read(lucidmq_file_path);
        match file_bytes {
            Ok(bytes) => {
                let decoded_lucidmq: LucidMQ =
                    bincode::deserialize(&bytes).expect("Unable to deserialize message");
                return decoded_lucidmq;
            }
            Err(_err) => {
                let lucidmq_vec = Vec::new();
                let lucidmq = LucidMQ {
                    base_directory: directory.clone(),
                    topics: Arc::new(RwLock::new(lucidmq_vec)),
                };
                fs::create_dir_all(directory).expect("Unable to create directory");
                return lucidmq;
            }
        }
    }

    pub fn new_producer(&mut self, topic: String) -> Producer {
        let found_index = self.check_topics(&topic);
        if found_index >= 0 {
            let usize_index: usize = found_index.try_into().expect("unable to convert");
            let found_topic = &self.topics.read().unwrap()[usize_index];
            let producer = Producer::new(found_topic.directory.clone(), found_topic.name.clone());
            return producer;
        } else {
            let new_topic = Topic::new(topic, self.base_directory.clone());
            let producer = Producer::new(new_topic.directory.clone(), new_topic.name.clone());
            {
                self.topics.write().unwrap().push(new_topic);
            }
            self.flush();
            return producer;
        }
    }

    pub fn new_consumer(mut self, topic: String, consumer_group_name: String) -> Consumer {
        let found_index = self.check_topics(&topic);
        if found_index >= 0 {
            let usize_index: usize = found_index.try_into().expect("unable to convert");
            let consumer_cg: Arc<ConsumerGroup>;
            let directory: String;
            let topic_name: String;
            {
                let found_topic = &mut self.topics.write().unwrap()[usize_index];
                directory = found_topic.directory.clone();
                topic_name = found_topic.name.clone();
                consumer_cg = found_topic.load_consumer_group(consumer_group_name.clone());
            }
            let consumer = Consumer::new(
                directory,
                topic_name,
                consumer_cg,
                Box::new(move || self.sync(consumer_group_name.clone())),
            );
            return consumer;
        } else {
            let user_cg = Arc::new(ConsumerGroup::new(consumer_group_name.clone()));
            let mut new_topic = Topic::new(topic, self.base_directory.clone());
            new_topic.consumer_groups.push(user_cg.clone());
            let new_directory_name = new_topic.directory.clone();
            let new_topic_name = new_topic.name.clone();
            {
                self.topics.write().unwrap().push(new_topic);
            }
            let consumer = Consumer::new(
                new_directory_name,
                new_topic_name,
                user_cg,
                Box::new(move || self.sync(consumer_group_name.clone())),
            );
            return consumer;
        }
    }

    fn check_topics(&mut self, topic_to_find: &String) -> i8 {
        if self.topics.read().unwrap().is_empty() {
            return -1;
        }
        let indexed_value = &self.topics.read().unwrap().iter().position(|topic| topic.name == *topic_to_find);
        match indexed_value {
            None => return -1,
            Some(index) => {
                return i8::try_from(*index).unwrap();
            },
        }
    }

    fn sync(&self, consumer_group_in_use: String) {
        let lucidmq_file_path = Path::new(&self.base_directory).join("lucidmq.meta");
        let file_bytes = fs::read(lucidmq_file_path);
        match file_bytes {
            Ok(bytes) => {
                let decoded_lucidmq: LucidMQ =
                    bincode::deserialize(&bytes).expect("Unable to deserialize message");
                for topic in decoded_lucidmq.topics.read().unwrap().iter() {
                    let indexed_value = self.topics.read().unwrap().iter().position(|self_topic| self_topic.name == topic.name);
                    match indexed_value {
                        None => {
                            let topic_to_add = Topic::new_topic_from_ref(topic);
                            self.topics.write().unwrap().push(topic_to_add);
                        },
                        Some(index) => {
                            let found_topic = &mut self.topics.write().unwrap()[index];
                            for cg in topic.consumer_groups.iter() {
                                let found_cg= found_topic.consumer_groups.iter().find(|self_cg| self_cg.name == cg.name);
                                match found_cg {
                                    None => {
                                        found_topic.consumer_groups.push(cg.clone());
                                    }
                                    Some(consumer_group) => {
                                        if consumer_group.name == consumer_group_in_use {
                                            continue;
                                        }
                                        let mut self_current_offset = consumer_group.offset.load(Ordering::SeqCst);
                                        let saved_current_offset = cg.offset.load(Ordering::SeqCst);
                                        //info!("Consumer Group updating {} current: {:?} new: {:?}", consumer_group.name, self_current_offset, saved_current_offset);
                                        loop {
                                            let res = consumer_group.offset.compare_exchange(self_current_offset, saved_current_offset, Ordering::SeqCst, Ordering::SeqCst);
                                            match res {
                                                Ok(_placeholder) => {
                                                    break;
                                                },
                                                Err(value) => {
                                                    //warn!("Unable to update consumer group offset {:?}", res);
                                                    self_current_offset = value;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                panic!("{}", err)
            }
        }
        self.flush()
    }

    fn flush(&self) {
        info!("Saving lucidmq state to file...");
        let lucidmq_file_path = Path::new(&self.base_directory).join("lucidmq.meta");
        let encoded_data: Vec<u8> =
            bincode::serialize(&self).expect("Unable to encode lucidmq metadata");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(false)
            .open(lucidmq_file_path)
            .expect("Unable to create and open file");

        file.write_all(&encoded_data)
            .expect("Unable to write to file");
    }
}
