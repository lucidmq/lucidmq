use std::fmt::Write;
use capnp::{serialize_packed, message::ReaderOptions};
use crate::lucid_schema_capnp::{message_envelope, topic_response};

pub fn parse_response(data: Vec<u8>) -> String {
    let reader = serialize_packed::read_message(data.as_slice(), ReaderOptions::new()).unwrap();
    let message_envelope = reader.get_root::<message_envelope::Reader>().unwrap();
    match message_envelope.which() {
        Ok(message_envelope::TopicResponse(envelope_topic_response)) => {
            let topic_response = envelope_topic_response.expect("Unable to get topic request from envelope");
            match topic_response.which() {
                Ok(topic_response::Which::Create(_create)) => {
                    let mut s = "Topic Create Response ------------\n".to_string();
                    write!(s, "Topic Name: {}\n", topic_response.get_topic_name().unwrap()).unwrap();
                    write!(s, "Status: {}\n", topic_response.get_success()).unwrap();
                    return s;
                },
                Ok(topic_response::Which::Describe(describe)) => {
                    let mut s = "Topic Describe Response ------------\n".to_string();
                    write!(s, "Topic Name: {}\n", topic_response.get_topic_name().unwrap()).unwrap();
                    write!(s, "Status: {}\n", topic_response.get_success()).unwrap();
                    // Parse out the consumer groups to a vector to write them pretty
                    let cgs = describe.get_consumer_groups().unwrap();
                    let mut cgs_vec = Vec::new();
                    for msg in cgs {
                        cgs_vec.push(msg.unwrap().to_string())
                    }
                    write!(s, "Topic max retention bytes: {}, max segments bytes: {}, consumer groups: {:?}\n", describe.get_max_retention_bytes(), describe.get_max_segment_bytes(), cgs_vec).unwrap();
                    return s;
                },
                Ok(topic_response::Which::Delete(_deletes)) => {
                    let mut s = "Topic Delete Response ------------\n".to_string();
                    write!(s, "Topic Name: {}\n", topic_response.get_topic_name().unwrap()).unwrap();
                    write!(s, "Status: {}\n", topic_response.get_success()).unwrap();
                    return s;
                },
                Ok(topic_response::Which::All(all)) => {
                    let unwraped_all = all.unwrap();
                    // this thing is fucked
                    let mut s = "Topic All Response ------------\n".to_string();
                    write!(s, "Status: {}\n", topic_response.get_success()).unwrap();
                    for i in 0..unwraped_all.len() {
                        let topic = unwraped_all.get(i);
                        let cgs = topic.get_consumer_groups().unwrap();
                        let mut cgs_vec = Vec::new();
                        for msg in cgs {
                            cgs_vec.push(msg.unwrap().to_string())
                        }
                        write!(s, "Topic Name: {}, consumer groups: {:?}\n", topic.get_topic_name().unwrap(), cgs_vec).unwrap();
                    }
                    return s;
                },
                Err(_) => unimplemented!(),
            }
        },
        Ok(message_envelope::ProduceResponse(envelope_produce_response)) => {
            let produce_response = envelope_produce_response.expect("Unable to get produce request from envelope");
            let mut s = "Produce Response ------------\n".to_string();
            write!(s, "Topic Name: {}\n", produce_response.get_topic_name().unwrap()).unwrap();
            write!(s, "Status: {}\n", produce_response.get_success()).unwrap();
            write!(s,"Last offset: {}\n", produce_response.get_offset()).unwrap();
            return s;
        },
        Ok(message_envelope::ConsumeResponse(envelope_consume_response)) => {
            let consume_response = envelope_consume_response.unwrap();
            let mut s = "Consume Response ------------\n".to_string();
            write!(s, "Topic Name: {}\n", consume_response.get_topic_name().unwrap()).unwrap();
            write!(s, "Status: {}\n", consume_response.get_success()).unwrap(); 

            let messages = consume_response.get_messages().unwrap();
            let mut message_vec = Vec::new();
            for msg in messages {
                let message_string = format!("Key: {:?},Value: {:?}, Timestamp: {}", msg.get_key().unwrap(), msg.get_value().unwrap(), msg.get_timestamp());
                message_vec.push(message_string)
            }
            write!(s, "Messages: {:?}\n", message_vec).unwrap();
            return s;
        },
        Ok(message_envelope::Which::InvalidResponse(envelope_invalid_request)) => {
            let invalid_response = envelope_invalid_request.unwrap();
            let invalid_response_text = invalid_response.get_error_message().unwrap();
            return invalid_response_text.to_string();
        }
        Ok(message_envelope::TopicRequest(_envelope_topic_request)) => {
            return "Topic Request is an invalid response type\n".to_string();
        },
        Ok(message_envelope::ConsumeRequest(_envelope_produce_request)) => {
            return "Consume request is an invalid request type\n".to_string();
        },
        Ok(message_envelope::ProduceRequest(_envelope_consume_request)) => {
            return "Produce request is an invalid request type\n".to_string();
        },
        Err(::capnp::NotInSchema(_)) => {
            return "Unable to parse cap n p message\n".to_string();
        }
    }
}