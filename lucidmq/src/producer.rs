use crate::{topic::Topic, lucidmq_errors::ProducerError};
use log::error;
use nolan::StoredRecord;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Producer {
    topic: Arc<RwLock<Topic>>,
}

impl Producer {
    /// Initialize a new producer
    pub fn new(producer_topic: Arc<RwLock<Topic>>) -> Producer {
        Producer {
            topic: producer_topic,
        }
    }

    /// Produce a typed stored record to the commitlog-backed store.
    pub fn produce_record(&mut self, record: StoredRecord) -> Result<u16, ProducerError> {
        let written_offset = self
            .topic
            .write()
            .unwrap()
            .commitlog
            .append_record(record)
            .map_err(|e| {
                error!("{}", e);
                ProducerError::new("Unable to produce message to the commitlog")
            })?;
        Ok(written_offset)
    }

    /// Upsert a source keyed record with the current wall clock timestamp.
    pub fn upsert(
        &mut self,
        source_id: Vec<u8>,
        parent_source_id: Option<Vec<u8>>,
        payload: Vec<u8>,
    ) -> Result<u16, ProducerError> {
        let timestamp = Self::current_time_millis()?;
        self.produce_record(StoredRecord::upsert(
            source_id,
            parent_source_id,
            payload,
            timestamp,
        ))
    }

    /// Append a delete tombstone for a source keyed record with the current wall clock timestamp.
    pub fn delete(
        &mut self,
        source_id: Vec<u8>,
        parent_source_id: Option<Vec<u8>>,
    ) -> Result<u16, ProducerError> {
        let timestamp = Self::current_time_millis()?;
        self.produce_record(StoredRecord::delete(
            source_id,
            parent_source_id,
            timestamp,
        ))
    }

    pub fn _get_topic(&self) -> String {
        self.topic.read().unwrap().name.clone()
    }

    fn current_time_millis() -> Result<u64, ProducerError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                error!("{}", e);
                ProducerError::new("Unable to get current timestamp")
            })?;
        u64::try_from(duration.as_millis()).map_err(|e| {
            error!("{}", e);
            ProducerError::new("Unable to convert current timestamp")
        })
    }
}

#[cfg(test)]
mod producer_tests {
    use std::sync::{Arc, RwLock};
    use crate::lucidmq_errors::ProducerError;
    use crate::topic::Topic;
    use crate::producer::Producer;
    use nolan::StoredRecord;
    use tempdir::TempDir;

    #[test]
    fn test_producer_produce_record() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_string = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let topic = Topic::new(
            "test_topic".to_string(),
            String::from(tmp_dir_string),
            256,
            1024,
        ).unwrap();

        let locked_topic = Arc::new(RwLock::new(topic));
        let mut producer = Producer::new(locked_topic.clone());
        let record = StoredRecord::upsert(
            b"source-1".to_vec(),
            None,
            b"hello".to_vec(),
            1000,
        );
        // check the offset
        let offset = producer.produce_record(record.clone()).expect("Unable to produce record");
        assert!(offset == 0);
        // check the message provided
        let msg = locked_topic
            .write()
            .expect("unable to get lock")
            .commitlog
            .read_record(0)
            .expect("unable to read commitlog");
        assert_eq!(record, msg);
    }

    #[test]
    fn test_producer_upsert() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_string = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let topic = Topic::new(
            "test_topic".to_string(),
            String::from(tmp_dir_string),
            256,
            1024,
        ).unwrap();

        let locked_topic = Arc::new(RwLock::new(topic));
        let mut producer = Producer::new(locked_topic.clone());
        let offset = producer
            .upsert(
                b"source-1".to_vec(),
                Some(b"parent-1".to_vec()),
                b"hello".to_vec(),
            )
            .expect("Unable to upsert record");

        assert_eq!(0, offset);
        let msg = locked_topic
            .write()
            .expect("unable to get lock")
            .commitlog
            .read_record(0)
            .expect("unable to read commitlog");
        assert_eq!(b"source-1".to_vec(), msg.source_id);
        assert_eq!(Some(b"parent-1".to_vec()), msg.parent_source_id);
        assert_eq!(Some(b"hello".to_vec()), msg.payload);
        assert!(msg.timestamp > 0);
    }

    #[test]
    fn test_producer_delete() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_string = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let topic = Topic::new(
            "test_topic".to_string(),
            String::from(tmp_dir_string),
            256,
            1024,
        ).unwrap();

        let locked_topic = Arc::new(RwLock::new(topic));
        let mut producer = Producer::new(locked_topic.clone());
        let offset = producer
            .delete(b"source-1".to_vec(), Some(b"parent-1".to_vec()))
            .expect("Unable to delete record");

        assert_eq!(0, offset);
        let msg = locked_topic
            .write()
            .expect("unable to get lock")
            .commitlog
            .read_record(0)
            .expect("unable to read commitlog");
        assert_eq!(b"source-1".to_vec(), msg.source_id);
        assert_eq!(Some(b"parent-1".to_vec()), msg.parent_source_id);
        assert_eq!(None, msg.payload);
        assert!(msg.timestamp > 0);
    }

    #[test]
    fn test_producer_produce_record_greater_than_segment_size() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_string = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let topic = Topic::new(
            "test_topic".to_string(),
            String::from(tmp_dir_string),
            10,
            100,
        ).unwrap();

        let locked_topic = Arc::new(RwLock::new(topic));
        let mut producer = Producer::new(locked_topic.clone());
        let record = StoredRecord::upsert(
            b"source-1".to_vec(),
            None,
            vec![0; 11],
            1000,
        );
        let producer_error = producer.produce_record(record).unwrap_err();
        let wanted_error =
            ProducerError::new("Unable to produce message to the commitlog");
        assert_eq!(wanted_error, producer_error);
    }

    #[test]
    fn test_producer_produce_multiple_records() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_string = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let topic = Topic::new(
            "test_topic".to_string(),
            String::from(tmp_dir_string),
            256,
            2048,
        ).unwrap();

        let locked_topic = Arc::new(RwLock::new(topic));
        let mut producer = Producer::new(locked_topic.clone());
        for i in 0u16..10 {
            let string_message = format!("hellow{}", i);
            let record = StoredRecord::upsert(
                format!("source-{}", i).into_bytes(),
                None,
                string_message.as_bytes().to_vec(),
                u64::from(i),
            );
            // check the offset
            let offset = producer
                .produce_record(record.clone())
                .expect("Unable to produce record");
            assert!(offset == i);
            // check the message provided
            let msg = locked_topic
                .write()
                .expect("unable to get lock")
                .commitlog
                .read_record(usize::from(i))
                .expect("unable to read commitlog");
            assert_eq!(record, msg);
        }
    }

    #[test]
    fn test_producer_produce_record_vector() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_string = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let topic = Topic::new(
            "test_topic".to_string(),
            String::from(tmp_dir_string),
            256,
            2048,
        ).unwrap();

        let locked_topic = Arc::new(RwLock::new(topic));
        let mut producer = Producer::new(locked_topic.clone());
        let mut msg_vec: Vec<StoredRecord>  = Vec::new();
        let mut last_offset = 0;
        for i in 0u16..10 {
            let string_message = format!("hellow{}", i);
            let record = StoredRecord::upsert(
                format!("source-{}", i).into_bytes(),
                None,
                string_message.as_bytes().to_vec(),
                u64::from(i),
            );
            last_offset = producer
                .produce_record(record.clone())
                .expect("Unable to produce record");
            msg_vec.push(record);
        }
        // check the offset
        let thin= u16::try_from(msg_vec.len()-1).expect("Unable to convert u16");
        assert!(last_offset == thin);
        for (i, msg) in msg_vec.iter().enumerate() {
            // check the message provided
            let commitlog_msg = locked_topic
                .write()
                .expect("unable to get lock")
                .commitlog
                .read_record(i.into())
                .expect("unable to read commitlog");
            assert_eq!(&commitlog_msg, msg);
        }
    }

}
