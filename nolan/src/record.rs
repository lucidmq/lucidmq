use serde::{Deserialize, Serialize};

use crate::nolan_errors::RecordError;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RecordOp {
    Upsert,
    Delete,
}

/// StoredRecord is the canonical on-disk and wire shape used by Nolan for
/// keyed records.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    pub timestamp: u64,
    pub source_id: Vec<u8>,
    #[serde(default)]
    pub payload: Option<Vec<u8>>,
    #[serde(default)]
    pub parent_source_id: Option<Vec<u8>>,
    pub op: RecordOp,
}

impl StoredRecord {
    pub fn upsert(
        source_id: Vec<u8>,
        parent_source_id: Option<Vec<u8>>,
        payload: Vec<u8>,
        timestamp: u64,
    ) -> StoredRecord {
        StoredRecord {
            timestamp,
            source_id,
            payload: Some(payload),
            parent_source_id,
            op: RecordOp::Upsert,
        }
    }

    pub fn delete(
        source_id: Vec<u8>,
        parent_source_id: Option<Vec<u8>>,
        timestamp: u64,
    ) -> StoredRecord {
        StoredRecord {
            timestamp,
            source_id,
            payload: None,
            parent_source_id,
            op: RecordOp::Delete,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, RecordError> {
        bincode::serialize(self).map_err(|_e| RecordError::new("Unable to serialize stored record"))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<StoredRecord, RecordError> {
        bincode::deserialize(bytes)
            .map_err(|_e| RecordError::new("Unable to deserialize stored record"))
    }
}

#[cfg(test)]
mod record_tests {
    use crate::record::{RecordOp, StoredRecord};

    #[test]
    fn test_round_trip_upsert() {
        let record = StoredRecord::upsert(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            br#"{"hello":"world"}"#.to_vec(),
            1234,
        );

        let encoded = record.to_bytes().expect("unable to encode record");
        let decoded = StoredRecord::from_bytes(&encoded).expect("unable to decode record");

        assert_eq!(record, decoded);
    }

    #[test]
    fn test_round_trip_delete() {
        let record = StoredRecord::delete(b"source-1".to_vec(), None, 9876);

        let encoded = record.to_bytes().expect("unable to encode record");
        let decoded = StoredRecord::from_bytes(&encoded).expect("unable to decode record");

        assert_eq!(RecordOp::Delete, decoded.op);
        assert_eq!(None, decoded.payload);
        assert_eq!(record, decoded);
    }
}
