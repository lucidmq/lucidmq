//! Nolan is a crate that specifies a implemeation for a commitlog storage structure.
use log::{error, info, warn};
use std::fs;

use crate::compactor::Compactor;
use crate::nolan_errors::{CommitlogError, SegmentError};
use crate::record::{RecordOp, StoredRecord};
use crate::segment::Segment;
use crate::virtual_segment::VirtualSegment;
use crate::utils;
use std::collections::{BTreeSet, HashMap};

/// Commitlog is a struct that represents the logs stored on memory and on disc.
/// At it's core, a commitlog is a directory that is made up of segments(log and index files)
/// that are stored within that directory.
pub struct Commitlog {
    directory: String,
    segments: Vec<Segment>,
    compactor: Compactor,
    max_segment_size: u64,
    current_segment: VirtualSegment,
    latest_by_source: HashMap<Vec<u8>, StoredRecord>,
    children_by_parent: HashMap<Vec<u8>, BTreeSet<Vec<u8>>>,
}

impl Commitlog {
    /// new creates a new commitlog taking in a base directory(where the segments live),
    /// a max segment size in bytes and a compaction threshold in bytes.
    pub fn new(
        base_directory: &str,
        max_segment_size_bytes: u64,
        compaction_threshold_bytes: u64,
    ) -> Result<Commitlog, CommitlogError> {
        let vec = Vec::new();
        let new_compactor = Compactor::new(compaction_threshold_bytes);
        let mut commitlog = Commitlog {
            directory: base_directory.to_string(),
            segments: vec,
            compactor: new_compactor,
            max_segment_size: max_segment_size_bytes,
            current_segment: VirtualSegment::new(&base_directory, max_segment_size_bytes, 0), // This is just a placeholder
            latest_by_source: HashMap::new(),
            children_by_parent: HashMap::new(),
        };
        fs::create_dir_all(base_directory).map_err(|e| {
            error!("{}", e);
            CommitlogError::new("Unable to create commitlog directory")
        })?;
        commitlog.load_segments()?;
        commitlog.rebuild_state_indexes()?;
        Ok(commitlog)
    }

    /// Given bytes, append those bytes onto the current segment. If those bytes will not fit onto the segment,
    /// split the segment by creating a new one and append the bytes there.
    pub fn append(&mut self, data: &[u8]) -> Result<u16, CommitlogError> {
        match self.current_segment.write(data) {
            Ok(segment_offset_written) => {
                self.current_segment.flush().map_err(|e| {
                    error!("{}", e);
                    CommitlogError::new("Unable to flush current segment")
                })?;
                info!("Successfully wrote to segment");
                let commitlog_written_offset =
                    self.current_segment.starting_offset + segment_offset_written;
                self.compact()?;
                Ok(commitlog_written_offset)
            }
            Err(err) => {
                let split_err = SegmentError::new(
                    "Write not possible. Segment log would be greater than max bytes",
                );
                if err == split_err {
                    self.split().expect("Unable to split commitlog");
                    self.append(data)
                } else {
                    Err(CommitlogError::new("Unknown error when writing occured"))
                }
            }
        }
    }

    /// Append a typed stored record and update the in-memory state indexes.
    pub fn append_record(&mut self, record: StoredRecord) -> Result<u16, CommitlogError> {
        let bytes = record.to_bytes().map_err(|e| {
            error!("{}", e);
            CommitlogError::new("Unable to serialize stored record")
        })?;
        match self.current_segment.write(&bytes) {
            Ok(segment_offset_written) => {
                self.current_segment.flush().map_err(|e| {
                    error!("{}", e);
                    CommitlogError::new("Unable to flush current segment")
                })?;
                info!("Successfully wrote stored record to segment");
                let commitlog_written_offset =
                    self.current_segment.starting_offset + segment_offset_written;
                Self::apply_record_to_state_maps(
                    &mut self.latest_by_source,
                    &mut self.children_by_parent,
                    record,
                );
                self.compact()?;
                Ok(commitlog_written_offset)
            }
            Err(err) => {
                let split_err = SegmentError::new(
                    "Write not possible. Segment log would be greater than max bytes",
                );
                if err == split_err {
                    self.split().expect("Unable to split commitlog");
                    self.append_record(record)
                } else {
                    Err(CommitlogError::new("Unknown error when writing occured"))
                }
            }
        }
    }

    /// Upsert a source keyed record into the commitlog-backed store.
    pub fn upsert(
        &mut self,
        source_id: Vec<u8>,
        parent_source_id: Option<Vec<u8>>,
        payload: Vec<u8>,
        last_updated: u64,
    ) -> Result<u16, CommitlogError> {
        self.append_record(StoredRecord::upsert(
            source_id,
            parent_source_id,
            payload,
            last_updated,
        ))
    }

    /// Tombstone a source keyed record in the commitlog-backed store.
    pub fn delete(
        &mut self,
        source_id: Vec<u8>,
        parent_source_id: Option<Vec<u8>>,
        last_updated: u64,
    ) -> Result<u16, CommitlogError> {
        self.append_record(StoredRecord::delete(
            source_id,
            parent_source_id,
            last_updated,
        ))
    }

    /// Get the latest visible record for a source id.
    pub fn get(&self, source_id: &[u8]) -> Option<StoredRecord> {
        match self.latest_by_source.get(source_id) {
            Some(record) if record.op == RecordOp::Upsert => Some(record.clone()),
            _ => None,
        }
    }

    /// Get all latest visible child records for a parent id.
    pub fn get_children(&self, parent_source_id: &[u8]) -> Vec<StoredRecord> {
        let mut children = Vec::new();
        if let Some(source_ids) = self.children_by_parent.get(parent_source_id) {
            for source_id in source_ids {
                if let Some(record) = self.get(source_id) {
                    children.push(record);
                }
            }
        }
        children
    }

    /// Return the visible current state of the store.
    pub fn scan_current(&self) -> Vec<StoredRecord> {
        let mut records: Vec<StoredRecord> = self
            .latest_by_source
            .values()
            .filter(|record| record.op == RecordOp::Upsert)
            .cloned()
            .collect();
        records.sort_by(|left, right| left.source_id.cmp(&right.source_id));
        records
    }

    /// Look through the directory of the commitlog and load the segments into memory.
    /// Also performs some cleanup on non-matching logs and indexes(for example, if there is a log file with a non-matching
    /// index or vice versa)
    fn load_segments(&mut self) -> Result<(), CommitlogError> {
        //let mut files_to_clean: HashMap<String, String> = HashMap::new();
        //let paths = fs::read_dir(&self.directory).expect("Unable to read files in directory.");
        let mut valid_segment_files: Vec<String> = Vec::new();
        let mut files_to_clean: Vec<String> = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.directory) {
            for entry in entries.flatten() {
                //if let Ok(entry) = entry {
                let path = entry.path();
                if let Some(extension) = path.extension() {
                    if extension == utils::LOG_EXTENSION {
                        let mut corresponding_index_path = entry.path();
                        corresponding_index_path.set_extension(utils::INDEX_EXTENSION);
                        if !corresponding_index_path.is_file() {
                            files_to_clean.push(path.to_str().unwrap().into());
                        }
                    } else if extension == utils::INDEX_EXTENSION {
                        let mut corresponding_log_path = entry.path();
                        corresponding_log_path.set_extension(utils::LOG_EXTENSION);
                        if corresponding_log_path.is_file() {
                            if let Some(file_stem) = path.file_stem() {
                                valid_segment_files.push(file_stem.to_str().unwrap().into());
                            }
                        } else {
                            files_to_clean.push(path.to_str().unwrap().into());
                        }
                    }
                }
                //}
            }
        }
        for segment_file in valid_segment_files {
            let loaded_segment = Segment::load_segment(&self.directory, segment_file).map_err(|e| {
                error!("{}", e);
                CommitlogError::new("unable to load segment")
            })?;
            self.segments.push(loaded_segment);
        }

        if !self.segments.is_empty() {
            self.segments
                .sort_by(|a, b| a.starting_offset.cmp(&b.starting_offset));
            let current_segment_from_loaded_segments = self
                .segments
                .pop()
                .expect("Unable to set current segment from loaded segments");
            self.current_segment = VirtualSegment::load_segment(
                &self.directory,
                current_segment_from_loaded_segments.starting_offset,
                self.max_segment_size,
            )
            .expect("unable to load virtual segment");
        }

        for file_to_clean in files_to_clean {
            fs::remove_file(file_to_clean).map_err(|e| {
                error!("{}", e);
                CommitlogError::new("unable to remove files during cleanup")
            })?;
        }
        Ok(())
    }

    // /**
    //  * Update the most current segment with information.
    //  */
    // fn reload_current_segment(&mut self) {
    //     //What do we want to do when reload is called??
    //     if self.segments.is_empty() {
    //         return;
    //     }
    //     let index = self.current_segment_index.get_mut();
    //     // Properly error handle this
    //     let current_segment = self
    //         .segments
    //         .get_mut(*index)
    //         .expect("Unable to get current segment");
    //     current_segment.reload().expect("Unable to reload segment");
    // }

    /// Get the max segment size that is assigned to the commitlog.
    pub fn get_max_segment_size(&self) -> u64 {
        self.max_segment_size
    }

    /// Load segments in from the commitlog directory that have not been loaded into memory yet.
    pub fn reload_segments(&mut self) {
        //self.reload_current_segment();
        let mut segment_map: HashMap<String, String> = HashMap::new();
        let mut valid_segments_found: Vec<String> = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.directory) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(extension) = path.extension() {
                    let file_stem = path
                        .file_stem()
                        .expect("Unable to get file stem")
                        .to_str()
                        .unwrap();
                    //let base_file_name = path.to_str().unwrap();
                    if extension == utils::LOG_EXTENSION {
                        if segment_map.contains_key(file_stem) {
                            valid_segments_found.push(file_stem.into())
                        } else {
                            segment_map.insert(file_stem.into(), utils::LOG_EXTENSION.to_string());
                        }
                    } else if extension == utils::INDEX_EXTENSION {
                        if segment_map.contains_key(file_stem) {
                            valid_segments_found.push(file_stem.into())
                        } else {
                            segment_map.insert(file_stem.into(), utils::INDEX_EXTENSION.to_string());
                        }
                    } else {
                        warn!("extension not found {:?}", extension);
                    }
                }
            }
        }

        let segments_to_add: Vec<String> = valid_segments_found
            .into_iter()
            .filter(|segment| {
                let segment_offset = segment
                    .clone()
                    .parse::<u16>()
                    .expect("Unable to parse segment base into int.");
                let mut segment_exists = false;
                for existing_segment in &self.segments {
                    if segment_offset == existing_segment.starting_offset {
                        segment_exists = true;
                        break;
                    }
                }
                if segment_offset == self.current_segment.starting_offset {
                    segment_exists = true;
                }
                !segment_exists
            })
            .collect();

        if segments_to_add.is_empty() {
            return;
        }

        for segment in segments_to_add {
            info!("updating a new segment {}", segment);
            let loaded_segment = Segment::load_segment(&self.directory, segment)
                .expect("unable to laod segment");
            self.segments.push(loaded_segment);
        }
        self.segments
            .sort_by(|a, b| a.starting_offset.cmp(&b.starting_offset));
        if let Err(err) = self.rebuild_state_indexes() {
            error!("{}", err);
        }
    }

    /// Create a new segment and set the latest segment value to that new segment. 
    /// Also flushes the sement to the disk, before creating and updating the segment.
    fn split(&mut self) -> Result<(), CommitlogError> {
        info!("Spliting commitlog segment");
        //Flush the current virtual segment to disk
        self.current_segment
            .flush()
            .map_err(|e| {
                error!("{}", e);
                CommitlogError::new("Unable to flush commitlog to disk")
            })?;
        // Get the next offset from current segment and create a new segment with it
        let next_offset = self.current_segment.starting_offset + self.current_segment.next_offset;
        self.current_segment =
            VirtualSegment::new(&self.directory, self.max_segment_size, next_offset);
        // Since our last current segment got flushed to disk, reload from disk to update the segments
        self.reload_segments();
        Ok(())
    }

    /// Given an offset, find and read the value from the commitlog for the segment that it is located in.
    pub fn read(&mut self, offset: usize) -> Result<Vec<u8>, CommitlogError> {
        //First check the current segment
        if usize::from(self.current_segment.starting_offset) <= offset {
            let search_offset = offset - usize::from(self.current_segment.starting_offset);
            return match self.current_segment.read_at(search_offset) {
                Ok(buffer) => Ok(buffer),
                Err(err) => {
                    let out_of_bounds = SegmentError::new("offset is out of bounds");
                    if err == out_of_bounds {
                        return Err(CommitlogError::new("Offset does not exist in the commitlog"));
                    } else {
                        return Err(CommitlogError::new("Unexpected error when reading commitlog"))
                    }
                }
            };
        }

        // Check the segments on disk
        let mut segment_index: Option<usize> = None;
        for (i, segment) in self.segments.iter().enumerate() {
            if usize::from(segment.starting_offset) <= offset {
                segment_index = Some(i);
            } else {
                break;
            }
        }

        if let Some(value) = segment_index {
            let segment = self
                .segments
                .get_mut(value)
                .expect("Unable to get current segment");
            let search_offset = offset - usize::from(segment.starting_offset);

            match segment.read_at(search_offset) {
                Ok(buffer) => Ok(buffer),
                Err(err) => {
                    let out_of_bounds = SegmentError::new("offset is out of bounds");
                    if err == out_of_bounds {
                        return Err(CommitlogError::new("Offset does not exist in the commitlog"));
                    } else {
                        return Err(CommitlogError::new("Unexpected error when reading commitlog"))
                    }
                }
            }
        } else {
            error!("offset {} does not exist in the commtlog", offset);
            Err(CommitlogError::new("Offset does not exist in the commitlog"))
        }
    }

    /// Given an offset, decode the stored record at that location.
    pub fn read_record(&mut self, offset: usize) -> Result<StoredRecord, CommitlogError> {
        let buffer = self.read(offset)?;
        StoredRecord::from_bytes(&buffer).map_err(|e| {
            error!("{}", e);
            CommitlogError::new("Unable to deserialize stored record from commitlog")
        })
    }

    /// Compact rewrites the persisted store from the current visible state when
    /// enough stale versions have accumulated.
    fn compact(&mut self) -> Result<(), CommitlogError>{
        info!("attempting to compact commitlog");
        let live_records = self.scan_current();
        if !self
            .compactor
            .should_compact(&self.segments, &self.current_segment, live_records.len())
        {
            self.rebuild_state_indexes()?;
            return Ok(());
        }

        if !self.store_contains_only_typed_records()? {
            info!("Skipping compaction because commitlog contains opaque bytes");
            self.rebuild_state_indexes()?;
            return Ok(());
        }

        let compacted = self.compactor.compact(
            &self.directory,
            self.max_segment_size,
            &self.segments,
            &self.current_segment,
            &live_records,
        ).map_err(|error| {
            error!("{}", error);
            CommitlogError::new("Unable to compact the commitlog")
        })?;
        if compacted {
            self.reload_from_disk()?;
        } else {
            self.rebuild_state_indexes()?;
        }
        Ok(())
    }

    fn store_contains_only_typed_records(&mut self) -> Result<bool, CommitlogError> {
        for segment in &mut self.segments {
            if !Self::segment_contains_only_typed_records(segment)? {
                return Ok(false);
            }
        }

        Self::virtual_segment_contains_only_typed_records(&mut self.current_segment)
    }

    /// Returns the first offset of the oldest segment stored in the commitlog.
    pub fn get_oldest_offset(&self) -> usize {
        if self.segments.is_empty() {
            return usize::from(self.current_segment.starting_offset);
        }
        let oldest_segment = &self.segments[0];
        let offset = oldest_segment.starting_offset;
        usize::from(offset)
    }

    /// Returns the first offset of the first segment.
    pub fn get_latest_offset(&self) -> usize {
        usize::from(self.current_segment.starting_offset + self.current_segment.next_offset)
    }

    fn rebuild_state_indexes(&mut self) -> Result<(), CommitlogError> {
        let mut latest_by_source = HashMap::new();
        let mut children_by_parent = HashMap::new();

        for segment in &mut self.segments {
            Self::replay_segment_into_state(segment, &mut latest_by_source, &mut children_by_parent)?;
        }
        Self::replay_virtual_segment_into_state(
            &mut self.current_segment,
            &mut latest_by_source,
            &mut children_by_parent,
        )?;

        self.latest_by_source = latest_by_source;
        self.children_by_parent = children_by_parent;
        Ok(())
    }

    fn replay_segment_into_state(
        segment: &mut Segment,
        latest_by_source: &mut HashMap<Vec<u8>, StoredRecord>,
        children_by_parent: &mut HashMap<Vec<u8>, BTreeSet<Vec<u8>>>,
    ) -> Result<(), CommitlogError> {
        for offset in 0..usize::from(segment.next_offset) {
            let buffer = segment.read_at(offset).map_err(|e| {
                error!("{}", e);
                CommitlogError::new("Unable to replay stored records from segment")
            })?;
            match StoredRecord::from_bytes(&buffer) {
                Ok(record) => {
                    Self::apply_record_to_state_maps(latest_by_source, children_by_parent, record);
                }
                Err(_err) => {
                    warn!(
                        "Skipping non-stored-record bytes while rebuilding state from sealed segment"
                    );
                }
            }
        }
        Ok(())
    }

    fn segment_contains_only_typed_records(
        segment: &mut Segment,
    ) -> Result<bool, CommitlogError> {
        for offset in 0..usize::from(segment.next_offset) {
            let buffer = segment.read_at(offset).map_err(|e| {
                error!("{}", e);
                CommitlogError::new("Unable to inspect segment before compaction")
            })?;
            if StoredRecord::from_bytes(&buffer).is_err() {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn replay_virtual_segment_into_state(
        segment: &mut VirtualSegment,
        latest_by_source: &mut HashMap<Vec<u8>, StoredRecord>,
        children_by_parent: &mut HashMap<Vec<u8>, BTreeSet<Vec<u8>>>,
    ) -> Result<(), CommitlogError> {
        for offset in 0..usize::from(segment.next_offset) {
            let buffer = segment.read_at(offset).map_err(|e| {
                error!("{}", e);
                CommitlogError::new("Unable to replay stored records from current segment")
            })?;
            match StoredRecord::from_bytes(&buffer) {
                Ok(record) => {
                    Self::apply_record_to_state_maps(latest_by_source, children_by_parent, record);
                }
                Err(_err) => {
                    warn!(
                        "Skipping non-stored-record bytes while rebuilding state from current segment"
                    );
                }
            }
        }
        Ok(())
    }

    fn virtual_segment_contains_only_typed_records(
        segment: &mut VirtualSegment,
    ) -> Result<bool, CommitlogError> {
        for offset in 0..usize::from(segment.next_offset) {
            let buffer = segment.read_at(offset).map_err(|e| {
                error!("{}", e);
                CommitlogError::new("Unable to inspect current segment before compaction")
            })?;
            if StoredRecord::from_bytes(&buffer).is_err() {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn apply_record_to_state_maps(
        latest_by_source: &mut HashMap<Vec<u8>, StoredRecord>,
        children_by_parent: &mut HashMap<Vec<u8>, BTreeSet<Vec<u8>>>,
        record: StoredRecord,
    ) {
        let source_id = record.source_id.clone();

        if let Some(existing_record) = latest_by_source.get(&source_id) {
            if let Some(parent_source_id) = &existing_record.parent_source_id {
                Self::remove_child_mapping(children_by_parent, parent_source_id, &source_id);
            }
        }

        if record.op == RecordOp::Upsert {
            if let Some(parent_source_id) = &record.parent_source_id {
                children_by_parent
                    .entry(parent_source_id.clone())
                    .or_default()
                    .insert(source_id.clone());
            }
        }

        latest_by_source.insert(source_id, record);
    }

    fn remove_child_mapping(
        children_by_parent: &mut HashMap<Vec<u8>, BTreeSet<Vec<u8>>>,
        parent_source_id: &[u8],
        source_id: &[u8],
    ) {
        let should_remove_entry = match children_by_parent.get_mut(parent_source_id) {
            Some(children) => {
                children.remove(source_id);
                children.is_empty()
            }
            None => false,
        };
        if should_remove_entry {
            children_by_parent.remove(parent_source_id);
        }
    }

    fn reload_from_disk(&mut self) -> Result<(), CommitlogError> {
        self.segments.clear();
        self.current_segment =
            VirtualSegment::new(&self.directory, self.max_segment_size, 0);
        self.load_segments()?;
        self.rebuild_state_indexes()?;
        Ok(())
    }
}

#[cfg(test)]
mod commitlog_tests {
    use crate::{commitlog::Commitlog, CommitlogError, StoredRecord};
    use std::path::Path;
    use tempdir::TempDir;

    #[test]
    fn test_new_commitlog() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_string = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let commitlog = Commitlog::new(tmp_dir_string, 100, 1000).expect("Unable to create commitlog");
        assert!(Path::new(&commitlog.directory).is_dir());
    }

    #[test]
    fn test_append() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl =
            Commitlog::new(tmp_dir_path, 100, 1000).expect("Unable to create commitlog");
        let test_data = "producer1".as_bytes();
        cl.append(test_data).expect("Unable to append message");

        let retrived_message = cl.read(0).expect("Unable to retrieve message");
        assert_eq!(test_data, &*retrived_message);
    }

    #[test]
    fn test_append_multiple() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl = Commitlog::new(tmp_dir_path, 1000, 10000).expect("Unable to create commitlog");
        let test_data = "message".as_bytes();
        for n in 0..10 {
            let offset = cl.append(test_data).expect("Unable to append message");
            assert_eq!(n, offset);
        }
    }

    #[test]
    fn test_split() {
        let number_of_iterations = 20;
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl = Commitlog::new(tmp_dir_path, 100, 1000).expect("Unable to create commitlog");

        for i in 0..number_of_iterations {
            let string_message = format!("myTestMessage{}", i);
            let test_data = string_message.as_bytes();
            let offset = cl.append(test_data).expect("Unable to append message");
            println!("{}", offset);
            assert_eq!(offset, i);
        }

        for i in 0..number_of_iterations {
            let string_message = format!("myTestMessage{}", i);
            let test_data = string_message.as_bytes();
            let retrived_message = cl.read(i.into()).expect("Unable to retrieve message");
            assert_eq!(test_data, &*retrived_message);
        }
    }

    #[test]
    fn get_oldest_offset_test() {
        let number_of_iterations = 5;
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl = Commitlog::new(tmp_dir_path, 1000, 10000).expect("Unable to create commitlog");

        for i in 0..number_of_iterations {
            let string_message = format!("myTestMessage{}", i);
            let test_data = string_message.as_bytes();
            cl.append(test_data).expect("Unable to append message");
        }

        let latest_cl_offset = cl.get_oldest_offset();
        assert_eq!(0, latest_cl_offset);
    }

    #[test]
    fn get_latest_offset_test() {
        let number_of_iterations = 5;
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl = Commitlog::new(tmp_dir_path, 1000, 10000).expect("Unable to create commitlog");

        for i in 0..number_of_iterations {
            let string_message = format!("myTestMessage{}", i);
            let test_data = string_message.as_bytes();
            cl.append(test_data).expect("Unable to append message");
        }

        let latest_cl_offset = cl.get_latest_offset();
        assert_eq!(number_of_iterations, latest_cl_offset);
    }

    #[test]
    fn test_append_message_bigger_than_segment() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl =
            Commitlog::new(tmp_dir_path, 10, 100).expect("Unable to create commitlog");
        let bytes: [u8; 11] = [0; 11];
        let commitlog_error = cl.append(&bytes).unwrap_err();
        let wanted_error =
            CommitlogError::new("Unknown error when writing occured");
        assert_eq!(wanted_error, commitlog_error);
    }

    #[test]
    fn test_upsert_get_and_scan_current() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl = Commitlog::new(tmp_dir_path, 512, 10000).expect("Unable to create commitlog");

        cl.upsert(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            br#"{"hello":"world"}"#.to_vec(),
            1234,
        )
        .expect("Unable to upsert record");

        let expected_record = StoredRecord::upsert(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            br#"{"hello":"world"}"#.to_vec(),
            1234,
        );

        let loaded_record = cl.get(b"source-1").expect("Unable to get stored record");
        assert_eq!(expected_record, loaded_record);
        assert_eq!(vec![expected_record.clone()], cl.get_children(b"parent-1"));
        assert_eq!(vec![expected_record], cl.scan_current());
    }

    #[test]
    fn test_delete_removes_record_from_visible_state() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl = Commitlog::new(tmp_dir_path, 512, 10000).expect("Unable to create commitlog");

        cl.upsert(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            br#"{"hello":"world"}"#.to_vec(),
            1234,
        )
        .expect("Unable to upsert record");
        cl.delete(b"source-1".to_vec(), None, 5678)
            .expect("Unable to delete record");

        assert_eq!(None, cl.get(b"source-1"));
        assert!(cl.get_children(b"parent-1").is_empty());
        assert!(cl.scan_current().is_empty());
    }

    #[test]
    fn test_typed_state_rebuilds_on_restart() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");

        {
            let mut cl = Commitlog::new(tmp_dir_path, 256, 10000).expect("Unable to create commitlog");
            cl.upsert(
                b"source-1".to_vec(),
                Some(b"parent-a".to_vec()),
                br#"{"version":1}"#.to_vec(),
                1000,
            )
            .expect("Unable to upsert record");
            cl.upsert(
                b"source-2".to_vec(),
                Some(b"parent-a".to_vec()),
                br#"{"version":1}"#.to_vec(),
                2000,
            )
            .expect("Unable to upsert record");
            cl.upsert(
                b"source-1".to_vec(),
                Some(b"parent-b".to_vec()),
                br#"{"version":2}"#.to_vec(),
                3000,
            )
            .expect("Unable to upsert record");
        }

        let reopened = Commitlog::new(tmp_dir_path, 256, 10000).expect("Unable to reload commitlog");

        let expected_latest = StoredRecord::upsert(
            b"source-1".to_vec(),
            Some(b"parent-b".to_vec()),
            br#"{"version":2}"#.to_vec(),
            3000,
        );

        assert_eq!(Some(expected_latest.clone()), reopened.get(b"source-1"));
        assert_eq!(1, reopened.get_children(b"parent-a").len());
        assert_eq!(b"source-2".to_vec(), reopened.get_children(b"parent-a")[0].source_id);
        assert_eq!(vec![expected_latest], reopened.get_children(b"parent-b"));
        assert_eq!(2, reopened.scan_current().len());
        assert_eq!(3, reopened.get_latest_offset());
    }

    #[test]
    fn test_compaction_keeps_latest_visible_records() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");

        {
            let mut cl = Commitlog::new(tmp_dir_path, 1024, 1).expect("Unable to create commitlog");
            cl.upsert(
                b"source-1".to_vec(),
                Some(b"parent-a".to_vec()),
                br#"{"version":1}"#.to_vec(),
                1000,
            )
            .expect("Unable to upsert record");
            cl.upsert(
                b"source-2".to_vec(),
                Some(b"parent-a".to_vec()),
                br#"{"version":1}"#.to_vec(),
                1500,
            )
            .expect("Unable to upsert record");
            cl.upsert(
                b"source-1".to_vec(),
                Some(b"parent-b".to_vec()),
                br#"{"version":2}"#.to_vec(),
                2000,
            )
            .expect("Unable to upsert record");

            assert_eq!(2, cl.scan_current().len());
            assert_eq!(1, cl.get_children(b"parent-a").len());
            assert_eq!(b"source-2".to_vec(), cl.get_children(b"parent-a")[0].source_id);
            assert_eq!(1, cl.get_children(b"parent-b").len());
            assert_eq!(b"source-1".to_vec(), cl.get_children(b"parent-b")[0].source_id);
            assert_eq!(2, cl.get_latest_offset());
        }

        let reopened = Commitlog::new(tmp_dir_path, 1024, 1).expect("Unable to reload commitlog");
        assert_eq!(2, reopened.scan_current().len());
        assert_eq!(Some(b"parent-b".to_vec()), reopened.get(b"source-1").unwrap().parent_source_id);
        assert_eq!(2, reopened.get_latest_offset());
    }

    #[test]
    fn test_compaction_removes_deleted_records_from_disk() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");

        {
            let mut cl = Commitlog::new(tmp_dir_path, 1024, 1).expect("Unable to create commitlog");
            cl.upsert(
                b"source-1".to_vec(),
                Some(b"parent-a".to_vec()),
                br#"{"version":1}"#.to_vec(),
                1000,
            )
            .expect("Unable to upsert record");
            cl.delete(b"source-1".to_vec(), None, 2000)
                .expect("Unable to delete record");

            assert_eq!(None, cl.get(b"source-1"));
            assert!(cl.scan_current().is_empty());
            assert_eq!(0, cl.get_latest_offset());
        }

        let reopened = Commitlog::new(tmp_dir_path, 1024, 1).expect("Unable to reload commitlog");
        assert_eq!(None, reopened.get(b"source-1"));
        assert!(reopened.scan_current().is_empty());
        assert_eq!(0, reopened.get_latest_offset());
    }

    #[test]
    fn test_compaction_skips_opaque_bytes() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl = Commitlog::new(tmp_dir_path, 1024, 1).expect("Unable to create commitlog");

        cl.append(b"hello").expect("Unable to append raw bytes");
        cl.append(b"world").expect("Unable to append raw bytes");

        assert_eq!(b"hello", &*cl.read(0).expect("Unable to read first raw entry"));
        assert_eq!(b"world", &*cl.read(1).expect("Unable to read second raw entry"));
        assert_eq!(2, cl.get_latest_offset());
    }

    #[test]
    fn test_read_record_returns_typed_record() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let tmp_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to conver path to string");
        let mut cl = Commitlog::new(tmp_dir_path, 512, 10000).expect("Unable to create commitlog");

        let record = StoredRecord::upsert(
            b"source-1".to_vec(),
            Some(b"parent-1".to_vec()),
            br#"{"hello":"world"}"#.to_vec(),
            1234,
        );

        cl.append_record(record.clone())
            .expect("Unable to append stored record");

        assert_eq!(record, cl.read_record(0).expect("Unable to read stored record"));
    }
}
