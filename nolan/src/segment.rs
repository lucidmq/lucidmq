use log::error;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use crate::utils;
use crate::index::Index;
use crate::nolan_errors::SegmentError;

/// Segment is a data type that holds all of the byte data within the commitlog in nolan.
/// It is made up of 2 main pieces the log and the index. The log is what actually
/// user supplied data is stored. The index is used to quickly retrieve information
/// that is persisted.
pub struct Segment {
    /// The file path to the log file
    pub file_name: String,
    /// Current position of the cursor within the log file
    pub position: u32,
    /// The Starting offset within the segment
    pub starting_offset: u16,
    /// Next offset within the segment
    pub next_offset: u16,
    /// Parent directory/Commitlog directory
    pub directory: String,
    /// File ref to the segment file
    log_file: File,
    /// Index ref to the index file
    index: Index,
}


impl Segment {
    /// Given a directory and the base name of the log and index file, load a new
    /// segment into memory.
    pub fn load_segment(
        base_directory: &str,
        segment_base: String,
        //max_segment_bytes: u64
    ) -> Result<Segment, SegmentError> {
        let segment_offset = segment_base.parse::<u16>().map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to parse base string into u16")
        })?;
        let log_file_name = utils::create_segment_file_name(
            base_directory,
            segment_offset,
            utils::LOG_SUFFIX,
        ).map_err(|e| {
            error!("{}", e);
            SegmentError::new("Unable to create log file")
        })?;

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(log_file_name.clone())
            .map_err(|e| {
                error!("{}", e);
                SegmentError::new("unable to open log file")
            })?;

        let metadata = file.metadata().map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable get metadata for log file")
        })?;
        // This would be unnesseary if we used u64 for the position
        let current_segment_postion: u32 = u32::try_from(metadata.len()).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to convert from u64 to u32")
        })?;

        let index_file_name = utils::create_segment_file_name(
            base_directory,
            segment_offset,
            utils::INDEX_SUFFIX,
        ).map_err(|e| {
            error!("{}", e);
            SegmentError::new("Unable to create index file")
        })?;
        let mut loaded_index = Index::new(&index_file_name).map_err(|e| {
            SegmentError::new(&e.to_string())
        })?;

        let total_entries = loaded_index.load_index().map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to load index")
        })?;

        let segment = Segment {
            file_name: log_file_name,
            position: current_segment_postion,
            //max_bytes: max_segment_bytes,
            starting_offset: segment_offset,
            next_offset: total_entries,
            directory: base_directory.to_string(),
            log_file: file,
            index: loaded_index,
        };

        Ok(segment)
    }

    /// Given an offset, find the entry in the index and get the bytes fromt he log
    pub fn read_at(&mut self, offset: usize) -> Result<Vec<u8>, SegmentError> {
        if offset >= usize::from(self.next_offset) {
            return Err(SegmentError::new("offset is out of bounds"));
        }
        let (start, total) = self
            .index
            .return_entry_details_by_offset(offset)
            .map_err(|e| {
                error!("{}", e);
                SegmentError::new("unable to get entry details from index")
            })?;
        // Let's create our buffer
        let mut buffer = vec![0; total];
        // Seek to entries start position
        self.log_file.seek(SeekFrom::Start(start)).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to seek to offset in the log file")
        })?;
        // Read log file bytes into the buffer
        self.log_file.read_exact(&mut buffer).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to read into buffer")
        })?;
        Ok(buffer)
    }

    /// Close the log file and the index file, then delete both of these files.
    pub fn delete(&self) -> Result<bool, SegmentError> {
        //self.close();
        fs::remove_file(&self.file_name).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to delete log file")
        })?;
        fs::remove_file(&self.index.file_name).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to delete index file")
        })?;
        Ok(true)
    }


}

#[cfg(test)]
mod segment_tests {
    use std::path::Path;
    use tempdir::TempDir;
    use crate::nolan_errors::SegmentError;
    use crate::virtual_segment::VirtualSegment;
    use crate::utils;
    use crate::segment::Segment;

    fn create_segment_file(test_dir_path: &str, message_to_write: &[u8]) -> String{
        let mut vs = VirtualSegment::new(test_dir_path, 100, 0);
        vs
            .write(message_to_write)
            .expect("unable to write data to virtual segment");
        vs.flush().expect("Unable to flush");
        let file_name = Path::new(&vs.full_log_path).file_name().unwrap().to_str().expect("Unbale to conver os string to string");
        let segment_base = str::strip_suffix(&file_name, utils::LOG_SUFFIX).expect("unable to strip");
        return segment_base.to_string();
    }

    #[test]
    fn test_load_segment() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let test_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to convert path to string");
        let segment_base = create_segment_file(test_dir_path, "hello".as_bytes());
        let segment = Segment::load_segment(test_dir_path, segment_base).expect("unable to load segment");
        //Check if the directory exists
        assert!(Path::new(&segment.file_name).exists());
    }

    #[test]
    fn test_read_at() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let test_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to convert path to string");
        let message = "hello".as_bytes();
        let segment_base = create_segment_file(test_dir_path, message);
        let mut segment = Segment::load_segment(test_dir_path, segment_base).expect("unable to load segment");

        let result = segment.read_at(0).expect("Unable to read at offset");

        assert!(result.iter().eq(message.iter()));
    }

    #[test]
    fn test_read_at_offset_dne() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let test_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to convert path to string");
        let message = "hello".as_bytes();
        let segment_base = create_segment_file(test_dir_path, message);
        let mut segment = Segment::load_segment(test_dir_path, segment_base).expect("unable to load segment");

        let segment_error = segment.read_at(1).unwrap_err();
        let wanted_error =
            SegmentError::new("offset is out of bounds");
        assert_eq!(wanted_error, segment_error);
    }

    #[test]
    fn test_delete() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let test_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to convert path to string");
        let message = "hello".as_bytes();
        let segment_base = create_segment_file(test_dir_path, message);
        let segment = Segment::load_segment(test_dir_path, segment_base).expect("unable to load segment");

        let segment_path = segment.file_name.clone();
        let index_path = segment.index.file_name.clone();

        let delete_result = segment.delete().expect("Unable to delete segment");
        assert!(delete_result.eq(&true));
        assert!(!Path::new(&segment_path).exists());
        assert!(!Path::new(&index_path).exists());
    }
}
