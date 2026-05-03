use crate::utils;
use crate::{nolan_errors::SegmentError, virtual_index::VirtualIndex};
use log::{error, info};
use std::{
    fs::OpenOptions,
    io::{Cursor, Read, Seek, SeekFrom, Write},
};

/// A virtual segment is a data type that holds all of the sement data only in memory.
/// This is intended to use only for the current segment, that is to allow for fast access for data in that segment.
pub struct VirtualSegment {
    contents: Cursor<Vec<u8>>,
    /// The last position in the log file to allow for easy writes
    position: u32,
    max_bytes: u64,
    /// The starting offset of the segment relative to the whole commitlog
    pub starting_offset: u16,
    /// The next offset that was written to relative to the current segment
    pub next_offset: u16,
    index: VirtualIndex,
    pub full_log_path: String,
}

impl VirtualSegment {
    /// Create a virtual segment with the provided starting offset and max segment bytes.
    pub fn new(base_directory: &str, max_segment_bytes: u64, offset: u16) -> VirtualSegment {
        info!("Creating a new virtual segment");
        let log_file_path =
            utils::create_segment_file_name(&base_directory, offset, utils::LOG_SUFFIX)
                .expect("Unable to create log file");
        let index_file_name =
            utils::create_segment_file_name(&base_directory, offset, utils::INDEX_SUFFIX)
                .expect("Unable to create index file");
        let new_virtual_index = VirtualIndex::new(index_file_name);
        VirtualSegment {
            contents: Cursor::new(Vec::new()),
            position: 0,
            max_bytes: max_segment_bytes,
            starting_offset: offset,
            next_offset: 0,
            index: new_virtual_index,
            full_log_path: log_file_path,
        }
    }

    /// Given a directory and the base name of the log and index file, load a new segment into memory.
    pub fn load_segment(
        base_directory: &str,
        segment_offset: u16,
        max_segment_bytes: u64,
    ) -> Result<VirtualSegment, SegmentError> {
        //let segment_error_str = format!("unable to parse base string {segment_offset} into u16");
        // let segment_offset = segment_base.parse::<u16>().map_err(|e| {
        //     error!("{}", e);
        //     SegmentError::new(&segment_error_str)
        // })?;
        let log_file_name =
            utils::create_segment_file_name(base_directory, segment_offset, utils::LOG_SUFFIX)
                .map_err(|e| {
                    error!("{}", e);
                    SegmentError::new("unable to create log file")
                })?;

        let mut log_file = OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .append(false)
            .open(log_file_name.clone())
            .map_err(|e| {
                error!("{}", e);
                SegmentError::new("unable to open log file")
            })?;

        //Create our buffer
        let mut buffer = Vec::new();
        // Read contents from the file into the buffer
        let total_bytes_read = log_file.read_to_end(&mut buffer).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to read file to buffer")
        })?;

        // This would be unnesseary if we used u64 for the position
        let current_segment_postion: u32 = u32::try_from(total_bytes_read).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to convert from u64 to u32")
        })?;

        let index_file_name = utils::create_segment_file_name(
            base_directory.clone(),
            segment_offset,
            utils::INDEX_SUFFIX,
        )
        .map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to create index file")
        })?;
        let mut loaded_index = VirtualIndex::new(index_file_name);

        let total_entries = loaded_index.load_index().map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to load index")
        })?;

        let segment = VirtualSegment {
            contents: Cursor::new(buffer),
            position: current_segment_postion,
            max_bytes: max_segment_bytes,
            starting_offset: segment_offset,
            next_offset: total_entries,
            index: loaded_index,
            full_log_path: log_file_name,
        };

        Ok(segment)
    }

    /// Given a byte array, write that data to the corresponding log and index.
    /// Return the offset in the segment that was written to.
    pub fn write(&mut self, data: &[u8]) -> Result<u16, SegmentError> {
        if data.len() > self.max_bytes.try_into().unwrap() {
            return Err(SegmentError::new("Data to write is greater than the allowed max segment size"));
        }
        let computed_size_bytes = u64::try_from(self.contents.get_ref().len() + data.len())
            .map_err(|e| {
                error!("{}", e);
                SegmentError::new("unable to convert from usize to u64")
            })?;
        if computed_size_bytes > self.max_bytes {
            return Err(SegmentError::new(
                "Write not possible. Segment log would be greater than max bytes",
            ));
        }
        let u_bytes = self.contents.write(data).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to write to log file")
        })?;
        let written_bytes: u32 = u32::try_from(u_bytes).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to convert from usize to u32")
        })?;
        self.index
            .add_entry(self.position, written_bytes)
            .map_err(|e| {
                error!("{}", e);
                SegmentError::new("unable to add entry to index")
            })?;
        self.position += written_bytes;
        let offset_written = self.next_offset;
        self.next_offset += 1;
        Ok(offset_written)
    }

    pub fn size_bytes(&self) -> u64 {
        self.contents.get_ref().len() as u64
    }

    pub fn entry_count(&self) -> u16 {
        self.next_offset
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
        self.contents.seek(SeekFrom::Start(start)).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to seek to offset in the log file")
        })?;
        // Read log file bytes into the buffer
        self.contents.read_exact(&mut buffer).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to read into buffer")
        })?;
        Ok(buffer)
    }

    /// Flushes the virtual segment to disk, where it will become a regular segment. This involves 2 write
    /// operations, writing the log file and writing the index file.
    pub fn flush(&self) -> Result<(), SegmentError> {
        let mut log_file = OpenOptions::new()
            .create(true)
            .read(false)
            .write(true)
            .append(false)
            .open(self.full_log_path.clone())
            .map_err(|e| {
                error!("{}", e);
                SegmentError::new("unable to open log file for flushing")
            })?;

        self.index.flush().map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to flush index contents to file")
        })?;

        log_file.write_all(self.contents.get_ref()).map_err(|e| {
            error!("{}", e);
            SegmentError::new("unable to flush log contents to file")
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod virtual_segment_tests {
    use crate::nolan_errors::SegmentError;
    use crate::virtual_segment::VirtualSegment;
    use std::path::Path;
    use tempdir::TempDir;

    #[test]
    fn test_one_message() {
        let mut vs = VirtualSegment::new("test_dir", 100, 0);
        let data = "hellos".as_bytes();
        let offset = vs
            .write(data)
            .expect("unable to write data to virtual segment");
        assert_eq!(0, offset.into());
        let retrieve_data = vs
            .read_at(offset.into())
            .expect("Failed to get message from virtual segment");
        assert_eq!(data, &*retrieve_data);
    }

    #[test]
    fn test_multiple_message() {
        let mut vs = VirtualSegment::new("test_dir", 100, 0);
        let messages = ["hello", "world", "im", "here"];
        for (i, message) in messages.iter().enumerate() {
            let data = message.as_bytes();
            let offset = vs.write(data).expect("unable to write to virtual seg");
            assert_eq!(i, offset.into());
        }

        for (i, message) in messages.iter().enumerate() {
            let retrieve_data = vs
                .read_at(i)
                .expect("Failed to get message from virtual segment");
            assert_eq!(message.as_bytes(), &*retrieve_data);
        }
    }

    #[test]
    fn test_message_greater_than_segment() {
        let mut vs = VirtualSegment::new("test_dir", 10, 0);
        let bytes: [u8; 11] = [0; 11];
        let segment_error = vs.write(&bytes).unwrap_err();
        let want =
            SegmentError::new("Data to write is greater than the allowed max segment size");
        assert_eq!(want, segment_error);
    }

    #[test]
    fn test_multi_messages_greater_than_segment() {
        let mut vs = VirtualSegment::new("test_dir", 10, 0);
        let bytes: [u8; 6] = [0; 6];
        vs.write(&bytes).unwrap();
        let segment_error = vs.write(&bytes).unwrap_err();
        let want =
            SegmentError::new("Write not possible. Segment log would be greater than max bytes");
        assert_eq!(want, segment_error);
    }

     #[test]
    fn test_offset_greater_than_segment() {
        let mut vs = VirtualSegment::new("test_dir", 100, 0);
        let bytes: [u8; 10] = [0; 10];
        vs.write(&bytes).unwrap();

        let segment_error = vs.read_at(1).unwrap_err();
        let wanted_error =
            SegmentError::new("offset is out of bounds");
        assert_eq!(wanted_error, segment_error);
    }

    // #[test]
    // fn test_offset_less_than_segment() {
    //     let mut vs = VirtualSegment::new("test_dir", 100, 1);
    //     let bytes: [u8; 10] = [0; 10];
    //     vs.write(&bytes).unwrap();

    //     let segment_error = vs.read_at(0).unwrap_err();
    //     let wanted_error =
    //         SegmentError::new("offset is out of bounds");
    //     assert_eq!(wanted_error, segment_error);
    // }

    #[test]
    fn test_flush() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let test_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to convert path to string");
        let mut vs = VirtualSegment::new(test_dir_path, 100, 0);
        let data = "hellos".as_bytes();
        vs.write(data)
            .expect("unable to write data to virtual segment");
        vs.flush().expect("Unable to flush");

        assert!(Path::new(&vs.full_log_path).exists());
    }

    #[test]
    fn test_flush_dir_dne() {
        let mut vs = VirtualSegment::new("test", 100, 0);
        let data = "hellos".as_bytes();
        vs.write(data)
            .expect("unable to write data to virtual segment");
        let segment_error = vs.flush().unwrap_err();
        let wanted_error =
            SegmentError::new("unable to open log file for flushing");
        assert_eq!(wanted_error, segment_error);
    }
}
