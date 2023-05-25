use std::str;
use crate::nolan_errors::IndexError;
use log::error;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::ErrorKind;
use std::io::SeekFrom;

/**
 * Basic data structure that holds our data in our index
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct Entry {
    pub start: u32,
    pub total: u32,
}

pub struct Index {
    pub file_name: String,
    entries: Vec<Entry>,
    index_file: File,
}

impl Index {
    /**
     * Create a new index
     */
    pub fn new(index_path: String) -> Result<Index, IndexError> {
        let error_message = format!("Unable to create and open file {}", index_path);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(index_path.clone())
            .map_err(|e| {
                error!("{}", e);
                IndexError::new(&error_message)
            })
            .expect(&error_message); // TODO: Error handle this
        let empty_entry_vec = Vec::new();
        Ok(Index {
            file_name: index_path,
            entries: empty_entry_vec,
            index_file: file,
        })
    }

    // /**
    //  * Add a new entry to the index
    //  */
    // pub fn add_entry(&mut self, start_position: u32, total_bytes: u32) -> Result<bool, IndexError> {
    //     let entry = Entry {
    //         start: start_position,
    //         total: total_bytes,
    //     };
    //     let encoded_entry: Vec<u8> = bincode::serialize(&entry).map_err(|e| {
    //         error!("{}", e);
    //         IndexError::new("Unable to serialize entry")
    //     })?;

    //     self.entries.push(entry);

    //     let entry_bytes: &[u8] = &encoded_entry[..];
    //     self.index_file.write(entry_bytes).map_err(|e| {
    //         error!("{}", e);
    //         IndexError::new("Unable to write entry to index file")
    //     })?;
    //     Ok(true)
    // }

    /**
     * Load the index into memory
     */
    pub fn load_index(&mut self) -> Result<u16, IndexError> {
        self.index_file.seek(SeekFrom::Start(0)).map_err(|e| {
            error!("{}", e);
            IndexError::new("unable seek to begining of the index")
        })?;
        let mut circut_break: bool = false;
        loop {
            let mut buffer = [0; 8];
            //TODO: error handle this correctly
            self.index_file
                .read_exact(&mut buffer)
                .unwrap_or_else(|error| {
                    if error.kind() == ErrorKind::UnexpectedEof {
                        circut_break = true;
                    } else {
                        // error!{"{}", error}
                        // return IndexError::new("unable seek to begining of the index");
                        panic!("{}", error)
                    }
                });
            if circut_break {
                break;
            }
            let decoded_entry: Entry = bincode::deserialize(&buffer).map_err(|e| {
                error!("{}", e);
                IndexError::new("unable to deserialize entry")
            })?;
            self.entries.push(decoded_entry);
        }
        let value = u16::try_from(self.entries.len()).map_err(|e| {
            error!("{}", e);
            IndexError::new("unable to convert usize to u16")
        })?;
        Ok(value)
    }

    // /**
    //  * Reload any new entries in the index to the entries section of the datastructure
    //  */
    // pub fn reload_index(&mut self) -> Result<u16, IndexError> {
    //     let entry_byte_size: u64 = 8;
    //     let total_current_entires: u64 = self.entries.len() as u64;
    //     let entry_read_start_bytes: u64 = entry_byte_size * total_current_entires;
    //     //Start from the last known entry
    //     self.index_file
    //         .seek(SeekFrom::Start(entry_read_start_bytes))
    //         .map_err(|e| {
    //             error!("{}", e);
    //             IndexError::new("unable to seek to last entry offset")
    //         })?;
    //     // Iterate through the file bytes and conver them to entries
    //     let mut circut_break: bool = false;
    //     loop {
    //         let mut buffer = [0; 8];
    //         //TODO: error handle this correctly
    //         self.index_file
    //             .read_exact(&mut buffer)
    //             .unwrap_or_else(|error| {
    //                 if error.kind() == ErrorKind::UnexpectedEof {
    //                     circut_break = true;
    //                 } else {
    //                     panic!("Unable to read from index file. {:?}", error);
    //                 }
    //             });
    //         if circut_break {
    //             break;
    //         }
    //         let decoded_entry: Entry = bincode::deserialize(&buffer).map_err(|e| {
    //             error!("{}", e);
    //             IndexError::new("unable to deserialize entry")
    //         })?;
    //         self.entries.push(decoded_entry);
    //     }
    //     let value = u16::try_from(self.entries.len()).map_err(|e| {
    //         error!("{}", e);
    //         IndexError::new("unable to convert usize to u16")
    //     })?;
    //     Ok(value)
    // }

    /**
     * Given an offset, return the entry start
     */
    pub fn return_entry_details_by_offset(
        &self,
        offset: usize,
    ) -> Result<(u64, usize), IndexError> {
        // This can throw an exception if the offset is greater than the size of the array, how do we check?
        if offset > self.entries.len() {
            return Err(IndexError::new("offset is greater than entries length"));
        }
        let entry = self.entries[offset];
        let start_offset: u64 = entry.start.into();
        //TODO: error handle this correctly
        let total_bytes: usize = usize::try_from(entry.total).map_err(|e| {
            error!("{}", e);
            IndexError::new("unable to convert from u32 to usize")
        })?;
        Ok((start_offset, total_bytes))
    }
}

#[cfg(test)]
mod index_tests {
    use std::str;
    use std::path::Path;
    use tempdir::TempDir;
    use rand::{distributions::Alphanumeric, Rng}; // 0.8
    use crate::index::Index;
    use crate::utils;
    use crate::virtual_segment::VirtualSegment;

    fn create_index_file(test_dir_path: &str, message_to_write: &[u8]) -> String{
        let mut vs = VirtualSegment::new(test_dir_path, 100, 0);
        vs
            .write(message_to_write)
            .expect("unable to write data to virtual segment");
        vs.flush();
        let file_name = vs.full_log_path.clone();
        let thing = str::strip_suffix(&file_name, utils::LOG_SUFFIX).expect("unable to strip");
        let index_file_name = format!("{}{}", thing, utils::INDEX_SUFFIX);
        return index_file_name
    }

    #[test]
    fn test_new_index() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let test_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to convert path to string");
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        let index_file_path = format!("{}{}{}", test_dir_path.to_string(), &s, utils::INDEX_SUFFIX);
        let index = Index::new(index_file_path).expect("Error creating index");  
        //Check if the index file exists
        assert!(Path::new(&index.file_name).exists());
    }

    #[test]
    fn test_load_index() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let test_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to convert path to string");
        let index_file_name = create_index_file(test_dir_path, "hello".as_bytes());

        let mut index = Index::new(index_file_name).expect("Error creating index");
        index.load_index().expect("unable to load index");
        assert!(index.entries.len() == 1);
    }

    #[test]
    fn return_entry_details_by_offset() {
        let tmp_dir = TempDir::new("test").expect("Unable to create temp directory");
        let test_dir_path = tmp_dir
            .path()
            .to_str()
            .expect("Unable to convert path to string");
        let message = "hello";
        let index_file_name = create_index_file(test_dir_path, message.as_bytes());

        let mut index = Index::new(index_file_name).expect("Error creating index");
        index.load_index().expect("unable to load index");

        let (start, total) = index.return_entry_details_by_offset(0).expect("Unable to get entry details");
        assert!(start == 0);
        assert!(total == message.len());
    }

}