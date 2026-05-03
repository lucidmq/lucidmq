use std::fs;
use std::path::{Path, PathBuf};

use log::{error, info};

use crate::nolan_errors::{CompactorError, SegmentError};
use crate::record::StoredRecord;
use crate::segment::Segment;
use crate::utils;
use crate::virtual_segment::VirtualSegment;

#[derive(Default)]
pub struct Compactor {
    compaction_threshold_bytes: u64,
}

impl Compactor {
    /// Initialize a new compactor, based on the total stored bytes threshold that
    /// should trigger a rewrite.
    pub fn new(compaction_threshold_bytes: u64) -> Compactor {
        Compactor {
            compaction_threshold_bytes,
        }
    }

    /// Rewrite the whole persisted store from the latest visible records.
    ///
    /// This drops obsolete versions and tombstones, and rewrites offsets
    /// contiguously from zero.
    pub fn compact(
        &self,
        directory: &str,
        max_segment_size: u64,
        segments: &[Segment],
        current_segment: &VirtualSegment,
        live_records: &[StoredRecord],
    ) -> Result<bool, CompactorError> {
        if !self.should_compact(segments, current_segment, live_records.len()) {
            return Ok(false);
        }

        info!("Compacting commitlog segments into current visible state");
        if current_segment.entry_count() > 0 {
            current_segment.flush().map_err(|e| {
                error!("{}", e);
                CompactorError::new("Unable to flush current segment before compaction")
            })?;
        }

        let temp_dir = Path::new(directory).join(".nolan-compaction");
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).map_err(|e| {
                error!("{}", e);
                CompactorError::new("Unable to clear previous compaction directory")
            })?;
        }
        fs::create_dir_all(&temp_dir).map_err(|e| {
            error!("{}", e);
            CompactorError::new("Unable to create compaction directory")
        })?;

        let temp_dir_string = temp_dir.to_str().ok_or_else(|| {
            CompactorError::new("Unable to convert compaction directory to string")
        })?;
        self.write_compacted_segments(temp_dir_string, max_segment_size, live_records)?;
        self.delete_existing_segment_files(directory)?;
        self.move_compacted_files_into_place(&temp_dir, directory)?;
        fs::remove_dir_all(&temp_dir).map_err(|e| {
            error!("{}", e);
            CompactorError::new("Unable to remove compaction directory")
        })?;
        Ok(true)
    }

    pub(crate) fn should_compact(
        &self,
        segments: &[Segment],
        current_segment: &VirtualSegment,
        live_record_count: usize,
    ) -> bool {
        let total_bytes: u64 = segments.iter().map(|segment| segment.position as u64).sum::<u64>()
            + current_segment.size_bytes();
        if total_bytes <= self.compaction_threshold_bytes {
            return false;
        }

        let total_entries: usize = segments
            .iter()
            .map(|segment| usize::from(segment.next_offset))
            .sum::<usize>()
            + usize::from(current_segment.entry_count());
        total_entries > live_record_count
    }

    fn write_compacted_segments(
        &self,
        directory: &str,
        max_segment_size: u64,
        live_records: &[StoredRecord],
    ) -> Result<(), CompactorError> {
        if live_records.is_empty() {
            return Ok(());
        }

        let split_err =
            SegmentError::new("Write not possible. Segment log would be greater than max bytes");
        let mut next_offset: u16 = 0;
        let mut segment = VirtualSegment::new(directory, max_segment_size, next_offset);

        for record in live_records {
            let bytes = record.to_bytes().map_err(|e| {
                error!("{}", e);
                CompactorError::new("Unable to serialize record during compaction")
            })?;
            loop {
                match segment.write(&bytes) {
                    Ok(_) => break,
                    Err(err) if err == split_err => {
                        segment.flush().map_err(|e| {
                            error!("{}", e);
                            CompactorError::new("Unable to flush compacted segment")
                        })?;
                        next_offset = segment.starting_offset + segment.next_offset;
                        segment = VirtualSegment::new(directory, max_segment_size, next_offset);
                    }
                    Err(err) => {
                        error!("{}", err);
                        return Err(CompactorError::new(
                            "Unable to write record to compacted segment",
                        ));
                    }
                }
            }
        }

        if segment.entry_count() > 0 {
            segment.flush().map_err(|e| {
                error!("{}", e);
                CompactorError::new("Unable to flush final compacted segment")
            })?;
        }
        Ok(())
    }

    fn delete_existing_segment_files(&self, directory: &str) -> Result<(), CompactorError> {
        if let Ok(entries) = fs::read_dir(directory) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(extension) = path.extension() {
                    if extension == utils::LOG_EXTENSION || extension == utils::INDEX_EXTENSION {
                        fs::remove_file(&path).map_err(|e| {
                            error!("{}", e);
                            CompactorError::new("Unable to delete stale segment file")
                        })?;
                    }
                }
            }
        }
        Ok(())
    }

    fn move_compacted_files_into_place(
        &self,
        temp_dir: &Path,
        directory: &str,
    ) -> Result<(), CompactorError> {
        let mut files_to_move: Vec<PathBuf> = Vec::new();
        if let Ok(entries) = fs::read_dir(temp_dir) {
            for entry in entries.flatten() {
                files_to_move.push(entry.path());
            }
        }
        files_to_move.sort();

        for file_path in files_to_move {
            let file_name = file_path.file_name().ok_or_else(|| {
                CompactorError::new("Unable to get compacted file name")
            })?;
            let destination = Path::new(directory).join(file_name);
            fs::rename(&file_path, destination).map_err(|e| {
                error!("{}", e);
                CompactorError::new("Unable to install compacted segment file")
            })?;
        }
        Ok(())
    }
}
