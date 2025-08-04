use crate::db::log_format::{K_BLOCK_SIZE, K_MAX_RECORD_TYPE};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::sequential_file::SequentialFile;
use std::sync::{Arc, Mutex};

const K_EOF: usize = K_MAX_RECORD_TYPE + 1;
const K_BAD_RECORD: usize = K_MAX_RECORD_TYPE + 2;

trait Reporter {
    fn corruption(&mut self, bytes: usize, status: &Status);
}

struct Reader {
    file_: Arc<Mutex<dyn SequentialFile>>,
    reporter_: Option<Box<dyn Reporter>>,
    checksum_: bool,
    backing_store_: Vec<u8>,
    buffer_: Slice,
    eof_: bool,
    last_record_offset_: u64,
    end_of_buffer_offset_: u64,
    initial_offset_: usize,
    resyncing_: bool,
}

impl Reader {
    fn new(
        file: Arc<Mutex<dyn SequentialFile>>,
        reporter: Option<Box<dyn Reporter>>,
        checksum: bool,
        initial_offset: usize,
    ) -> Self {
        Reader {
            file_: file,
            reporter_: reporter,
            checksum_: checksum,
            backing_store_: vec![0; K_BLOCK_SIZE],
            buffer_: Slice::new_from_empty(),
            eof_: false,
            last_record_offset_: 0,
            end_of_buffer_offset_: 0,
            initial_offset_: initial_offset,
            resyncing_: initial_offset > 0,
        }
    }
    fn report_corruption(&mut self, bytes: u64, reason: String) {
        self.report_drop(bytes, Status::corruption(reason.as_str(), None))
    }

    fn report_drop(&mut self, bytes: u64, reason: Status) {
        if let Some(report) = self.reporter_.as_mut() {
            if (self.end_of_buffer_offset_ as i64 - self.buffer_.size() as i64 - bytes as i64)
                >= self.initial_offset_ as i64
            {
                report.corruption(bytes as usize, &reason);
            }
        }
    }

    fn skip_to_initial_block(&mut self) -> bool {
        let offset_in_block = self.initial_offset_ % (K_BLOCK_SIZE);
        let mut block_start_location = self.initial_offset_ - offset_in_block;
        if offset_in_block > K_BLOCK_SIZE - 6 {
            block_start_location += K_BLOCK_SIZE;
        }
        self.end_of_buffer_offset_ = block_start_location as u64;
        if block_start_location > 0 {
            let skip_status = self.file_.lock().unwrap().skip(block_start_location as i64);
            if !skip_status.is_ok() {
                self.report_drop(block_start_location as u64, skip_status);
                return false;
            }
        }
        true
    }
}
