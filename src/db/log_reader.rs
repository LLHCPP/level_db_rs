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
    reporter_: Box<dyn Reporter>,
    checksum_: bool,
    backing_store_: Vec<u8>,
    buffer_: Slice,
    eof_: bool,
    last_record_offset_: u64,
    end_of_buffer_offset_: u64,
    initial_offset_: u64,
    resyncing_: bool,
}

impl Reader {
    fn new(
        file: Arc<Mutex<dyn SequentialFile>>,
        reporter: Box<dyn Reporter>,
        checksum: bool,
        initial_offset: u64,
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
}
