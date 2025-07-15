use crate::db::log_format::{RecordType, K_BLOCK_SIZE, K_HEADER_SIZE, K_MAX_RECORD_TYPE};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::crc32c;
use crate::util::writable_file::WritableFile;
use std::sync::{Arc, Mutex};

struct LogWriter {
    dest_: Arc<Mutex<dyn WritableFile>>,
    block_offset_: usize,
    type_crc_: [u32; K_MAX_RECORD_TYPE + 1],
}

fn init_type_crc(type_crc: &mut [u32]) {
    for i in 0..K_MAX_RECORD_TYPE + 1 {
        let t = i as u8;
        type_crc[i] = crc32c::value(&[t]);
    }
}

impl LogWriter {
    pub fn new(dest: Arc<Mutex<dyn WritableFile>>) -> Self {
        let mut obj = LogWriter {
            dest_: dest,
            block_offset_: 0,
            type_crc_: [0; K_MAX_RECORD_TYPE + 1],
        };
        init_type_crc(&mut obj.type_crc_);
        obj
    }
    pub fn new_dest_length(dest: Arc<Mutex<dyn WritableFile>>, dest_length: u64) -> Self {
        let mut obj = LogWriter {
            dest_: dest,
            block_offset_: dest_length as usize % K_BLOCK_SIZE,
            type_crc_: [0; K_MAX_RECORD_TYPE + 1],
        };
        init_type_crc(&mut obj.type_crc_);
        obj
    }
    fn add_record(&mut self, slice: &Slice) -> Result<(), Status> {
        let ptr = slice.data();
        let left = slice.size();
        let s = Status::ok();
        let begin = true;
        loop {
            let leftover = K_BLOCK_SIZE as i64 - self.block_offset_ as i64;
            debug_assert!(leftover >= 0);
            if leftover < K_HEADER_SIZE as i64 {
                if leftover > 0 {
                    self.dest_.lock().unwrap().append(
                        &Slice::new_from_ptr(&[0x00;7][..(leftover as usize)]),
                    );
                }
                self.block_offset_ = 0;
            }
            assert!(K_BLOCK_SIZE as i64 - self.block_offset_ as i64 - K_HEADER_SIZE as i64 >= 0);
            let avail = K_BLOCK_SIZE - self.block_offset_ - K_HEADER_SIZE;
            let fragment_length = if left < avail {
                left
            } else {
                avail
            };
           
            let end = left == fragment_length;
            let record_type = if begin && end {
                RecordType::KFullType
            } else if begin {
                RecordType::KFirstType
            } else if end{
                RecordType::KLastType
            } else { 
                RecordType::KMiddleType
            };
            
            




        }
    }

}
