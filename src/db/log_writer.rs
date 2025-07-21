use crate::db::log_format::{RecordType, K_BLOCK_SIZE, K_HEADER_SIZE, K_MAX_RECORD_TYPE};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::coding::{encode_fixed32, encode_fixed64};
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

    fn emit_physical_record(&mut self, t: RecordType, data_ptr: &[u8]) -> Status {
        let length = data_ptr.len();
        assert!(length <= 0xffff);
        assert!(self.block_offset_ + K_HEADER_SIZE + length <= K_BLOCK_SIZE);
        let mut buf: [u8; K_HEADER_SIZE] = [0; K_HEADER_SIZE];
        buf[4] = (length & 0xff) as u8;
        buf[5] = (length >> 8) as u8;
        buf[6] = t.clone() as u8;
        let mut crc = crc32c::extend(self.type_crc_[t as usize], data_ptr);
        crc = crc32c::mask(crc);
        encode_fixed32(&mut buf, crc);
        let mut dest_lock = self.dest_.lock().unwrap();
        let mut s = dest_lock.append(&Slice::new_from_ptr(&buf));
        if s.is_ok() {
            s = dest_lock.append(&Slice::new_from_ptr(&data_ptr));
            if s.is_ok() {
                s = dest_lock.flush();
            }
        }
        drop(dest_lock);
        self.block_offset_ += K_HEADER_SIZE + length;
        s
    }

    fn add_record(&mut self, slice: &Slice) -> Status {
        let mut ptr = slice.data();
        let mut left = slice.size();
        let s = Status::ok();
        let mut begin = true;
        loop {
            let leftover = K_BLOCK_SIZE as i64 - self.block_offset_ as i64;
            debug_assert!(leftover >= 0);
            if leftover < K_HEADER_SIZE as i64 {
                if leftover > 0 {
                    self.dest_
                        .lock()
                        .unwrap()
                        .append(&Slice::new_from_ptr(&[0x00; 7][..(leftover as usize)]));
                }
                self.block_offset_ = 0;
            }
            assert!(K_BLOCK_SIZE as i64 - self.block_offset_ as i64 - K_HEADER_SIZE as i64 >= 0);
            let avail = K_BLOCK_SIZE - self.block_offset_ - K_HEADER_SIZE;
            let fragment_length = if left < avail { left } else { avail };

            let end = left == fragment_length;
            let record_type = if begin && end {
                RecordType::KFullType
            } else if begin {
                RecordType::KFirstType
            } else if end {
                RecordType::KLastType
            } else {
                RecordType::KMiddleType
            };
            let s = self.emit_physical_record(record_type, &ptr[..fragment_length]);
            ptr = &ptr[fragment_length..];
            left -= fragment_length;
            begin = false;

            if (!s.is_ok() || left <= 0) {
                break;
            }
        }
        s
    }
}
