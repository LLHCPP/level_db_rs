use crate::db::log_format::{RecordType, K_BLOCK_SIZE, K_HEADER_SIZE, K_MAX_RECORD_TYPE};
use crate::obj::byte_buffer::ByteBuffer;
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util;
use crate::util::coding::decode_fixed32;
use crate::util::crc32c;
use crate::util::sequential_file::SequentialFile;
use bytes::{BufMut, BytesMut};
use std::sync::{Arc, Mutex};

#[repr(usize)]
enum ReadStatus {
    KEof = K_MAX_RECORD_TYPE + 1,
    KBadRecord = K_MAX_RECORD_TYPE + 2,
}

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
    fn report_corruption(&mut self, bytes: u64, reason: &str) {
        self.report_drop(bytes, Status::corruption(reason, None))
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
    fn read_physical_record(&mut self) -> Result<(Slice, RecordType), ReadStatus> {
        loop {
            if self.buffer_.size() < K_HEADER_SIZE {
                if !self.eof_ {
                    self.buffer_.clear();
                    let res = self.file_.lock().unwrap().read(K_BLOCK_SIZE);
                    match res {
                        Ok(buffer) => {
                            self.end_of_buffer_offset_ += buffer.size() as u64;
                            self.buffer_ = buffer;
                            if self.buffer_.size() < K_BLOCK_SIZE {
                                self.eof_ = true;
                            }
                        }
                        Err(e) => {
                            self.buffer_.clear();
                            self.report_drop(K_BLOCK_SIZE as u64, e);
                            self.eof_ = true;
                            return Err(ReadStatus::KEof);
                        }
                    }
                    continue;
                } else {
                    self.buffer_.clear();
                    return Err(ReadStatus::KEof);
                }
            }
            let header = self.buffer_.data();
            let a = header[4] as u32 & 0xff;
            let b = header[5] as u32 & 0xff;
            let data_type = header[6];
            let length = (a | (b << 8)) as usize;
            if K_HEADER_SIZE + length > self.buffer_.size() {
                let drop_size = self.buffer_.size();
                self.buffer_.clear();
                if !self.eof_ {
                    self.report_corruption(drop_size as u64, "bad record length");
                    return Err(ReadStatus::KBadRecord);
                }
                return Err(ReadStatus::KEof);
            }

            if data_type == RecordType::KZeroType as u8 && length == 0 {
                self.buffer_.clear();
                return Err(ReadStatus::KBadRecord);
            }

            if self.checksum_ {
                let expected_crc = crc32c::unmask(decode_fixed32(header));
                let actual_crc = crc32c::value(&header[6..length + 7]);
                if expected_crc != actual_crc {
                    let drop_size = self.buffer_.size();
                    self.buffer_.clear();
                    self.report_corruption(drop_size as u64, "checksum mismatch");
                    return Err(ReadStatus::KBadRecord);
                }
            }
            let buffer_size = self.buffer_.size();
            let res = Slice::new_from_array(&header[K_HEADER_SIZE..K_HEADER_SIZE + length]);
            self.buffer_.remove_prefix(K_HEADER_SIZE + length);
            if (self.end_of_buffer_offset_ as usize)
                < (self.initial_offset_ + buffer_size - K_HEADER_SIZE + length)
            {
                return Err(ReadStatus::KBadRecord);
            }

            let enum_value: RecordType = unsafe { std::mem::transmute(data_type) };
            return Ok((res, enum_value));
        }
    }

    fn read_record(&mut self, record: &mut Slice, scratch: &mut BytesMut) {
        if self.last_record_offset_ < self.initial_offset_ as u64 {
            if !self.skip_to_initial_block() {
                return;
            }
        }
        scratch.clear();
        record.clear();
        let in_fragmented_record = false;
        let prospective_record_offset = 0;
        let fragment = Slice::new_from_empty();
        loop { /* let record_type = self*/ }
    }
}
