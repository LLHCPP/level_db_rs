use crate::obj::byte_buffer::ByteBuffer;
use crate::obj::options::{CompressionType, ReadOptions};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::coding::{decode_fixed32, get_varint64, put_fixed32, put_varint64};
use crate::util::random_access_file::RandomAccessFile;
use bytes::BytesMut;
use num_traits::FromPrimitive;
use snap::raw::{decompress_len, Decoder};
use zstd_safe::DCtx;
const K_MAX_ENCODED_LENGTH: u64 = 10 + 10;
#[derive(Debug, Clone)]
pub(crate) struct BlockHandle {
    offset: u64,
    size: u64,
}
impl BlockHandle {
    fn new() -> BlockHandle {
        BlockHandle { offset: 0, size: 0 }
    }
    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }
    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn encode_to(&self, dst: &mut BytesMut) {
        debug_assert!(self.offset != 0);
        debug_assert!(self.size != 0);
        put_varint64(dst, self.offset as u64);
        put_varint64(dst, self.size as u64);
    }
    pub fn decode_from(&mut self, input: &mut Slice) -> Status {
        if (get_varint64(input, &mut self.offset) && get_varint64(input, &mut self.size)) {
            Status::ok()
        } else {
            Status::corruption("bad block handle", None)
        }
    }
}

pub const K_ENCODED_LENGTH: u64 = 2 * K_MAX_ENCODED_LENGTH + 8;
#[derive(Debug, Clone)]
pub struct Footer {
    meta_index_handle: BlockHandle,
    index_handle: BlockHandle,
}
const K_TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

// 1-byte type + 32-bit crc
const K_BLOCK_TRAILER_SIZE: u64 = 5;

impl Footer {
    pub fn new() -> Footer {
        Footer {
            meta_index_handle: BlockHandle::new(),
            index_handle: BlockHandle::new(),
        }
    }
    pub fn meta_index_handle(&self) -> &BlockHandle {
        &self.meta_index_handle
    }

    pub fn index_handle(&self) -> &BlockHandle {
        &self.index_handle
    }

    pub fn set_meta_index_handle(&mut self, meta_index_handle: &BlockHandle) {
        self.meta_index_handle = meta_index_handle.clone();
    }

    pub fn set_index_handle(&mut self, index_handle: &BlockHandle) {
        self.index_handle = index_handle.clone();
    }

    fn encode_to(&self, dst: &mut BytesMut) {
        let original_size = dst.len();
        self.meta_index_handle.encode_to(dst);
        self.index_handle.encode_to(dst);
        dst.resize((2 * K_MAX_ENCODED_LENGTH) as usize, 0);
        put_fixed32(dst, (K_TABLE_MAGIC_NUMBER & 0xffffffff) as u32);
        put_fixed32(dst, (K_TABLE_MAGIC_NUMBER >> 32) as u32);
        debug_assert!(dst.len() == original_size + K_ENCODED_LENGTH as usize)
    }

    pub fn decode_from(&mut self, input: &mut Slice) -> Status {
        if input.size() < K_ENCODED_LENGTH as usize {
            return Status::corruption("input is too short to be an sstable", None);
        }
        let magic_ptr = &input.data()[(K_ENCODED_LENGTH - 8) as usize..];
        let magic_lo = decode_fixed32(magic_ptr);
        let magic_hi = decode_fixed32(&magic_ptr[4..]);
        let magic = ((magic_hi as u64) << 32) | (magic_lo as u64);
        if magic != K_TABLE_MAGIC_NUMBER {
            return Status::corruption("not an sstable (bad magic number)", None);
        }
        let mut result = self.meta_index_handle.decode_from(input);
        if result.is_ok() {
            result = self.index_handle.decode_from(input);
        }
        if result.is_ok() {
            input.advance(K_ENCODED_LENGTH as usize);
        }
        result
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BlockContents {
    pub(crate) data: Slice,
    pub(crate) cachable: bool,
    /*    pub(crate) heap_allocated: bool,*/
}

fn read_block<T: RandomAccessFile>(
    file: &mut T,
    options: &ReadOptions,
    handle: &BlockHandle,
) -> Result<BlockContents, Status> {
    let n = handle.size();
    let mut buf = ByteBuffer::new((n + K_BLOCK_TRAILER_SIZE) as usize);
    let contents = file.read(
        handle.offset(),
        (n + K_BLOCK_TRAILER_SIZE) as usize,
        Some(buf.as_mut_slice()),
    )?;
    if contents.size() != (n + K_BLOCK_TRAILER_SIZE) as usize {
        return Err(Status::corruption("truncated block read", None));
    }
    let read_data = contents.data();
    if options.verify_checksums {
        let crc = crate::util::crc32c::unmask(decode_fixed32(&read_data[(n + 1) as usize..]));
        let actual = crate::util::crc32c::value(&read_data[..(n + 1) as usize]);
        if crc != actual {
            return Err(Status::corruption("block checksum mismatch", None));
        }
    }
    let compression = CompressionType::from_u8(read_data[n as usize]);
    if compression.is_none() {
        return Err(Status::corruption(
            "bad block type or unsupported block compression type",
            None,
        ));
    }
    let compression = compression.unwrap();
    match compression {
        CompressionType::None => {
            if read_data.as_ptr() != buf.as_slice().as_ptr() {
                let result = BlockContents {
                    data: contents,
                    cachable: false,
                };
                Ok(result)
            } else {
                buf.resize(read_data.len());
                let result = BlockContents {
                    data: Slice::new_from_buff(buf),
                    cachable: true,
                };
                Ok(result)
            }
        }
        CompressionType::Snappy => {
            let u_length = decompress_len(read_data);
            if u_length.is_err() {
                return Err(Status::corruption(
                    "corrupted snappy compressed block length",
                    None,
                ));
            }
            let mut uncompressed = ByteBuffer::new(u_length.unwrap());
            let success = Decoder::new().decompress(read_data, uncompressed.as_mut_slice());
            if success.is_err() {
                return Err(Status::corruption("corrupted zstd compressed block", None));
            }
            let result = BlockContents {
                data: Slice::new_from_buff(uncompressed),
                cachable: true,
            };
            Ok(result)
        }
        CompressionType::Zstd => {
            let u_length = zstd_safe::get_frame_content_size(read_data);
            if u_length.is_err() {
                return Err(Status::corruption(
                    "corrupted zstd compressed block length",
                    None,
                ));
            }
            let u_length = u_length.unwrap();
            if u_length.is_none() {
                return Err(Status::corruption(
                    "corrupted zstd compressed block length",
                    None,
                ));
            }
            let mut uncompressed = ByteBuffer::new(u_length.unwrap() as usize);
            let mut ctx = DCtx::create();
            let res = ctx.decompress(uncompressed.as_mut_slice(), read_data);
            if res.is_err() {
                return Err(Status::corruption("corrupted zstd compressed block", None));
            }
            let result = BlockContents {
                data: Slice::new_from_buff(uncompressed),
                cachable: true,
            };
            Ok(result)
        }
    }
}
