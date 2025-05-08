use crate::obj::byte_buffer::ByteBuffer;
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::coding::{decode_fixed32, get_varint64, put_fixed32, put_varint64};
use crate::util::random_access_file::RandomAccessFile;
use bytes::BytesMut;

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

const K_ENCODED_LENGTH: u64 = 2 * K_MAX_ENCODED_LENGTH + 8;
#[derive(Debug, Clone)]
struct Footer {
    meta_index_handle: BlockHandle,
    index_handle: BlockHandle,
}
const K_TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

// 1-byte type + 32-bit crc
const K_BLOCK_TRAILER_SIZE: u64 = 5;

impl Footer {
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

    fn decode_from(&mut self, input: &mut Slice) -> Status {
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
    pub(crate) data: ByteBuffer,
    cachable: bool,
    heap_allocated: bool,
}

/*fn ReadBlock<T:RandomAccessFile>(file:&mut T, )*/
