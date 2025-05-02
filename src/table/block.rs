use crate::obj::byte_buffer::ByteBuffer;
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::format::BlockContents;
use crate::util::coding::{decode_fixed32, get_varint32ptr};
use crate::util::comparator::Comparator;
use std::cmp::Ordering;

struct Block {
    data: ByteBuffer,
    restart_offset_: usize,
}

impl Block {
    fn new(contents: &BlockContents) -> Block {
        let mut res = Block {
            data: contents.data.clone(),
            restart_offset_: 0,
        };
        let buffer = &mut res.data;

        if buffer.len() < size_of::<u32>() {
            buffer.resize(0);
        } else {
            let num_restarts = decode_fixed32(&buffer[buffer.len() - size_of::<u32>()..]);

            let max_restarts_allowed = (buffer.len() - size_of::<u32>()) / size_of::<u32>();
            if num_restarts > max_restarts_allowed as u32 {
                buffer.resize(0);
            } else {
                res.restart_offset_ =
                    buffer.len() - ((1 + num_restarts as usize) * size_of::<u32>());
            }
        }
        res
    }

    fn num_restarts(&self) -> u32 {
        debug_assert!(self.data.len() >= size_of::<u32>());
        let pos = self.data.len() - size_of::<u32>();
        decode_fixed32(&self.data[pos..])
    }
}

#[inline]
pub fn decode_entry<'a>(
    mut input: &'a [u8],
    shared: &mut u32,
    non_shared: &mut u32,
    value_length: &mut u32,
) -> Option<&'a [u8]> {
    // Check if there are at least 3 bytes available
    if input.len() < 3 {
        return None;
    }

    // Read the first three bytes
    *shared = input[0] as u32;
    *non_shared = input[1] as u32;
    *value_length = input[2] as u32;

    // Fast path: all three values are encoded in one byte each (< 128)
    if (*shared | *non_shared | *value_length) < 128 {
        input = &input[3..];
    } else {
        // Slow path: decode varint for each value
        input = get_varint32ptr(input, shared)?;
        input = get_varint32ptr(input, non_shared)?;
        input = get_varint32ptr(input, value_length)?;
    } // Check if there are enough bytes left for non_shared + value_length
    if input.len() < (*non_shared as usize + *value_length as usize) {
        return None;
    }
    Some(input)
}

struct BlockIterator<C>
where
    C: Comparator,
{
    comparator_: C,
    data_: ByteBuffer,
    restarts_: u32,
    num_restarts_: u32,
    current_: u32,
    restart_index_: u32,
    key_: String,
    value_: Slice,
    status: Status,
}

impl<C> BlockIterator<C>
where
    C: Comparator,
{
    fn new(comparator: C, data: ByteBuffer, restarts: u32, num_restarts: u32) -> Self {
        BlockIterator {
            comparator_: comparator,
            data_: data,
            restarts_: restarts,
            num_restarts_: num_restarts,
            current_: restarts,
            restart_index_: num_restarts,
            key_: String::new(),
            value_: Slice::new_from_str(""),
            status: Status::ok(),
        }
    }
    fn compare(&self, a: &Slice, b: &Slice) -> Ordering {
        self.comparator_.compare(a, b)
    }

    fn next_entry_offset(&self) -> u32 {
        (self.value_.data().as_ptr() as u64 + self.value_.size() as u64
            - self.data_.as_slice().as_ptr() as u64) as u32
    }

    fn get_restart_point(&self, index: u32) -> u32 {
        assert!(index < self.num_restarts_);
        decode_fixed32(
            &self.data_.as_slice()[(self.restarts_ + index * size_of::<u32>() as u32) as usize..],
        )
    }

    fn seek_to_restart_point(&mut self, index: u32) {
        self.key_.clear();
        self.restart_index_ = index;
        let offset = self.get_restart_point(index);
        let mut new_value = self.data_.clone();
        new_value.advance(offset as usize);
        self.value_ = Slice::new_from_buff(new_value);
        self.value_.resize(0);
    }
}
