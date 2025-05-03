use crate::obj::byte_buffer::ByteBuffer;
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::format::BlockContents;
use crate::table::iterator::Iter;
use crate::util::coding::{decode_fixed32, get_varint32ptr};
use crate::util::comparator::Comparator;
use std::cmp::Ordering;

struct Block {
    data: ByteBuffer,
    restart_offset_: usize,
}

/// [Keys and Values (data part)]&emsp;&ensp;[Restart Points]&emsp;&emsp;[Metadata]
/// ^&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;^&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;^
/// data_&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;restarts_&emsp;&emsp;&emsp;end of block

/*Block Data part:
+---------------------------------------+
| shared | non_shared | value_length | non_shared key | value |
+---------------------------------------+
|  0x02  |   0x03     |    0x04      |   "abc"        | "val" |
+---------------------------------------+*/

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

///comparator_：比较器，用于比较键的顺序。
///
///data_：指向 Block 数据部分的起始地址（偏移量 0）。
///
///restarts_：重启点数组的起始偏移量（数据部分的末尾）。
///
///num_restarts_：重启点数组中的条目数（uint32_t 数量）。
///
///current_：当前记录在 data_ 中的偏移量，指向当前键值对的开始位置。
///
///restart_index_：当前记录所属的重启点索引，表示 current_ 位于哪个重启点块。
///
///key_：当前记录的完整键（字符串形式）。
///
///value_：当前记录的值（Slice 形式，指向 data_ 中的值数据）。
///
///status_：迭代器的状态，用于记录错误（如数据损坏）。

/*位置：restarts_ 是一个偏移量，指向 Block 数据部分末尾的重启点数组。重启点数组由一组 uint32_t 值组成，每个值表示 Block 中某个键值对记录的起始偏移量（相对于 data_）。
存储内容：重启点数组记录的是一些特定键值对的偏移量，这些键值对被称为重启点记录。这些记录的特点是：
完整键存储：在重启点记录中，键不使用共享前缀压缩（即 shared=0），存储完整的键。
分块优化：重启点将 Block 数据部分划分为多个小块（block），每个块包含若干键值对，块的第一个记录是重启点记录。
数量：重启点数组的长度由 num_restarts_ 指定，表示数组中 uint32_t 条目的数量。*/

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

    fn corruption_error(&mut self) {
        self.current_ = self.restarts_;
        self.restart_index_ = self.num_restarts_;
        self.status = Status::corruption("block has corrupted restart points", None);
        self.key_.clear();
        self.value_.clear();
    }

    fn parse_next_key(&mut self) -> bool {
        self.current_ = self.next_entry_offset();
        let p = self.current_ as usize;
        let limit = self.restarts_ as usize;
        if p >= limit {
            self.current_ = self.restarts_;
            self.restart_index_ = self.num_restarts_;
            return false;
        }
        let mut shared = 0;
        let mut non_shared = 0;
        let mut value_length = 0;
        let kv_ptr = decode_entry(
            &self.data_[p..limit],
            &mut shared,
            &mut non_shared,
            &mut value_length,
        );
        if kv_ptr.is_none() || self.key_.len() < shared as usize {
            self.corruption_error();
            false
        } else {
            let kv_ptr = kv_ptr.unwrap();
            self.key_.reserve((shared + non_shared) as usize);
            // 调整到 shared 长度（截断或清空）
            self.key_.truncate(shared as usize);
            self.key_.push_str(
                std::str::from_utf8(&kv_ptr[..non_shared as usize]).expect("Invalid UTF-8"),
            );
            self.value_ = Slice::new_from_ptr(
                &kv_ptr[non_shared as usize..(non_shared + value_length) as usize],
            );
            while self.restart_index_ + 1 < self.num_restarts_
                && self.get_restart_point(self.restart_index_ + 1) < self.current_
            {
                self.restart_index_ += 1;
            }
            true
        }
    }
}
impl<C> Iter for BlockIterator<C>
where
    C: Comparator,
{
    fn valid(&self) -> bool {
        self.current_ < self.restarts_
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_next_key();
    }

    fn seek_to_last(&mut self) {
        self.seek_to_restart_point(self.num_restarts_ - 1);
        self.parse_next_key();
    }

    fn seek(&mut self, target: &Slice) {
        let mut left = 0;
        let mut right = self.num_restarts_ - 1;
        let mut key_compare = Ordering::Equal;
        if self.valid() {
            key_compare = self.compare(&Slice::new_from_string_buffer(&self.key_), target);
            if key_compare == Ordering::Less {
                left = self.restart_index_;
            } else if key_compare == Ordering::Greater {
                right = self.restart_index_;
            } else {
                return;
            }
        }

        while left < right {
            let mid = (right - left + 1) / 2 + left;
            let region_offset = self.get_restart_point(mid);
            let mut shared = 0u32;
            let mut non_shared = 0u32;
            let mut value_length = 0u32;
            let kv_ptr = decode_entry(
                &self.data_[region_offset as usize..self.restarts_ as usize],
                &mut shared,
                &mut non_shared,
                &mut value_length,
            );
            if kv_ptr.is_none() || shared != 0 {
                self.corruption_error();
                return;
            }
            let kv_ptr = kv_ptr.unwrap();
            let mid_key = Slice::new_from_ptr(&kv_ptr[..non_shared as usize]);
            let mid_compare = self.compare(&mid_key, target);
            if mid_compare == Ordering::Less {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        assert!(key_compare == Ordering::Equal || self.valid());
        let skip_seek = left == self.restart_index_ && key_compare == Ordering::Less;
        if !skip_seek {
            self.seek_to_restart_point(left);
        }
        loop {
            if !self.parse_next_key() {
                return;
            }
            if self.compare(&Slice::new_from_string_buffer(&self.key_), target) >= Ordering::Equal {
                return;
            }
        }
    }

    fn next(&mut self) {
        assert!(self.valid());
        self.parse_next_key();
    }

    fn prev(&mut self) {
        assert!(self.valid());
        let original_index = self.current_;
        while self.get_restart_point(self.restart_index_) >= original_index {
            if self.restart_index_ == 0 {
                self.current_ = self.restarts_;
                self.restart_index_ = self.num_restarts_;
                return;
            }
            self.restart_index_ -= 1;
        }
        self.seek_to_restart_point(self.restart_index_);
        while self.parse_next_key() && self.next_entry_offset() < original_index {}
    }

    fn key(&self) -> Slice {
        assert!(self.valid());
        Slice::new_from_string_buffer(&self.key_)
    }

    fn value(&self) -> Slice {
        assert!(self.valid());
        self.value_.clone()
    }

    fn status(&self) -> Status {
        self.status.clone()
    }
}
