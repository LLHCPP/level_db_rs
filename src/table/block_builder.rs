use crate::obj::options::Options;
use crate::obj::slice::Slice;
use crate::util::coding::{put_fixed32, put_varint32};
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use bytes::{BufMut, BytesMut};
use std::cmp::{min, Ordering};
use std::hash::Hash;

struct BlockBuilder<C, E, K, V>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
{
    option: Options<C, E, K, V>,
    buffer_: BytesMut,
    restarts_: Vec<u32>,
    counter_: i32, //上一个restart index之后，存储了多少个kv
    finished: bool,
    last_key: BytesMut,
}

impl<C, E, K, V> BlockBuilder<C, E, K, V>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
{
    fn new(option: Options<C, E, K, V>) -> BlockBuilder<C, E, K, V> {
        assert!(option.block_restart_interval >= 1);
        BlockBuilder {
            option,
            buffer_: BytesMut::new(),
            restarts_: vec![0],
            counter_: 0,
            finished: false,
            last_key: BytesMut::new(),
        }
    }

    fn reset(&mut self) {
        self.buffer_.clear();
        self.restarts_.clear();
        self.restarts_.push(0);
        self.counter_ = 0;
        self.finished = false;
        self.last_key.clear();
    }

    fn add(&mut self, key: &Slice, value: &Slice) {
        let last_key_piece = Slice::new_from_ptr(&self.last_key);
        assert!(!self.finished);
        assert!(self.counter_ <= self.option.block_restart_interval as i32);
        assert!(
            self.buffer_.is_empty()
                || self.option.comparator.compare(key, &last_key_piece) == Ordering::Greater
        );
        let mut shared = 0;
        if self.counter_ < self.option.block_restart_interval as i32 {
            let min_len = min(key.len(), last_key_piece.len());
            while shared < min_len && last_key_piece[shared] == key[shared] {
                shared += 1;
            }
        } else {
            self.restarts_.push(self.buffer_.len() as u32);
            self.counter_ = 0;
        }
        let non_shared = key.len() - shared;
        put_varint32(&mut self.buffer_, shared as u32);
        put_varint32(&mut self.buffer_, non_shared as u32);
        put_varint32(&mut self.buffer_, value.size() as u32);
        self.buffer_.put_slice(&key.data()[shared..]);
        self.buffer_.put_slice(value.data());
        self.last_key.truncate(shared);
        self.last_key.put_slice(&key.data()[shared..]);
        debug_assert!(Slice::new_from_ptr(&self.last_key) == *key);
        self.counter_ += 1;
    }

    fn current_size_estimate(&self) -> usize {
        self.buffer_.len() + self.restarts_.len() * size_of::<u32>() + size_of::<u32>()
    }

    fn finish(&mut self) -> Slice {
        // Append restart array
        for i in 0..self.restarts_.len() {
            put_fixed32(&mut self.buffer_, self.restarts_[i]);
        }
        put_fixed32(&mut self.buffer_, self.restarts_.len() as u32);
        self.finished = true;
        Slice::new_from_ptr(&self.buffer_)
    }

    fn empty(&self) -> bool {
        self.buffer_.is_empty()
    }
}
