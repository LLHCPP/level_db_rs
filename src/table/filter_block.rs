use crate::obj::slice::Slice;
use crate::util::coding::{decode_fixed32, put_fixed32};
use crate::util::filter_policy::FilterPolicy;
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

const K_FILTER_BASE_LG: u8 = 11;
const K_FILTER_BASE: u64 = 1 << K_FILTER_BASE_LG;
struct FilterBlockBuilder {
    policy_: Arc<dyn FilterPolicy>,
    keys_: BytesMut,
    start_: Vec<usize>,
    result: BytesMut,
    tmp_keys: Vec<Slice>,
    filter_offsets_: Vec<u32>,
}

pub(crate) struct FilterBlockReader {
    policy_: Arc<dyn FilterPolicy>,
    data: Slice,    // 数据的开始
    offset_: usize, //filter分块位置数组
    num_: usize,    // 一共多少个filter块
    base_lg_: u8,   // 2<<base_lg_的数据一个过滤块
}

impl FilterBlockBuilder {
    pub fn new(policy: Arc<dyn FilterPolicy>) -> FilterBlockBuilder {
        FilterBlockBuilder {
            policy_: policy,
            keys_: BytesMut::new(),
            start_: vec![],
            result: BytesMut::new(),
            tmp_keys: vec![],
            filter_offsets_: vec![],
        }
    }
    fn generate_filter(&mut self) {
        let num_keys = self.start_.len();
        if num_keys == 0 {
            self.filter_offsets_.push(self.result.len() as u32);
            return;
        }
        self.start_.push(self.keys_.len());
        self.tmp_keys.clear();
        self.tmp_keys.reserve(num_keys);
        for i in 0..num_keys {
            let base = &self.keys_.as_ref()[self.start_[i]..];
            let key_len = self.start_[i + 1] - self.start_[i];
            self.tmp_keys.push(Slice::new_from_ptr(&base[..key_len]));
        }
        self.filter_offsets_.push(self.result.len() as u32);
        self.policy_.create_filter(&self.tmp_keys, &mut self.result);
        self.tmp_keys.clear();
        self.keys_.clear();
        self.start_.clear();
    }

    fn start_block(&mut self, block_offset: u64) {
        let filter_index = block_offset / K_FILTER_BASE;
        assert!(filter_index >= self.filter_offsets_.len() as u64);
        while filter_index > self.filter_offsets_.len() as u64 {
            self.generate_filter();
        }
    }

    fn add_key(&mut self, key: &Slice) {
        self.start_.push(self.keys_.len());
        self.keys_.put_slice(key.data());
    }

    fn finish(&mut self) -> Slice {
        if !self.start_.is_empty() {
            self.generate_filter();
        }
        let array_offset = self.result.len();
        for i in 0..self.filter_offsets_.len() {
            put_fixed32(&mut self.result, self.filter_offsets_[i]);
        }
        put_fixed32(&mut self.result, array_offset as u32);
        self.result.put_u8(K_FILTER_BASE_LG);
        Slice::new_from_ptr(self.result.as_ref())
    }
}

impl FilterBlockReader {
    pub fn new(policy: Arc<dyn FilterPolicy>, contents: Slice) -> FilterBlockReader {
        let n = contents.len();
        let base_lag = contents[n - 1];
        let mut res = FilterBlockReader {
            policy_: policy,
            data: contents,
            offset_: 0,
            num_: 0,
            base_lg_: 0,
        };
        if n < 5 {
            return res;
        }
        res.base_lg_ = base_lag;
        let data = res.data();
        let last_word = decode_fixed32(&data[n - 5..]);
        if last_word > (n - 5) as u32 {
            return res;
        }
        res.offset_ = last_word as usize;
        res.num_ = (n - 5 - last_word as usize) / 4;
        res
    }
    pub fn data(&self) -> &[u8] {
        self.data.data()
    }

    pub fn offset(&self) -> &[u8] {
        &self.data.data()[self.offset_..]
    }

    fn key_may_match(&self, key: &Slice, block_offset: u64) -> bool {
        let index = block_offset >> self.base_lg_;
        if index < self.num_ as u64 {
            let start = decode_fixed32(&self.offset()[(index * 4) as usize..]);
            let limit = decode_fixed32(&self.offset()[(index * 4 + 4) as usize..]);

            if start <= limit && limit <= self.offset_ as u32 {
                let filter = Slice::new_from_ptr(&self.data()[start as usize..limit as usize]);
                return self.policy_.key_may_match(&key, &filter);
            } else if (start == limit) {
                return false;
            }
        }
        true
    }
}
