use crate::obj::Slice;
use crate::unit::filter_policy::FilterPolicy;
use crate::unit;
struct BloomFilterPolicy {
    bits_per_key: usize,
    k: u8,
}
fn bloom_hash(key :&Slice) -> u32 {
    return unit::hash(key.data(), 0xbc9f1d34);
}


impl FilterPolicy for BloomFilterPolicy {
    fn name() -> &'static str {
        "leveldb.BuiltinBloomFilter2"
    }

    fn create_filter(&self, keys: &[Slice], n:usize ,dst: &mut Vec<u8>) {
        let mut bits = n* self.bits_per_key;
        if bits <64 {
            bits = 64;
        }
        let bytes = (bits+7)/8;
        bits = bytes * 8;
        let init_size = dst.len();
        dst.resize(init_size + bytes, 0);
        dst.push(self.k);
        let array = &mut dst[init_size..];
        for i in 0..n {
            let mut h = bloom_hash(&keys[i]) as usize;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % bits;
                array[bit_pos /8] |= 1<<(bit_pos % 8);
                h += delta;
            }
        }
    }

    fn key_may_match(key: &Slice, bloom_filter: &Slice)->bool {
        let len = bloom_filter.len();
        if len < 2 {
            return false;
        }
        let array = bloom_filter.data();
        let bits = (len-1)*8;
        let k = array[len-1];
        if k> 30 {
            return true;
        }
        let mut h = bloom_hash(key) as usize;
        let delta = (h >> 17) | (h << 15);
        for _ in 0..k {
            let bit_pos = h % bits;
            if array[bit_pos /8] & (1<<(bit_pos % 8)) == 0 {
                return false;
            }
            h += delta;
        }
        true
    }
}