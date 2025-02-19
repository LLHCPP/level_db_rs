use bytes::Bytes;
use crate::obj::slice::Slice;
use crate::unit::filter_policy::FilterPolicy;
use crate::unit::hash;


struct BloomFilterPolicy {
    bits_per_key: usize,
    k: u8,
}
fn bloom_hash(key :&Slice) -> u32 {
    hash(key.data(), 0xbc9f1d34)
}

impl BloomFilterPolicy {
    fn new(bits_per_key: usize) -> Self {
       let mut k = (bits_per_key as f64 * 0.69) as usize;
        if k<1 {
            k=1;
        }else if k>30 {
            k = 30;
        }
        BloomFilterPolicy {
            bits_per_key,
            k: k as u8,
        }
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &'static str {
        "leveldb.BuiltinBloomFilter2"
    }

    fn create_filter(&self, keys: &[Slice], dst: &mut Bytes) {
        let mut bits = keys.len()* self.bits_per_key;
        if bits <64 {
            bits = 64;
        }
        let bytes = (bits+7)/8;
        bits = bytes * 8;
        let init_size = dst.len();
        let mut vec = dst.to_vec();
        vec.resize(init_size + bytes, 0);
        vec.push(self.k);
        let array = &mut vec[init_size..];
        for key in keys.iter() {
            let mut h = bloom_hash(&key) as usize;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % bits;
                array[bit_pos /8] |= 1<<(bit_pos % 8);
                h += delta;
            }
        }
        *dst = Bytes::from(vec);
    }

    fn key_may_match(&self, key: &Slice, bloom_filter: &Bytes)->bool {
        let len = bloom_filter.len();
        if len < 2 {
            return false;
        }
        let array = bloom_filter;
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
struct BloomTest {
    policy: BloomFilterPolicy,
    filter: Bytes,
    keys: Vec<String>,
}

impl BloomTest {
    fn key(i:u32, mut buffer: Box<[u8]>) -> Slice{
        buffer[0] = i as u8;
        buffer[1] = (i>>8) as u8;
        buffer[2] = (i>>16) as u8;
        buffer[3] = (i>>24) as u8;
        Slice::new_from_vec(buffer.into_vec())
    }
    fn new(bits_per_key: usize) -> Self {
        Self {
            policy: BloomFilterPolicy::new(bits_per_key),
            filter: Bytes::new(),
            keys: Vec::new(),
        }
    }
    fn reset(&mut self) {
        self.keys.clear();
        self.filter.clear();
    }
    fn add(&mut self, s: &Slice) {
        self.keys.push(s.to_string());
    }
    fn build(&mut self) {
        let key_slices:Vec<Slice> = self.keys.iter().map(|k| Slice::new_from_string(k.clone())).collect();
        self.filter.clear();
        self.policy.create_filter(&key_slices, &mut self.filter);
        self.keys.clear();
        // if verbose >= 2 { self.dump_filter(); }
    }
    fn filter_size(&self) -> usize{
        self.filter.len()
    }
    fn matches(&mut self, s: &Slice) -> bool {
        if !self.keys.is_empty() {
            self.build();
        }
        self.policy.key_may_match(s, &self.filter)
    }
    fn false_positive_rate(&mut self) -> f64 {
        let buffer:[u8;4] = [0;4];
        let mut result = 0u32;
        for i in 0..10000 {
            let key = BloomTest::key(i, Box::new(buffer));
            if self.matches(&key) {
                result += 1;
            }
        }
        result as f64 /10000.0
    }
}





#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use bytes::Bytes;
    use crate::obj::slice::Slice;
    use crate::unit::bloom_filter_policy::{BloomFilterPolicy, BloomTest};
    use crate::unit::filter_policy::FilterPolicy;

    #[test]
    fn test_empty_filter() {
        let mut test = BloomTest::new(10);
        assert!(!test.matches(&("hello".into())));
        assert!(!test.matches(&("world".into())));
    }
    #[test]
    fn test_small() {
        let mut test = BloomTest::new(10);
        test.add(&("hello".into()));
        test.add(&("world".into()));
        assert!(test.matches(&("hello".into())));
        assert!(test.matches(&("world".into())));
        assert!(!test.matches(&("x".into())));
        assert!(!test.matches(&("foo".into())));
    }
}