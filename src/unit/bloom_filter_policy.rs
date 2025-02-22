use crate::obj::slice::Slice;
use crate::unit::filter_policy::FilterPolicy;
use crate::unit::hash;
use bytes::Bytes;
use std::num::Wrapping;

struct BloomFilterPolicy {
    bits_per_key: usize,
    k: u8,
}

impl BloomFilterPolicy {
    fn new(bits_per_key: usize) -> Self {
        let mut k = (bits_per_key as f64 * 0.69) as usize;
        if k < 1 {
            k = 1;
        } else if k > 30 {
            k = 30;
        }
        BloomFilterPolicy {
            bits_per_key,
            k: k as u8,
        }
    }
    fn bloom_hash(key: &Slice) -> u32 {
        hash(key.data(), 0xbc9f1d34)
    }
    fn test()->u32{
        32
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &'static str {
        "leveldb.BuiltinBloomFilter2"
    }
    

    fn create_filter(&self, keys: &[Slice], dst: &mut Bytes) {
        let mut bits = keys.len() * self.bits_per_key;
        if bits < 64 {
            bits = 64;
        }
        let bytes = (bits + 7) / 8;
        bits = bytes * 8;
        let init_size = dst.len();
        let mut vec = dst.to_vec();
        vec.resize(init_size + bytes, 0);
        vec.push(self.k);
        let array = &mut vec[init_size..];
        for key in keys.iter() {
            let mut h = Wrapping(BloomFilterPolicy::bloom_hash(&key));
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h.0 as usize % (bits);
                array[bit_pos / 8] |= 1u8 << (bit_pos % 8);
                h += delta;
            }
        }
        *dst = Bytes::from(vec);
    }

    fn key_may_match(&self, key: &Slice, bloom_filter: &Bytes) -> bool {
        let len = bloom_filter.len();
        if len < 2 {
            return false;
        }
        let array = &bloom_filter.to_vec();
        let bits = (len - 1) * 8;
        let k = array[len - 1];
        if k > 30 {
            return true;
        }
        let mut h = Wrapping(BloomFilterPolicy::bloom_hash(key));
        let delta = (h >> 17) | (h << 15);
        for _ in 0..k {
            let bit_pos = h.0 as usize % (bits);
            if array[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h += delta;
        }
        true
    }
}
#[cfg(test)]
struct BloomTest {
    policy: BloomFilterPolicy,
    filter: Bytes,
    keys: Vec<Slice>,
}

#[cfg(test)]
impl BloomTest {
    fn key(i: u32, buffer: &mut [u8]) -> Slice {
        buffer[0] = i as u8;
        buffer[1] = (i >> 8) as u8;
        buffer[2] = (i >> 16) as u8;
        buffer[3] = (i >> 24) as u8;
        Slice::new_from_array(buffer)
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
    fn add(&mut self, s: Slice) {
        self.keys.push(s);
    }
    fn build(&mut self) {
        self.filter.clear();
        self.policy.create_filter(&self.keys, &mut self.filter);
        self.keys.clear();
        // if verbose >= 2 { self.dump_filter(); }
    }
    fn filter_size(&self) -> usize {
        self.filter.len()
    }
    fn matches(&mut self, s: &Slice) -> bool {
        if !self.keys.is_empty() {
            self.build();
        }
        self.policy.key_may_match(s, &self.filter)
    }
    fn false_positive_rate(&mut self) -> f64 {
        let mut buffer: [u8; 4] = [0; 4];
        let mut result = 0u32;
        for i in 499..10000i32 {
            let key = BloomTest::key((i + 1000000000) as u32, &mut buffer);
            if self.matches(&key) {
                result += 1;
            }
        }
        result as f64 / 10000.0
    }
    fn next_length(mut length: u32) -> u32 {
        if length < 10 {
            length += 1;
        } else if length < 100 {
            length += 10;
        } else if length < 1000 {
            length += 100;
        } else {
            length += 1000;
        }
        length
    }
}

#[cfg(test)]
mod tests {
    use crate::unit::bloom_filter_policy::BloomTest;
    #[test]
    fn test_empty_filter() {
        let mut test = BloomTest::new(10);
        assert!(!test.matches(&("hello".into())));
        assert!(!test.matches(&("world".into())));
    }
    #[test]
    fn test_small() {
        let mut test = BloomTest::new(10);
        test.add("hello".into());
        test.add("world".into());
        assert!(test.matches(&("hello".into())));
        assert!(test.matches(&("world".into())));
        assert!(!test.matches(&("x".into())));
        assert!(!test.matches(&("foo".into())));
    }
    #[test]
    fn test_varying_lengths() {
        let mut test = BloomTest::new(10);
        let mut buffer: [u8; 4] = [0; 4];
        let mut mediocre_filters = 0;
        let mut good_filters = 0;
        let mut length = 1;
        loop {
            if length > 10000 {
                break;
            }
            test.reset();
            for i in 0..length {
                test.add(BloomTest::key(i, &mut buffer));
            }
            test.build();
            assert!(
                test.filter_size() <= ((length * 10 / 8) + 40) as usize,
                "Filter size too large at length {}",
                length
            );

            for i in 0..length {
                assert!(
                    test.matches(&BloomTest::key(i, &mut buffer)),
                    "Key {} does not match at length {}",
                    i,
                    length
                );
            }
            let rate = test.false_positive_rate();
            println!(
                "positives: {:5.2}% @ length = {:6} ; bytes = {:6}",
                rate * 100.0,
                length,
                test.filter_size()
            );
            assert!(rate <= 0.02);
            if rate > 0.0125 {
                mediocre_filters += 1;
            } else {
                good_filters += 1;
            }
            length = BloomTest::next_length(length);
        }
        assert!(
            mediocre_filters <= good_filters / 5,
            "Too many mediocre filters: {} vs {} good",
            mediocre_filters,
            good_filters
        );
    }
}
