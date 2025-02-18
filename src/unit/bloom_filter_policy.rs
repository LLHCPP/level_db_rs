use crate::obj::slice::Slice;
use crate::unit::filter_policy::FilterPolicy;
use crate::unit::hash;

struct BloomFilterPolicy {
    bits_per_key: usize,
    k: u8,
}
fn bloom_hash(key :&Slice) -> u32 {
    return hash(key.data(), 0xbc9f1d34);
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

    fn key_may_match(&self, key: &Slice, bloom_filter: &Vec<u8>)->bool {
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

fn key(i:u32, mut buffer: Box<[u8]>) -> Slice{
    buffer[0] = i as u8;
    buffer[1] = (i>>8) as u8;
    buffer[2] = (i>>16) as u8;
    buffer[3] = (i>>24) as u8;
    Slice::new(*Box::new((buffer)))
}



#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use crate::obj::slice::Slice;
    use crate::unit::bloom_filter_policy::{key, BloomFilterPolicy};
    use crate::unit::filter_policy::FilterPolicy;

    #[test]
    fn test_filter_policy() {
        let policy = &BloomFilterPolicy::new(10);
        let name = policy.name();
        let keys =  RefCell::new(vec![]);
        let filter = RefCell::new(Vec::new());
        let reset = || {
            keys.borrow_mut().clear();
            filter.borrow_mut().clear();
        };
        let add = |key: &Slice| {
            keys.borrow_mut().push(key.to_string());
        };
        let build = || {
            let mut key_slices = vec![];
            for key in keys.borrow().iter() {
                key_slices.push(Slice::new_from_string(key.clone()));
            }
            filter.borrow_mut().clear();
            policy.create_filter(&key_slices, key_slices.len(), &mut filter.borrow_mut());
            keys.borrow_mut().clear();
        };

        let matches = |s:&Slice| {
            if (!keys.borrow().is_empty()) {
                build();
            }
            policy.key_may_match(s, &filter.borrow())
        };
        let FalsePositiveRate = || {
            let mut buffer:[u8;4] = [0;4];
            let mut result = 0u32;
            for i in 0..10000 {
                let key = key(i, Box::new(buffer));
                if matches(&key) {
                    result += 1;
                }
            }
            return result as f64 /10000.0;
        };
        assert_eq!(true, !matches(&Slice::new_from_string("hello".parse().unwrap())))

    }
}