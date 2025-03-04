use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use lru::LruCache;
use crate::obj::slice::Slice;
use crate::util::hash;

struct LRUCache<T>{
    cache:LruCache<Slice,T>
}
impl<T> LRUCache<T>{
    fn new(capacity:NonZeroUsize)->Self{
        LRUCache{
            cache:LruCache::new(capacity)
        }
    }
    fn get(&mut self,key:&Slice)->Option<&T>{
        self.cache.get(key)
    }
    fn put(&mut self,key:Slice,value:T){
        self.cache.put(key,value);
    }
    fn erase(&mut self,key:&Slice) {
        self.cache.pop(key);
    }
}
const K_NUM_SHARD_BITS: usize = 4;
const K_NUM_SHARDS: usize = 1 << K_NUM_SHARD_BITS;
struct ShardedLRUCache<T> {
    shared:[LRUCache<T>; K_NUM_SHARDS],
    last_id_: AtomicU64
}

impl<T> ShardedLRUCache<T> {
    fn new(capacity: NonZeroUsize) -> Self {
        let per_shard = (usize::from(capacity) + (K_NUM_SHARDS - 1)) / K_NUM_SHARDS;
        ShardedLRUCache {
            shared: std::array::from_fn(|_| LRUCache::new(NonZeroUsize::try_from(per_shard).unwrap())),
            last_id_: AtomicU64::new(0)
        }
    }
    fn shard(hash:u32) -> usize {
        (hash>> (32 - K_NUM_SHARD_BITS)) as usize
    }
    fn hash_slice(s:&Slice) -> u32 {
        hash(s.data(), 0)
    }
    fn insert(&mut self, key: &Slice, value: T) {
        let hash = Self::hash_slice(key);
        self.shared[Self::shard(hash)].put(key.clone(), value)
    }
    fn new_id(&self) -> u64 {
        self.last_id_.fetch_add(1, Ordering::SeqCst);
        self.last_id_.load(Ordering::SeqCst)
    }
    fn get(&mut self, key: &Slice) ->Option<&T> {
        let hash = Self::hash_slice(key);
        self.shared[Self::shard(hash)].get(key)
    }
    fn erase(&mut self, key: &Slice) {
        let hash = Self::hash_slice(key);
        self.shared[Self::shard(hash)].erase(key)
    }
}

#[cfg(test)]
const K_CACHE_SIZE:usize = 1000;
#[cfg(test)]
struct CacheTest<T> {
    cache:ShardedLRUCache<T>
}
#[cfg(test)]
impl CacheTest<i32> {
    fn new() -> Self {
        CacheTest {
            cache: ShardedLRUCache::new(NonZeroUsize::new(K_CACHE_SIZE).unwrap()),
        }
    }
    fn encode_key(i:i32) -> Slice{
        let mut buffer: [u8; 4] = [0; 4];
        buffer[0] = i as u8;
        buffer[1] = (i >> 8) as u8;
        buffer[2] = (i >> 16) as u8;
        buffer[3] = (i >> 24) as u8;
        Slice::new_from_array(&buffer)
    }
    fn decode_key(s:&Slice) -> i32 {
        let mut buffer: [u8; 4] = [0; 4];
        buffer[0] = s[0];
        buffer[1] = s[1];
        buffer[2] = s[2];
        buffer[3] = s[3];
        i32::from_be_bytes(buffer)
    }
    fn lookup(&mut self, key: i32) -> &i32 {
        let en_key = Self::encode_key(key);
       if let Some(value) = self.cache.get(&en_key) {
           value
       }else {
           &-1
       }
    }
    fn insert(&mut self, key:i32, value:i32) {
        self.cache.insert(&CacheTest::encode_key(key), value)
    }
    fn erase(&mut self, key: i32) {
        self.cache.erase(&CacheTest::encode_key(key))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_hit_and_miss() {
        let mut test = CacheTest::<i32>::new();
        assert_eq!(-1, *test.lookup(100));
        test.insert(100, 101);
        assert_eq!(101, *test.lookup(100));
        assert_eq!(-1, *test.lookup(200));
        assert_eq!(-1, *test.lookup(300));
        test.insert(200, 201);
        assert_eq!(101, *test.lookup(100));
        assert_eq!(201, *test.lookup(200));
        assert_eq!(-1, *test.lookup(300));
        test.insert(100, 102);
        assert_eq!(102, *test.lookup(100));
        assert_eq!(201, *test.lookup(200));
        assert_eq!(-1, *test.lookup(300));
    }
    #[test]
    fn test_erase() {
        let mut test = CacheTest::<i32>::new();
        test.erase(200);
        test.insert(100, 101);
        test.insert(200, 201);
        test.erase(100);
        assert_eq!(-1, *test.lookup(100));
        assert_eq!(201, *test.lookup(200));
        test.erase(100);
        assert_eq!(-1, *test.lookup(100));
        assert_eq!(201, *test.lookup(200));
    }
    #[test]
    fn test_eviction_policy() {
        let mut test = CacheTest::<i32>::new();
        test.insert(100, 101);
        test.insert(200, 201);
        test.insert(300, 301);
        for i in 0..K_CACHE_SIZE+100 {
           test.insert((1000 + i) as i32, (i + 2000) as i32);
            assert_eq!((i + 2000) as i32, *test.lookup((1000 + i) as i32));
            assert_eq!(101, *test.lookup(100));
        }
        assert_eq!(101, *test.lookup(100));
        assert_eq!(-1, *test.lookup(200));
        assert_eq!(-1, *test.lookup(300));
    }
}