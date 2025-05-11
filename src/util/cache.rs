use crate::obj::slice::Slice;
use crate::util::hash::LocalHash;
use ahash::AHashMap;
use std::borrow::Borrow;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/*trait Cache<T, S> {
    fn new(capacity: NonZeroUsize) -> Self;
    fn get(&mut self, key: &T) -> Option<&T>;
    fn put(&mut self, key: T, value: T);
    fn erase(&mut self, key: &T);
    fn release(&mut self, key: &T);
}*/
// Node in either in-use or LRU doubly-linked list
struct Node<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    key: K,
    value: Option<V>,
    prev: *mut Node<K, V>,
    next: *mut Node<K, V>,
    ref_count: u64,
}

impl<K, V> Node<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V:  Clone,
{
    fn new(key: K, value: V) -> *mut Node<K, V> {
        let node = Box::new(Node {
            key,
            value: Some(value),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            ref_count: 0,
        });
        Box::into_raw(node)
    }

    fn new_empty(key: K) -> *mut Node<K, V> {
        let node = Box::new(Node {
            key,
            value: None,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            ref_count: 0,
        });
        Box::into_raw(node)
    }

}
struct LRUCacheInner<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    capacity: usize,
    map: AHashMap<K, *mut Node<K, V>>,
    in_use_head: *mut Node<K, V>, // Dummy head for in-use list
    in_use_tail: *mut Node<K, V>, // Dummy tail for in-use list
    lru_head: *mut Node<K, V>,    // Dummy head for LRU list
    lru_tail: *mut Node<K, V>,    // Dummy tail for LRU list
}

impl<K, V> LRUCacheInner<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    // Unlink node from its current list
    unsafe fn unlink_node(&self, node: *mut Node<K, V>) {
        (*(*node).prev).next = (*node).next;
        (*(*node).next).prev = (*node).prev;
        (*node).prev = ptr::null_mut();
        (*node).next = ptr::null_mut();
    }

    // Add node to in-use list
    unsafe fn add_to_in_use(&mut self, node: *mut Node<K, V>) {
        (*node).next = (*self.in_use_head).next;
        (*node).prev = self.in_use_head;
        (*(*self.in_use_head).next).prev = node;
        (*self.in_use_head).next = node;
    }

    // Add node to LRU list
    unsafe fn add_to_lru(&mut self, node: *mut Node<K, V>) {
        (*node).next = (*self.lru_head).next;
        (*node).prev = self.lru_head;
        (*(*self.lru_head).next).prev = node;
        (*self.lru_head).next = node;
    }

    // Move node between lists or within the same list
    unsafe fn move_node(&mut self, node: *mut Node<K, V>, to_in_use: bool) {
        self.unlink_node(node);
        if to_in_use {
            self.add_to_in_use(node);
        } else {
            self.add_to_lru(node);
        }
    }

    // Remove and deallocate node (only from LRU list)
    unsafe fn remove_node(&mut self, node: *mut Node<K, V>) {
        if (*node).ref_count > 0 {
            panic!("Cannot remove node from in-use list");
        }
        self.unlink_node(node);
        let _ = Box::from_raw(node); // Deallocate
    }
}

struct LRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V:Clone,
{
    inner: Mutex<LRUCacheInner<K, V>>,
}

#[derive(PartialEq, Debug)]
pub struct LruRes<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    node: *mut Node<K, V>,
    lru: *mut LRUCache<K, V>,
}

impl<K, V> LRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    pub fn new(capacity: NonZeroUsize) -> Self {
        let in_use_head = Node::new_empty(K::default());
        let in_use_tail = Node::new_empty(K::default());
        let lru_head = Node::new_empty(K::default());
        let lru_tail = Node::new_empty(K::default());

        unsafe {
            (*in_use_head).next = in_use_tail;
            (*in_use_tail).prev = in_use_head;
            (*lru_head).next = lru_tail;
            (*lru_tail).prev = lru_head;
        }
        LRUCache {
            inner: Mutex::new(LRUCacheInner {
                capacity: usize::from(capacity),
                map: AHashMap::new(),
                in_use_head,
                in_use_tail,
                lru_head,
                lru_tail,
            }),
        }
    }
    pub fn get<Q>(&mut self, key: &Q) -> Option<LruRes<K, V>>
    where
        Q: ?Sized + Hash + Eq,
        K: Borrow<Q>,
    {
        let mut cache = self.inner.lock().unwrap();
        let node = { cache.map.get(key) }; // lock_guard 在此离开作用域，锁释放
        if let Some(&node) = node {
            unsafe {
                if (*node).ref_count == 0 {
                    cache.move_node(node, true);
                }
                (*node).ref_count += 1;
                drop(cache);
                Some(LruRes {
                    node,
                    lru: self as *mut Self,
                })
            }
        } else {
            None
        }
    }
    pub fn put(&mut self, key: K, value: V) -> Option<LruRes<K, V>> {
        let mut cache = self.inner.lock().unwrap();
        // Check if key exists
        if let Some(&node) = cache.map.get(&key) {
            unsafe {
                (*node).value = Some(value);
                (*node).ref_count += 1;
                cache.move_node(node, true);
            }
            drop(cache);
            return Some(LruRes {
                node,
                lru: self as *mut Self,
            });
        }
        // Create new node
        let node = Node::new(key.clone(), value);
        cache.map.insert(key, node);
        unsafe {
            (*node).ref_count += 1;
            cache.add_to_in_use(node);
            // Evict from LRU if over capacity
            if cache.map.len() > cache.capacity {
                // Get the least recently used node (before lru_tail)
                let lru = (*cache.lru_tail).prev;
                if lru != cache.lru_head {
                    let lru_key = (*lru).key.clone();
                    cache.remove_node(lru);
                    cache.map.remove(&lru_key);
                }
            }
        }
        drop(cache);
        Some(LruRes {
            node,
            lru: self as *mut Self,
        })
    }

    // Move node from in-use to LRU list (simulating release of reference)
    pub fn release(&mut self, key: &K) {
        let mut cache = self.inner.lock().unwrap();
        if let Some(&node) = cache.map.get(key) {
            unsafe {
                (*node).ref_count -= 1;
                if (*node).ref_count == 0 {
                    cache.move_node(node, false);
                }
            }
        }
    }

    /// 删除时，对应value被持有的引用会变成空指针
    pub fn erase(&mut self, key: &K) {
        let mut cache = self.inner.lock().unwrap();
        if let Some(&node) = cache.map.get(&key) {
            unsafe { cache.remove_node(node) }
            cache.map.remove(key);
        }
    }
}

impl<K, V> Drop for LRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    fn drop(&mut self) {
        let cache = self.inner.lock().unwrap();

        unsafe {
            // Clean up in-use list (nodes are not deallocated per requirement)
            let mut current = (*cache.in_use_head).next;
            while current != cache.in_use_tail {
                let next = (*current).next;
                // Do not deallocate in-use nodes
                current = next;
            }
            let _ = Box::from_raw(cache.in_use_head);
            let _ = Box::from_raw(cache.in_use_tail);

            // Clean up LRU list
            let mut current = (*cache.lru_head).next;
            while current != cache.lru_tail {
                let next = (*current).next;
                let _ = Box::from_raw(current);
                current = next;
            }
            let _ = Box::from_raw(cache.lru_head);
            let _ = Box::from_raw(cache.lru_tail);
        }
    }
}

impl<K, V> Drop for LruRes<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    fn drop(&mut self) {
        let key = unsafe { &(*self.node).key };
        unsafe {
            (*(self.lru)).release(key);
        }
    }
}

impl<K, V> LruRes<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    pub fn value(&self) -> &V {
        let data = unsafe { &((*(self.node)).value)};
        match data {
            Some(v) => v,
            None => panic!("value is None"),
        }
    }
}

impl<K, V> Deref for LruRes<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Clone,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

const K_NUM_SHARD_BITS: usize = 4;
const K_NUM_SHARDS: usize = 1 << K_NUM_SHARD_BITS;
pub(crate) struct ShardedLRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
{
    shared: [LRUCache<K, V>; K_NUM_SHARDS],
    last_id_: AtomicU64,
}

impl<K, V> ShardedLRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
{
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        let per_shard = (usize::from(capacity) + (K_NUM_SHARDS - 1)) / K_NUM_SHARDS;
        ShardedLRUCache {
            shared: std::array::from_fn(|_| {
                LRUCache::new(NonZeroUsize::try_from(per_shard).unwrap())
            }),
            last_id_: AtomicU64::new(0),
        }
    }
    fn shard(hash: u32) -> usize {
        (hash >> (32 - K_NUM_SHARD_BITS)) as usize
    }
    pub fn insert(&mut self, key: &K, value: V) -> Option<LruRes<K, V>> {
        let hash = key.local_hash();
        self.shared[Self::shard(hash)].put(key.clone(), value)
    }
    fn new_id(&self) -> u64 {
        self.last_id_.fetch_add(1, Ordering::Relaxed);
        self.last_id_.load(Ordering::Relaxed)
    }

    pub fn get(&mut self, key: &K) -> Option<LruRes<K, V>> {
        let hash = key.local_hash();
        self.shared[Self::shard(hash)].get(key)
    }
    /// 删除时，对应value被持有的引用会变成空指针
   pub fn erase(&mut self, key: &K) {
        let hash = key.local_hash();
        self.shared[Self::shard(hash)].erase(key)
    }
}

#[cfg(test)]
const K_CACHE_SIZE: usize = 1000;
#[cfg(test)]
struct CacheTest<T: Clone + Default> {
    cache: ShardedLRUCache<Slice, T>,
}

#[cfg(test)]
impl CacheTest<i32> {
    fn new() -> Self {
        CacheTest {
            cache: ShardedLRUCache::new(NonZeroUsize::new(K_CACHE_SIZE).unwrap()),
        }
    }
    fn encode_key(i: i32) -> Slice {
        let mut buffer: [u8; 4] = [0; 4];
        buffer[0] = i as u8;
        buffer[1] = (i >> 8) as u8;
        buffer[2] = (i >> 16) as u8;
        buffer[3] = (i >> 24) as u8;
        Slice::new_from_array(&buffer)
    }
    /*    fn decode_key(s: &Slice) -> i32 {
        let mut buffer: [u8; 4] = [0; 4];
        buffer[0] = s[0];
        buffer[1] = s[1];
        buffer[2] = s[2];
        buffer[3] = s[3];
        i32::from_be_bytes(buffer)
    }
    */

    fn lookup(&mut self, key: i32) -> std::sync::Arc<i32> {
        let en_key = Self::encode_key(key);
        if let Some(value) = self.cache.get(&en_key) {
            std::sync::Arc::from(*value)
        } else {
            std::sync::Arc::from(-1)
        }
    }
    fn insert(&mut self, key: i32, value: i32) {
        let _ = self.cache.insert(&CacheTest::encode_key(key), value);
    }
    fn erase(&mut self, key: i32) {
        self.cache.erase(&CacheTest::encode_key(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache() {
        let mut cache = LRUCache::new(NonZeroUsize::new(2).unwrap());

        // Add to in-use
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());
        assert_eq!(cache.get("key1").unwrap().value(), "value1");
        assert_eq!(cache.get("key2").unwrap().value(), "value2");
        assert_eq!(cache.get("key1").unwrap().value(), "value1");
        assert_eq!(cache.get("key2").unwrap().value(), "value2"); // Moves back to in-use
        cache.put("key3".to_string(), "value3".to_string());
        assert_eq!(cache.get("key1"), None);
        assert_eq!(*cache.get("key2").unwrap(), "value2");
        assert_eq!(*cache.get("key3").unwrap(), "value3");
        // Update key2
        cache.put("key2".to_string(), "value2_updated".to_string());
        assert_eq!(cache.get("key2").unwrap().value(), "value2_updated");
        assert_eq!(cache.get("key3").unwrap().value(), "value3");
        cache.put("key4".to_string(), "value4".to_string());
        assert_eq!(cache.get("key2"), None);
        assert_eq!(cache.get("key3").unwrap().value(), "value3");
        assert_eq!(cache.get("key4").unwrap().value(), "value4");
    }
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
        for i in 0..K_CACHE_SIZE + 100 {
            test.insert((1000 + i) as i32, (i + 2000) as i32);
            assert_eq!((i + 2000) as i32, *test.lookup((1000 + i) as i32));
            assert_eq!(101, *test.lookup(100));
        }
        assert_eq!(101, *test.lookup(100));
        assert_eq!(-1, *test.lookup(200));
        assert_eq!(-1, *test.lookup(300));
    }
}
