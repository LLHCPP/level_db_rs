use crate::util::hash::LocalHash;
use ahash::{AHashMap, AHasher};
use std::borrow::Borrow;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

trait Cache<T, S> {
    fn new(capacity: NonZeroUsize) -> Self;
    fn get(&mut self, key: &T) -> Option<&T>;
    fn put(&mut self, key: T, value: T);
    fn erase(&mut self, key: &T);
    fn release(&mut self, key: &T);
}
// Node in either in-use or LRU doubly-linked list
struct Node<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Default + Clone,
{
    key: K,
    value: V,
    prev: *mut Node<K, V>,
    next: *mut Node<K, V>,
    ref_count: u64,
}

impl<K, V> Node<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Default + Clone,
{
    fn new(key: K, value: V) -> *mut Node<K, V> {
        let node = Box::new(Node {
            key,
            value,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            ref_count: 0,
        });
        Box::into_raw(node)
    }
}
pub struct LRUCacheInner<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Default + Clone,
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
    V: Default + Clone,
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

pub struct LRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Default + Clone,
{
    inner: Mutex<LRUCacheInner<K, V>>,
}

#[derive(PartialEq, Debug)]
pub struct LruRes<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Default + Clone,
{
    node: *mut Node<K, V>,
    lru: *mut LRUCache<K, V>,
}

impl<K, V> LRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Default + Clone,
{
    pub fn new(capacity: NonZeroUsize) -> Self {
        let in_use_head = Node::new(K::default(), V::default());
        let in_use_tail = Node::new(K::default(), V::default());
        let lru_head = Node::new(K::default(), V::default());
        let lru_tail = Node::new(K::default(), V::default());

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
                (*node).value = value;
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
    V: Default + Clone,
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
    V: Default + Clone,
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
    V: Default + Clone,
{
    fn value(&self) -> &V {
        unsafe { &((*(self.node)).value) }
    }
}

impl<K, V> Deref for LruRes<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Default + Clone,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

const K_NUM_SHARD_BITS: usize = 4;
const K_NUM_SHARDS: usize = 1 << K_NUM_SHARD_BITS;
struct ShardedLRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone,
    V: Default + Clone,
{
    shared: [LRUCache<K, V>; K_NUM_SHARDS],
    last_id_: AtomicU64,
    hasher: AHasher,
}

impl<K, V> ShardedLRUCache<K, V>
where
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
{
    fn new(capacity: NonZeroUsize) -> Self {
        let per_shard = (usize::from(capacity) + (K_NUM_SHARDS - 1)) / K_NUM_SHARDS;
        ShardedLRUCache {
            shared: std::array::from_fn(|_| {
                LRUCache::new(NonZeroUsize::try_from(per_shard).unwrap())
            }),
            last_id_: AtomicU64::new(0),
            hasher: Default::default(),
        }
    }
    fn shard(hash: u32) -> usize {
        (hash >> (32 - K_NUM_SHARD_BITS)) as usize
    }
    fn insert(&mut self, key: &K, value: V) -> Option<LruRes<K, V>> {
        let hash = key.local_hash();
        self.shared[Self::shard(hash)].put(key.clone(), value)
    }
    fn new_id(&self) -> u64 {
        self.last_id_.fetch_add(1, Ordering::Relaxed);
        self.last_id_.load(Ordering::Relaxed)
    }
    fn get(&mut self, key: &K) -> Option<LruRes<K, V>> {
        let hash = key.local_hash();
        self.shared[Self::shard(hash)].get(key)
    }
    fn erase(&mut self, key: &K) {
        let hash = key.local_hash();
        self.shared[Self::shard(hash)].erase(key)
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
}
