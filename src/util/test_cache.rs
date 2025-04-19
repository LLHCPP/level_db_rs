use ahash::AHashMap;
use std::num::NonZeroUsize;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

trait Cache<T, S> {
    fn new(capacity: NonZeroUsize) -> Self;
    fn get(&mut self, key: &T) -> Option<&T>;
    fn put(&mut self, key: T, value: T);
    fn erase(&mut self, key: &T);
    fn release(&mut self, key: &T);
}
// Node in either in-use or LRU doubly-linked list
struct Node {
    key: String,
    value: String,
    prev: *mut Node,
    next: *mut Node,
    ref_count: u64,
}

impl Node {
    fn new(key: String, value: String, in_use: bool) -> *mut Node {
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
pub struct LRUCacheInner {
    capacity: usize,
    map: AHashMap<String, *mut Node>,
    in_use_head: *mut Node, // Dummy head for in-use list
    in_use_tail: *mut Node, // Dummy tail for in-use list
    lru_head: *mut Node,    // Dummy head for LRU list
    lru_tail: *mut Node,    // Dummy tail for LRU list
}

impl LRUCacheInner {
    // Unlink node from its current list
    unsafe fn unlink_node(&self, node: *mut Node) {
        (*(*node).prev).next = (*node).next;
        (*(*node).next).prev = (*node).prev;
        (*node).prev = ptr::null_mut();
        (*node).next = ptr::null_mut();
    }

    // Add node to in-use list
    unsafe fn add_to_in_use(&mut self, node: *mut Node) {
        (*node).next = (*self.in_use_head).next;
        (*node).prev = self.in_use_head;
        (*(*self.in_use_head).next).prev = node;
        (*self.in_use_head).next = node;
    }

    // Add node to LRU list
    unsafe fn add_to_lru(&mut self, node: *mut Node) {
        (*node).next = (*self.lru_head).next;
        (*node).prev = self.lru_head;
        (*(*self.lru_head).next).prev = node;
        (*self.lru_head).next = node;
    }

    // Move node between lists or within the same list
    unsafe fn move_node(&mut self, node: *mut Node, to_in_use: bool) {
        self.unlink_node(node);
        if to_in_use {
            self.add_to_in_use(node);
        } else {
            self.add_to_lru(node);
        }
    }

    // Remove and deallocate node (only from LRU list)
    unsafe fn remove_node(&mut self, node: *mut Node) {
        if (*node).ref_count > 0 {
            panic!("Cannot remove node from in-use list");
        }
        self.unlink_node(node);
        let _ = Box::from_raw(node); // Deallocate
    }
}

pub struct LRUCache {
    inner: Mutex<LRUCacheInner>,
}

#[derive(PartialEq, Debug)]
struct LruRes {
    node: *mut Node,
    lru: *mut LRUCache,
}

impl LRUCache {
    pub fn new(capacity: usize) -> Self {
        let in_use_head = Node::new(String::new(), String::new(), true);
        let in_use_tail = Node::new(String::new(), String::new(), true);
        let lru_head = Node::new(String::new(), String::new(), false);
        let lru_tail = Node::new(String::new(), String::new(), false);

        unsafe {
            (*in_use_head).next = in_use_tail;
            (*in_use_tail).prev = in_use_head;
            (*lru_head).next = lru_tail;
            (*lru_tail).prev = lru_head;
        }
        LRUCache {
            inner: Mutex::new(LRUCacheInner {
                capacity,
                map: AHashMap::new(),
                in_use_head,
                in_use_tail,
                lru_head,
                lru_tail,
            }),
        }
    }
    pub fn get(&mut self, key: &str) -> Option<LruRes> {
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
    pub fn put(&mut self, key: String, value: String) -> Option<LruRes> {
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
        let node = Node::new(key.clone(), value, true);
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
    pub fn release(&mut self, key: &str) {
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
}

impl Drop for LRUCache {
    fn drop(&mut self) {
        let mut cache = self.inner.lock().unwrap();

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

impl Drop for LruRes {
    fn drop(&mut self) {
        let key = unsafe { &(*(*self.node).key) };
        unsafe {
            (*(self.lru)).release(key);
        }
    }
}

impl LruRes {
    fn value(&self) -> &str {
        unsafe { &((*(self.node)).value) }
    }
}

const K_NUM_SHARD_BITS: usize = 4;
const K_NUM_SHARDS: usize = 1 << K_NUM_SHARD_BITS;
/*struct ShardedLRUCache<T> {
    shared: [LRUCache<T>; K_NUM_SHARDS],
    last_id_: AtomicU64,
}

impl<T> ShardedLRUCache<T> {
    fn new(capacity: NonZeroUsize) -> Self {
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
    fn hash_slice(s: &Slice) -> u32 {
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
    fn get(&mut self, key: &Slice) -> Option<&T> {
        let hash = Self::hash_slice(key);
        self.shared[Self::shard(hash)].get(key)
    }
    fn erase(&mut self, key: &Slice) {
        let hash = Self::hash_slice(key);
        self.shared[Self::shard(hash)].erase(key)
    }
}*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache() {
        let mut cache = LRUCache::new(2);

        // Add to in-use
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());
        assert_eq!(cache.get("key1").unwrap().value(), "value1");
        assert_eq!(cache.get("key2").unwrap().value(), "value2");
        assert_eq!(cache.get("key1").unwrap().value(), "value1");
        assert_eq!(cache.get("key2").unwrap().value(), "value2"); // Moves back to in-use
        cache.put("key3".to_string(), "value3".to_string());
        assert_eq!(cache.get("key1"), None);
        assert_eq!(cache.get("key2").unwrap().value(), "value2");
        assert_eq!(cache.get("key3").unwrap().value(), "value3");
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
