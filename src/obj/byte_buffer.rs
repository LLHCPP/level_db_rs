use std::cmp::min;
use std::ops::{Index, IndexMut};
use std::slice;

struct ByteBuffer {
    ptr: *mut u8, // 指向字节数组的指针
    len: usize,
    cap: usize,
    is_owner: bool,
}


impl ByteBuffer {
    pub fn new(capacity: usize) -> Self {
        let ptr = unsafe { libc::malloc(capacity) as *mut u8 };
        ByteBuffer {
            ptr,
            len: 0,
            cap: capacity,
            is_owner: true,
        }
    }
    pub fn from_slice(slice: &[u8]) -> Self {
        let ptr = slice.as_ptr() as *mut u8;
        ByteBuffer {
            ptr,
            len: slice.len(),
            cap: slice.len(),
            is_owner: false,
        }
    }

    pub(crate) fn from_vec(vec: &Vec<u8>) -> Self {
        let ptr = vec.as_ptr() as *mut u8;
        ByteBuffer {
            ptr,
            len: vec.len(),
            cap: vec.len(),
            is_owner: false,
        }
    }
    pub fn resize(&mut self, len: usize) {
        assert!(len <= self.cap);
        self.len = len;
    }
    
    pub fn init(&mut self, start:usize, end:usize) {
        for i in start..min(end, self.len)  {
            unsafe { self.ptr.add(i).write(0); }
        }
    }
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn is_owner(&self) -> bool {
        self.is_owner
    }
    pub(crate) fn is_empty(&self) -> bool {
       self.len ==0
    }
}

impl Clone for ByteBuffer {
    fn clone(&self) -> Self {
        ByteBuffer {
            ptr:self.ptr,
            len: self.len,
            cap: self.cap,
            is_owner: false,
        }
    }
 }
// 实现 Index trait 以支持只读索引
impl Index<usize> for ByteBuffer {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.len {
            panic!("index out of bounds: the len is {} but the index is {}", self.len, index);
        }
        unsafe { &*self.ptr.add(index) }
    }
}

// 实现 IndexMut trait 以支持可写索引
impl IndexMut<usize> for ByteBuffer {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if index >= self.len {
            panic!("index out of bounds: the len is {} but the index is {}", self.len, index);
        }
        unsafe { &mut *self.ptr.add(index) }
    }
}

impl Drop for ByteBuffer {
    fn drop(&mut self) {
        if self.is_owner {
            unsafe {
                libc::free(self.ptr as *mut libc::c_void);
            }
        }
    }
}

unsafe impl Send for ByteBuffer {}
unsafe impl Sync for ByteBuffer {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_buffer_new() {
        let capacity = 16;
        let mut buffer = ByteBuffer::new(capacity);
        assert_eq!(buffer.cap(), capacity);
        buffer.resize(capacity);
        assert_eq!(buffer.len(), capacity);
        assert!(!buffer.is_empty());
        buffer.init(0, buffer.len);
        assert_eq!(buffer.as_slice(), &[0; 16]);
    }

    #[test]
    fn test_byte_buffer_from_slice() {
        let data = b"Hello, Rust!";
        let buffer = ByteBuffer::from_slice(data);
        assert_eq!(buffer.as_slice(), data);
        assert_eq!(buffer.len(), data.len());
    }

    #[test]
    fn test_byte_buffer_from_vec() {
        let data:Vec<u8> = vec![1, 2, 3, 4, 5];
        let buffer = ByteBuffer::from_vec(&data);
        let buffer_slice: &[u8] = buffer.as_slice();
        let data_slice: &[u8] = data.as_slice();
        assert_eq!(buffer_slice, data_slice);
        assert_eq!(buffer.len(), data.len());
    }

    #[test]
    fn test_empty_buffer() {
        let buffer = ByteBuffer::new(0);
        assert_eq!(buffer.as_slice(), &[]);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_index() {
        let mut buffer = ByteBuffer::new(4);
        buffer.resize(4);
        // 测试写操作
        buffer[0] = 42;
        buffer[1] = 255;
        // 测试读操作
        assert_eq!(buffer[0], 42);
        assert_eq!(buffer[1], 255);
    }

    #[test]
    #[should_panic]
    fn test_index_out_of_bounds() {
        let buffer = ByteBuffer::new(4);
        let _ = buffer[4]; // 应触发 panic
    }

    #[test]
    fn test_mutable_slice() {
        let mut buffer = ByteBuffer::new(4);
        buffer.resize(4);
        let mut slice = buffer.as_mut_slice();
        slice[0] = 42;
        assert_eq!(buffer.as_slice()[0], 42);
    }
}