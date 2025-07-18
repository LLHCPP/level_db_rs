use std::alloc::{alloc, dealloc, Layout};
use std::cmp::min;
use std::ops::{Index, IndexMut, Range, RangeFrom};
use std::slice;

#[derive(Debug)]
pub(crate) struct ByteBuffer {
    ptr: *mut u8, // 指向字节数组的指针
    offset: usize,
    len: usize,
    cap: usize,
    is_owner: bool,
    layout: Option<Layout>,
}

impl ByteBuffer {
    pub fn new(capacity: usize) -> Self {
        let layout = Layout::from_size_align(capacity, align_of::<u8>()).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            panic!("Memory allocation failed");
        }
        ByteBuffer {
            ptr,
            offset: 0,
            len: 0,
            cap: capacity,
            is_owner: true,
            layout: Some(layout),
        }
    }
    pub fn from_slice(slice: &[u8]) -> Self {
        let ptr = slice.as_ptr() as *mut u8;
        ByteBuffer {
            ptr,
            offset: 0,
            len: slice.len(),
            cap: slice.len(),
            is_owner: false,
            layout: None,
        }
    }

    pub fn from_string(str: &String) -> Self {
        let ptr = str.as_ptr() as *mut u8;
        ByteBuffer {
            ptr,
            offset: 0,
            len: str.len(),
            cap: str.len(),
            is_owner: false,
            layout: None,
        }
    }

    pub fn from_ptr(data: &[u8]) -> Self {
        let ptr = data.as_ptr() as *mut u8;
        ByteBuffer {
            ptr,
            offset: 0,
            len: data.len(),
            cap: data.len(),
            is_owner: false,
            layout: None,
        }
    }

    pub(crate) fn from_vec(vec: &Vec<u8>) -> Self {
        let ptr = vec.as_ptr() as *mut u8;
        ByteBuffer {
            ptr,
            offset: 0,
            len: vec.len(),
            cap: vec.len(),
            is_owner: false,
            layout: None,
        }
    }
    pub fn resize(&mut self, len: usize) {
        assert!(len <= self.cap());
        self.len = len;
    }

    pub fn init(&mut self, start: usize, end: usize) {
        for i in start..min(end, self.len) {
            unsafe {
                self.ptr.add(i + self.offset).write(0);
            }
        }
    }
    pub fn as_slice(&self) -> &[u8] {
        unsafe { &slice::from_raw_parts(self.ptr, self.len + self.offset)[self.offset..] }
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { &mut slice::from_raw_parts_mut(self.ptr, self.len + self.offset)[self.offset..] }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn cap(&self) -> usize {
        self.cap - self.offset
    }

    pub fn is_owner(&self) -> bool {
        self.is_owner
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn advance(&mut self, n: usize) -> &Self {
        debug_assert!(n <= self.len);
        self.offset += n;
        self.len -= n;
        self
    }

    pub fn clear(&mut self) {
        if self.is_owner {
            unsafe {
                dealloc(self.ptr, self.layout.unwrap());
            }
        }
        self.ptr = std::ptr::null_mut();
        self.offset = 0;
        self.len = 0;
        self.cap = 0;
        self.is_owner = false;
    }
}

impl Clone for ByteBuffer {
    fn clone(&self) -> Self {
        ByteBuffer {
            ptr: self.ptr,
            offset: self.offset,
            len: self.len,
            cap: self.cap,
            is_owner: false,
            layout: self.layout.clone(),
        }
    }
}
// 实现 Index trait 以支持只读索引
impl Index<usize> for ByteBuffer {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.len {
            panic!(
                "index out of bounds: the len is {} but the index is {}",
                self.len, index
            );
        }
        unsafe { &*self.ptr.add(index + self.offset) }
    }
}

// 实现 IndexMut trait 以支持可写索引
impl IndexMut<usize> for ByteBuffer {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if index >= self.len {
            panic!(
                "index out of bounds: the len is {} but the index is {}",
                self.len, index
            );
        }
        unsafe { &mut *self.ptr.add(index + self.offset) }
    }
}

impl Index<Range<usize>> for ByteBuffer {
    type Output = [u8];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        assert!(range.start < range.end);
        assert!(range.end <= self.len);
        unsafe {
            &slice::from_raw_parts(
                self.ptr.add(range.start),
                range.end - range.start + self.offset,
            )[self.offset..]
        }
    }
}

// 实现 IndexMut trait 以支持范围可写索引
impl IndexMut<Range<usize>> for ByteBuffer {
    fn index_mut(&mut self, range: Range<usize>) -> &mut Self::Output {
        assert!(range.start < range.end);
        assert!(range.end <= self.len);
        unsafe {
            &mut slice::from_raw_parts_mut(
                self.ptr.add(range.start),
                range.end - range.start + self.offset,
            )[self.offset..]
        }
    }
}

impl Index<RangeFrom<usize>> for ByteBuffer {
    type Output = [u8];

    fn index(&self, range: RangeFrom<usize>) -> &Self::Output {
        assert!(range.start < self.len);
        unsafe {
            slice::from_raw_parts(
                self.ptr.add(range.start + self.offset),
                self.len() - range.start,
            )
        }
    }
}

// 实现 IndexMut trait 以支持范围可写索引
impl IndexMut<RangeFrom<usize>> for ByteBuffer {
    fn index_mut(&mut self, range: RangeFrom<usize>) -> &mut Self::Output {
        assert!(range.start < self.len);
        unsafe {
            slice::from_raw_parts_mut(
                self.ptr.add(range.start + self.offset),
                self.len() - range.start,
            )
        }
    }
}

impl Drop for ByteBuffer {
    fn drop(&mut self) {
        if self.is_owner {
            unsafe {
                dealloc(self.ptr, self.layout.unwrap());
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
        let data: Vec<u8> = vec![1, 2, 3, 4, 5];
        let buffer = ByteBuffer::from_vec(&data);
        let buffer_slice: &[u8] = buffer.as_slice();
        let data_slice: &[u8] = data.as_slice();
        assert_eq!(buffer_slice, data_slice);
        assert_eq!(buffer.len(), data.len());
    }

    #[test]
    fn test_empty_buffer() {
        let buffer = ByteBuffer::new(0);
        assert_eq!(buffer.as_slice(), &[0; 0]);
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
        let slice = buffer.as_mut_slice();
        slice[0] = 42;
        assert_eq!(buffer.as_slice()[0], 42);
    }
}
