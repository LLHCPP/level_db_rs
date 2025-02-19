use bytes::{Buf, Bytes};
use std::cmp::Ordering;
use std::ops::Index;

#[derive(Debug, Clone)]
pub struct Slice {
    pub(crate) data_bytes:Bytes,
}
impl Slice {
    // 构造函数
    pub(crate) fn new(data: Bytes) -> Self {
        Slice { data_bytes: data }
    }

    pub fn data(&self) -> &[u8] {
        &*self.data_bytes
    }
    pub(crate) fn new_from_vec(data: Vec<u8>) -> Self {
        Slice { data_bytes:  Bytes::from(data) }
    }
    pub(crate) fn new_from_array(data: &[u8]) -> Self {
        Slice { data_bytes:  Bytes::copy_from_slice(data) }
    }
    pub(crate) fn new_from_string(data: String) -> Self {
        Slice { data_bytes:  Bytes::from(data.into_bytes()) }
    }
    pub(crate) fn new_from_static(data: &'static str) -> Self {
        Slice { data_bytes:  Bytes::from(data) }
    }
    pub(crate) fn new_from_str(data: &str) -> Self {
        Slice { data_bytes:  Bytes::copy_from_slice(data.as_bytes()) }
    }
    pub fn size(&self) -> usize {
        self.len()
    }
    // 获取引用的长度
    pub fn len(&self) -> usize {
        self.data_bytes.len()
    }
    fn remove_prefix(&mut self, n:usize ){
        if n > self.len() {
            panic!("remove_prefix: n is out of range")
        }
        self.data_bytes.advance(n);
    }
    pub fn to_string(&self) -> String {
        String::from_utf8_lossy(&*self.data_bytes).to_string()
    }

    fn compare(&self, x: &Slice) -> Ordering {
        self.data_bytes.cmp(&x.data_bytes)
    }

    fn starts_with(&self, x: &Slice) -> bool {
        self.data_bytes.len() >= x.data_bytes.len() &&
               self.data_bytes[..x.data_bytes.len()] == x.data_bytes[..]
    }

    // 打印内容
    fn print(&self) {
        println!("Slice data: {:?}", self.data_bytes);
    }
}
impl Index<usize> for Slice {
    type Output = u8;
    fn index(&self, index: usize) -> &Self::Output {
        &self.data_bytes[index]
    }
}

impl PartialEq for Slice {
    fn eq(&self, other: &Self) -> bool {
        self.data_bytes == other.data_bytes
    }
}

impl From<&str> for Slice {
    fn from(s: &str) -> Self {
        Slice::new_from_str(s)
    }
}