use bytes::{Buf, Bytes, BytesMut};
use std::cmp::{min, Ordering};
use std::ops::{Index, Range};

#[derive(Debug, Clone, Hash)]
enum SliceData {
    Bytes(Bytes),       // 包含 Bytes 数据
    BytesMut(BytesMut), // 包含 BytesMut 数据
}

impl SliceData {
    pub(crate) fn slice(&self, range: Range<usize>) -> SliceData {
        let (start, end) = match range {
            Range { start, end } => (start, end),
        };
        match self {
            SliceData::Bytes(b) => SliceData::Bytes(b.slice(start..end)),
            SliceData::BytesMut(bm) => SliceData::BytesMut(BytesMut::from(&bm[start..end])),
        }
    }
}

impl PartialEq for SliceData {
    fn eq(&self, other: &Self) -> bool {
        // 获取两个 SliceData 的字节切片表示
        let self_bytes = match self {
            SliceData::Bytes(b) => b.as_ref(),      // Bytes 实现 AsRef<[u8]>
            SliceData::BytesMut(bm) => bm.as_ref(), // BytesMut 实现 AsRef<[u8]>
        };
        let other_bytes = match other {
            SliceData::Bytes(b) => b.as_ref(),
            SliceData::BytesMut(bm) => bm.as_ref(),
        };
        // 逐字节比较
        self_bytes == other_bytes
    }
}

impl Eq for SliceData {}
impl PartialOrd for SliceData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_bytes = match self {
            SliceData::Bytes(b) => b.as_ref(),
            SliceData::BytesMut(bm) => bm.as_ref(),
        };
        let other_bytes = match other {
            SliceData::Bytes(b) => b.as_ref(),
            SliceData::BytesMut(bm) => bm.as_ref(),
        };
        Some(self_bytes.cmp(other_bytes)) // &[u8] 已实现 Ord，直接调用 cmp
    }
}
// 实现 Ord，提供 cmp 方法
impl Ord for SliceData {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_bytes = match self {
            SliceData::Bytes(b) => b.as_ref(),
            SliceData::BytesMut(bm) => bm.as_ref(),
        };
        let other_bytes = match other {
            SliceData::Bytes(b) => b.as_ref(),
            SliceData::BytesMut(bm) => bm.as_ref(),
        };
        self_bytes.cmp(other_bytes) // &[u8] 的逐字节比较
    }
}

#[derive(Debug, Clone, Eq, Hash)]
pub struct Slice {
    pub(crate) data_bytes: Bytes,
}

impl Slice {
    pub(crate) fn new_from_slice(p0: &Slice, p1: Range<usize>) -> Slice {
        Slice {
            data_bytes: p0.data_bytes.slice(p1),
        }
    }
    // 构造函数
    pub(crate) fn new(data: Bytes) -> Self {
        Slice { data_bytes: data }
    }
    pub(crate) fn new_from_mut(data: &BytesMut) -> Self {
        Slice {
            data_bytes: Bytes::copy_from_slice(&data[..]),
        }
    }

    pub fn data(&self) -> &[u8] {
        &*self.data_bytes
    }
    pub(crate) fn new_from_vec(data: Vec<u8>) -> Self {
        Slice {
            data_bytes: Bytes::from(data),
        }
    }
    pub(crate) fn new_from_array(data: &[u8]) -> Self {
        Slice {
            data_bytes: Bytes::copy_from_slice(data),
        }
    }
    pub(crate) fn new_from_string(data: String) -> Self {
        Slice {
            data_bytes: Bytes::from(data.into_bytes()),
        }
    }
    pub(crate) fn new_from_static(data: &'static str) -> Self {
        Slice {
            data_bytes: Bytes::from(data),
        }
    }
    pub(crate) fn new_from_str(data: &str) -> Self {
        Slice {
            data_bytes: Bytes::copy_from_slice(data.as_bytes()),
        }
    }
    pub fn size(&self) -> usize {
        self.len()
    }
    // 获取引用的长度
    pub fn len(&self) -> usize {
        self.data_bytes.len()
    }
    pub(crate) fn remove_prefix(&mut self, n: usize) {
        if n > self.len() {
            panic!("remove_prefix: n is out of range")
        }
        self.data_bytes.advance(n);
    }
    pub fn to_string(&self) -> String {
        String::from_utf8_lossy(&*self.data_bytes).to_string()
    }

    pub(crate) fn compare(&self, x: &Slice) -> Ordering {
        self.data_bytes.cmp(&x.data_bytes)
    }

    fn starts_with(&self, x: &Slice) -> bool {
        self.data_bytes.len() >= x.data_bytes.len()
            && self.data_bytes[..x.data_bytes.len()] == x.data_bytes[..]
    }
    pub fn slice(&self, n: usize) -> Slice {
        let data_bytes = self.data_bytes.slice(..min(n, self.len()));
        Slice { data_bytes }
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

impl From<&'static str> for Slice {
    fn from(s: &'static str) -> Self {
        Slice::new_from_str(s)
    }
}

impl From<&BytesMut> for Slice {
    fn from(s: &BytesMut) -> Self {
        Slice {
            data_bytes: Bytes::copy_from_slice(&s[..]),
        }
    }
}
