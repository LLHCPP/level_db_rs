use crate::util::hash;
use bytes::{Buf, Bytes, BytesMut};
use memmap2::Mmap;
use std::cmp::{min, Ordering};
use std::hash::{Hash, Hasher};
use std::ops::{Index, Range, RangeFull, RangeTo};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct MMapSlice {
    mmap: Arc<Mmap>,
    offset: usize,
    len: usize,
}

#[derive(Debug, Clone)]
struct BytesMutSlice {
    byte_mut: Arc<BytesMut>,
    offset: usize,
    len: usize,
}

#[derive(Debug, Clone)]
enum SliceData {
    Bytes(Bytes),            // 包含 Bytes 数据
    BytesMut(BytesMutSlice), // 包含 BytesMut 数据
    MMap(MMapSlice),
}

impl SliceData {
    pub fn from_bytes(bytes: &Bytes) -> SliceData {
        SliceData::Bytes(bytes.clone())
    }

    pub fn new_bytes(bytes: Bytes) -> SliceData {
        SliceData::Bytes(bytes)
    }
    pub fn from_bytes_mut(bytes: &BytesMut) -> SliceData {
        SliceData::Bytes(Bytes::copy_from_slice(&bytes[..]))
    }
    pub fn new_bytes_mut(bytes: BytesMut) -> SliceData {
        SliceData::Bytes(bytes.freeze())
    }

    pub fn from_arc_mmap(mmap: Arc<Mmap>, offset: usize, len: usize) -> SliceData {
        debug_assert!(mmap.len() >= offset + len);
        SliceData::MMap(MMapSlice { len, mmap, offset })
    }

    pub fn from_arc_bytes_mut(bytes: Arc<BytesMut>) -> SliceData {
        SliceData::BytesMut(BytesMutSlice {
            len: bytes.len(),
            byte_mut: bytes,
            offset: 0,
        })
    }

    pub(crate) fn slice(&self, range: Range<usize>) -> SliceData {
        let (start, end) = match range {
            Range { start, end } => (start, end),
        };
        match self {
            SliceData::Bytes(b) => SliceData::Bytes(b.slice(start..end)),
            SliceData::BytesMut(bm) => {
                let new_len = min(end - start, bm.len);
                SliceData::BytesMut(BytesMutSlice {
                    byte_mut: bm.byte_mut.clone(),
                    offset: bm.offset + start,
                    len: new_len,
                })
            }
            SliceData::MMap(mm) => {
                let new_len = min(end - start, mm.len);
                SliceData::MMap(MMapSlice {
                    mmap: mm.mmap.clone(),
                    offset: mm.offset + start,
                    len: new_len,
                })
            }
        }
    }
    pub fn as_ref(&self) -> &[u8] {
        match self {
            SliceData::Bytes(b) => b.as_ref(), // Bytes 实现 AsRef<[u8]>
            SliceData::BytesMut(bm) => {
                let mut_bytes = bm.byte_mut.as_ref().as_ref();
                let end = min(bm.offset + bm.len, bm.byte_mut.len());
                &mut_bytes[bm.offset..end]
            }
            SliceData::MMap(mm) => {
                let mut_bytes = mm.mmap.as_ref();
                let end = min(mm.offset + mm.len, mm.mmap.len());
                &mut_bytes[mm.offset..end]
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SliceData::Bytes(b) => b.len(),
            SliceData::BytesMut(bm) => bm.len,
            SliceData::MMap(mm) => mm.len,
        }
    }

    pub fn advance(&mut self, n: usize) {
        match self {
            SliceData::Bytes(b) => {
                debug_assert!(n <= b.len());
                b.advance(n);
            }
            SliceData::BytesMut(bm) => {
                debug_assert!(n <= bm.len);
                bm.offset += n;
                bm.len -= n;
            }
            SliceData::MMap(mm) => {
                debug_assert!(n <= mm.len);
                mm.offset += n;
                mm.len -= n;
            }
        }
    }
}

impl Index<usize> for SliceData {
    type Output = u8;
    fn index(&self, index: usize) -> &Self::Output {
        &self.as_ref()[index]
    }
}

impl Index<RangeTo<usize>> for SliceData {
    type Output = [u8];
    fn index(&self, range: RangeTo<usize>) -> &Self::Output {
        &self.as_ref()[..range.end]
    }
}

impl Index<RangeFull> for SliceData {
    type Output = [u8];
    fn index(&self, _: RangeFull) -> &Self::Output {
        &self.as_ref()[..]
    }
}

impl PartialEq for SliceData {
    fn eq(&self, other: &Self) -> bool {
        // 获取两个 SliceData 的字节切片表示
        let self_bytes = self.as_ref();
        let other_bytes = other.as_ref();
        // 逐字节比较
        self_bytes == other_bytes
    }
}

impl Eq for SliceData {}
impl PartialOrd for SliceData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_bytes = self.as_ref();
        let other_bytes = other.as_ref();
        Some(self_bytes.cmp(other_bytes)) // &[u8] 已实现 Ord，直接调用 cmp
    }
}
// 实现 Ord，提供 cmp 方法
impl Ord for SliceData {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_bytes = self.as_ref();
        let other_bytes = other.as_ref();
        self_bytes.cmp(other_bytes) // &[u8] 的逐字节比较
    }
}
impl Hash for SliceData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

#[derive(Debug, Clone, Eq, Hash)]
pub struct Slice {
    pub(crate) data_bytes: SliceData,
}

impl Slice {
    pub(crate) fn new_from_slice(p0: &Slice, p1: Range<usize>) -> Slice {
        Slice {
            data_bytes: p0.data_bytes.slice(p1),
        }
    }
    // 构造函数
    pub(crate) fn new(data: Bytes) -> Self {
        Slice {
            data_bytes: SliceData::from_bytes(&data),
        }
    }
    pub(crate) fn new_from_mut(data: &BytesMut) -> Self {
        Slice {
            data_bytes: SliceData::from_bytes_mut(data),
        }
    }

    pub fn data(&self) -> &[u8] {
        self.data_bytes.as_ref()
    }
    pub(crate) fn new_from_vec(data: Vec<u8>) -> Self {
        Slice {
            data_bytes: SliceData::new_bytes(Bytes::from(data)),
        }
    }
    pub(crate) fn new_from_array(data: &[u8]) -> Self {
        Slice {
            data_bytes: SliceData::new_bytes(Bytes::copy_from_slice(data)),
        }
    }
    pub(crate) fn new_from_string(data: String) -> Self {
        Slice {
            data_bytes: SliceData::new_bytes(Bytes::from(data.into_bytes())),
        }
    }
    pub(crate) fn new_from_static(data: &'static str) -> Self {
        Slice {
            data_bytes: SliceData::new_bytes(Bytes::from(data)),
        }
    }
    pub(crate) fn new_from_str(data: &str) -> Self {
        Slice {
            data_bytes: SliceData::new_bytes(Bytes::copy_from_slice(data.as_bytes())),
        }
    }
    pub(crate) fn new_from_mmap(data: Arc<Mmap>, offset: usize, len: usize) -> Self {
        Slice {
            data_bytes: SliceData::from_arc_mmap(data, offset, len),
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

    pub(crate) fn advance(&mut self, n: usize) {
        self.remove_prefix(n);
    }

    pub fn to_string(&self) -> String {
        String::from_utf8_lossy(self.data()).to_string()
    }

    pub(crate) fn compare(&self, x: &Slice) -> Ordering {
        self.data_bytes.cmp(&x.data_bytes)
    }

    fn starts_with(&self, x: &Slice) -> bool {
        self.data_bytes.len() >= x.data_bytes.len()
            && self.data_bytes[..x.data_bytes.len()] == x.data_bytes[..]
    }
    pub fn slice(&self, n: usize) -> Slice {
        let data_bytes = self.data_bytes.slice(0..min(n, self.len()));
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
            data_bytes: SliceData::Bytes(Bytes::copy_from_slice(&s[..])),
        }
    }
}

impl hash::LocalHash for Slice {
    fn local_hash(&self) -> u32 {
        hash(self.data(), 0)
    }
}
