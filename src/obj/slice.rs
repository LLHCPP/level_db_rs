use crate::obj::byte_buffer::ByteBuffer;
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
pub(crate) enum SliceData {
    Bytes(Bytes),            // 包含 Bytes 数据
    BytesMut(BytesMutSlice), // 包含 BytesMut 数据
    MMap(MMapSlice),
    PtrBuffer(ByteBuffer),
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

    pub fn from_buffer_byte(buffer: ByteBuffer) -> SliceData {
        SliceData::PtrBuffer(buffer)
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
            SliceData::PtrBuffer(p) => {
                let new_len = min(end - start, p.len());
                SliceData::PtrBuffer(ByteBuffer::from_slice(&p.as_slice()[start..new_len]))
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
            SliceData::PtrBuffer(p) => p.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SliceData::Bytes(b) => b.len(),
            SliceData::BytesMut(bm) => bm.len,
            SliceData::MMap(mm) => mm.len,
            SliceData::PtrBuffer(p) => p.len(),
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
            SliceData::PtrBuffer(p) => {
                debug_assert!(n <= p.len());
                p.advance(n);
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        match self {
            SliceData::Bytes(b) => b.clear(),
            SliceData::BytesMut(bm) => {
                bm.offset = 0;
                bm.len = 0;
            }
            SliceData::MMap(mm) => {
                mm.offset = 0;
                mm.len = 0;
            }
            SliceData::PtrBuffer(p) => p.clear(),
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

impl Eq for SliceData {

}
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
#[derive(PartialOrd)]
pub struct Slice {
    pub(crate) data_bytes: SliceData,
    len: usize,
    cap: usize,
}

impl Slice {
    pub fn new_empty() -> Slice {
        Slice {
            data_bytes: SliceData::PtrBuffer(ByteBuffer::from_ptr(&[])),
            len: 0,
            cap: 0,
        }
    }

    pub(crate) fn new_from_slice(p0: &Slice, p1: Range<usize>) -> Slice {
        let data = p0.data_bytes.slice(p1);
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }
    // 构造函数
    pub(crate) fn new(data: Bytes) -> Self {
        let data = SliceData::from_bytes(&data);
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }
    pub(crate) fn new_from_mut(data: &BytesMut) -> Self {
        let data = SliceData::from_bytes_mut(data);
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }

    pub(crate) fn new_from_buff(data: ByteBuffer) -> Self {
        let data = SliceData::from_buffer_byte(data);
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }

    pub fn new_bytes_mut(bytes: BytesMut) -> Self {
        let len = bytes.len();
        let data = SliceData::new_bytes_mut(bytes);
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }

    pub(crate) fn new_from_string_buffer(str: &String) -> Self {
        let buffer_data = ByteBuffer::from_string(str);
        Self::new_from_buff(buffer_data)
    }

    pub(crate) fn new_from_ptr(data: &[u8]) -> Self {
        let buffer_data = ByteBuffer::from_ptr(data);
        Self::new_from_buff(buffer_data)
    }

    pub fn data(&self) -> &[u8] {
        &self.data_bytes.as_ref()[..self.len()]
    }

    pub(crate) fn new_from_vec(data: Vec<u8>) -> Self {
        let data = SliceData::new_bytes(Bytes::from(data));
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }
    pub(crate) fn new_from_array(data: &[u8]) -> Self {
        let data = SliceData::new_bytes(Bytes::copy_from_slice(data));
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }
    pub(crate) fn new_from_string(data: String) -> Self {
        let data = SliceData::new_bytes(Bytes::from(data.into_bytes()));
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }
    pub(crate) fn new_from_static(data: &'static str) -> Self {
        let data = SliceData::new_bytes(Bytes::from(data));
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }
    pub(crate) fn new_from_str(data: &str) -> Self {
        let data = SliceData::new_bytes(Bytes::copy_from_slice(data.as_bytes()));
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }
    pub(crate) fn new_from_mmap(data: Arc<Mmap>, offset: usize, len: usize) -> Self {
        let data = SliceData::from_arc_mmap(data, offset, len);
        let len = data.len();
        Slice {
            data_bytes: data,
            len,
            cap: len,
        }
    }
    pub fn size(&self) -> usize {
        self.len()
    }
    // 获取引用的长度
    pub fn len(&self) -> usize {
        self.len
    }
    pub(crate) fn remove_prefix(&mut self, n: usize) {
        if n > self.len() {
            panic!("remove_prefix: n is out of range")
        }
        self.advance(n);
    }

    pub(crate) fn advance(&mut self, n: usize) {
        assert!(n <= self.len());
        self.data_bytes.advance(n);
        self.len -= n;
    }

    pub fn to_string(&self) -> String {
        String::from_utf8_lossy(self.data()).to_string()
    }

    pub(crate) fn compare(&self, x: &Slice) -> Ordering {
        self.data().cmp(x.data())
    }

    fn starts_with(&self, x: &Slice) -> bool {
        self.len() >= x.len() && self.data()[..x.data_bytes.len()] == x.data()[..]
    }
    pub fn slice(&self, n: usize) -> Slice {
        let data_bytes = self.data_bytes.clone();
        Slice {
            data_bytes,
            len: min(n, self.len()),
            cap: self.len(),
        }
    }

    pub fn resize(&mut self, n: usize) {
        self.len = n;
    }
    pub fn clear(&mut self) {
        self.len = 0;
        self.cap = 0;
        self.data_bytes.clear();
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

impl Ord for Slice {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
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
            len: s.len(),
            cap: s.len(),
        }
    }
}

impl hash::LocalHash for Slice {
    fn local_hash(&self) -> u32 {
        hash(self.data(), 0)
    }
}

impl Default for Slice {
    fn default() -> Self {
        Slice::new_from_array(&[])
    }
}
