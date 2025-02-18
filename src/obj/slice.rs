use std::cmp::Ordering;
use std::ops::Index;

#[derive(Debug, Clone)]
pub struct Slice {
    pub(crate) data_bytes:Box<[u8]>,
}
impl Slice {
    // 构造函数
    fn new(data: Box<[u8]>) -> Self {
        Slice { data_bytes: data }
    }

    pub fn data(&self) -> &[u8] {
        &*self.data_bytes
    }
    fn new_from_string(data: String) -> Self {
        Slice { data_bytes: Box::from(data.into_bytes()) }
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
        self.data_bytes = Box::from(&self.data()[n..]);
    }
    fn to_string(&self) -> String {
        String::from_utf8_lossy(&*self.data_bytes).to_string()
    }

    fn compare(&self, x: &Slice) -> Ordering {
        return self.data_bytes.cmp(&x.data_bytes)
    }

    fn starts_with(&self, x: &Slice) -> bool {
        return self.data_bytes.len() >= x.data_bytes.len() &&
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