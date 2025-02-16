struct Slice<'a> {
    data_bytes:&'a str,         
}

impl<'a> Slice<'a> {
    // 构造函数
    fn new(data: &'a str) -> Self {
        Slice { data_bytes: data }
    }

    // 获取引用的长度
    fn len(&self) -> usize {
        self.data_bytes.len()
    }

    // 打印内容
    fn print(&self) {
        println!("Slice data: {:?}", self.data_bytes);
    }
}