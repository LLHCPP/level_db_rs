use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::K_OPEN_BASE_FLAGS;
use bytes::BytesMut;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub trait SequentialFile: Send + Sync {
    fn new<P: AsRef<Path>>(filename: P) -> io::Result<Self>
    where
        Self: Sized;
    fn read(&mut self, n: usize) -> Result<Slice, Status>;
    fn skip(&mut self, n: i64) -> Status;
}

pub struct StdSequentialFile {
    file: BufReader<File>,
    filename: String, // 用于错误报告
}

impl SequentialFile for StdSequentialFile {
    fn new<P: AsRef<Path>>(filename: P) -> io::Result<Self> {
        let mut option = OpenOptions::new();
        option.read(true);
        #[cfg(unix)]
        {
            option.custom_flags(K_OPEN_BASE_FLAGS); // 对应 O_CLOEXEC
        }
        let file = option.open(filename.as_ref())?;
        let buffered_file = BufReader::new(file);
        Ok(StdSequentialFile {
            file: buffered_file,
            filename: filename.as_ref().to_string_lossy().into_owned(),
        })
    }
    fn read(&mut self, n: usize) -> Result<Slice, Status> {
        // 创建一个 BytesMut 来存储数据
        let mut buffer = BytesMut::with_capacity(n);
        buffer.resize(n, 0);
        match self.file.read(&mut buffer) {
            Ok(len) => {
                let res = buffer.split_off(len).freeze();
                Ok(Slice::new(res))
            }
            Err(err) => Err(Status::from_io_error(err, &self.filename)),
        }
    }

    fn skip(&mut self, n: i64) -> Status {
        // 使用 BufReader 的 seek 方法跳过指定字节数
        match self.file.seek(SeekFrom::Current(n)) {
            Ok(_) => Status::ok(),
            Err(e) => Status::from_io_error(e, &self.filename),
        }
    }
}
