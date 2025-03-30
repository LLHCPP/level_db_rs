use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use rustix::fs::{fdatasync, fsync};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufWriter, Write};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::path::Path;

pub trait WritableFile {
    fn append(&mut self, data: &Slice) -> Status;
    fn flush(&mut self) -> Status;
    fn sync(&mut self) -> Status;
}
const K_WRITABLE_FILE_BUFFER_SIZE: usize = 65536;

fn basename<'a, P: AsRef<Path> + 'a>(path: &'a P) -> Option<&'a str> {
    path.as_ref().file_name().and_then(|s| s.to_str())
}

fn dirname<'a, P: AsRef<Path> + 'a>(path: &'a P) -> Option<&'a str> {
    path.as_ref().parent().and_then(|p| p.to_str())
}
pub fn is_manifest<'a, P: AsRef<Path> + 'a>(filename: &'a P) -> bool {
    match basename(filename) {
        Some(s) => s.starts_with("MANIFEST"),
        None => false,
    }
}

// 自定义文件同步函数，根据平台选择最佳同步方法
pub fn sync_fd(file: &File) -> io::Result<()> {
    let raw_fd = file.as_raw_fd();
    let fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
    // 在 macOS 上尝试使用 F_FULLFSYNC
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        if let Ok(_) = fcntl_fullfsync(fd) {
            return Ok(());
        }
        // 如果 F_FULLFSYNC 失败，继续回退到其他方法
    }
    // 在 Linux 或 Android 上尝试使用 fdatasync
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        if let Ok(_) = fdatasync(fd) {
            return Ok(());
        }
        // 如果 fdatasync 失败，继续回退
    }
    // 最后回退到标准的 fsync
    fsync(fd)?;
    Ok(())
}

struct StdWritableFile {
    write_buf: BufWriter<File>,
    filename: String,
    #[cfg(target_family = "unix")]
    dirname_: String,
    is_manifest_: bool,
}
impl StdWritableFile {
    fn new<P: AsRef<Path>>(filename: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true) // 启用写入权限
            .append(true) // 启用追加模式
            .create(true) // 如果文件不存在，则创建
            .open(filename.as_ref())?;
        let write_buf = BufWriter::with_capacity(K_WRITABLE_FILE_BUFFER_SIZE, file);
        let filename = filename.as_ref().to_string_lossy().into_owned();
        let is_manifest_ = is_manifest(&filename);
        #[cfg(target_family = "unix")]
        let dirname_ = dirname(&filename).unwrap_or(".").to_string();
        Ok(StdWritableFile {
            write_buf,
            #[cfg(target_family = "unix")]
            dirname_,
            filename,
            is_manifest_,
        })
    }
    #[cfg(target_family = "unix")]
    fn sync_dir_if_manifest(&self) -> Status {
        if !self.is_manifest_ {
            return Status::ok();
        }
        match File::open(&self.dirname_) {
            Ok(file) => match sync_fd(&file) {
                Ok(_) => Status::ok(),
                Err(e) => Status::from_io_error(e, &self.dirname_),
            },
            Err(err) => Status::from_io_error(err, &self.dirname_),
        }
    }
}

impl WritableFile for StdWritableFile {
    fn append(&mut self, data: &Slice) -> Status {
        let write_data = data.data();
        match self.write_buf.write_all(write_data) {
            Ok(_) => Status::ok(),
            Err(e) => Status::from_io_error(e, &self.filename),
        }
    }

    fn flush(&mut self) -> Status {
        match self.write_buf.flush() {
            Ok(_) => Status::ok(),
            Err(e) => Status::from_io_error(e, &self.filename),
        }
    }

    fn sync(&mut self) -> Status {
        #[cfg(target_family = "unix")]
        {
            let status = self.sync_dir_if_manifest();
            if !status.is_ok() {
                return status;
            };
        }
        let status = self.flush();
        if !status.is_ok() {
            return status;
        };
        match sync_fd(&self.write_buf.get_ref()) {
            Ok(_) => Status::ok(),
            Err(e) => Status::from_io_error(e, &self.filename),
        }
    }
}
