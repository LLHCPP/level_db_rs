// 类 Unix 系统相关依赖
#[cfg(unix)]
use rustix::fs::fcntl_lock;
use std::fs::File;
use std::io::{self};
use std::os::fd::BorrowedFd;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

// Windows 系统相关依赖
use crate::obj::status_rs::Status;
use crate::util::sequential_file::SequentialFile;
use rustix::fs::FlockOperation;
#[cfg(windows)]
use rustix::windows::lock_file;
// 假设 rustix 提供此函数
use crate::util::random_access_file::{
    Limiter, PosixMmapReadableFile, RandomAccessFile, StdRandomAccessFile,
};
use crate::util::writable_file::WritableFile;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::sync::Arc;

/// 为文件添加独占锁（exclusive lock）
fn lock_file_exclusive(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    {
        let raw_fd = file.as_raw_fd();
        let fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
        // 使用 flock 设置独占锁，将错误转换为 io::Error
        fcntl_lock(fd, FlockOperation::NonBlockingLockExclusive)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
    #[cfg(windows)]
    {
        let handle = file.as_raw_handle();
        lock_file(handle /* 独占锁参数 */).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

/// 释放文件锁
fn unlock_file(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    {
        let raw_fd = file.as_raw_fd();
        let fd = unsafe { BorrowedFd::borrow_raw(raw_fd) };
        // 使用 flock 解锁
        fcntl_lock(fd, FlockOperation::NonBlockingUnlock)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
    #[cfg(windows)]
    {
        let handle = file.as_raw_handle();
        // 假设 rustix 提供 Windows 的解锁函数
        unlock_file(handle).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

/*struct FileLock{
    file: Arc<File>,
}*/

pub trait Env {
    fn new() -> Self;
    fn new_sequential_file<T: SequentialFile, P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<T, Status>;
    fn new_random_access_file<P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<Arc<dyn RandomAccessFile>, Status>;
    fn new_writable_file<T: WritableFile, P: AsRef<Path>>(&self, filename: P) -> Result<T, Status>;
    fn new_appendable_file<T: WritableFile, P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<T, Status>;

    fn file_exists<P: AsRef<Path>>(&self, filename: P) -> bool;
    fn get_children<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<String>, Status>;
    fn remove_file<P: AsRef<Path>>(&self, filename: P) -> Status;
    fn delete_file<P: AsRef<Path>>(&self, filename: P) -> Status;

    fn create_dir<P: AsRef<Path>>(&self, dir: P) -> Status;
    fn remove_dir<P: AsRef<Path>>(&self, dir: P) -> Status;

    fn delete_dir<P: AsRef<Path>>(&self, dir: P) -> Status;

    fn get_file_size<P: AsRef<Path>>(&self, filename: P) -> Result<u64, Status>;

    fn rename_file<P: AsRef<Path>>(
        &self,
        src_filename: P,
        target_filename: P,
    ) -> Result<u64, Status>;

    fn lock_file<P: AsRef<Path>>(&self, filename: P) -> Status;
    fn unlock_file<P: AsRef<Path>>(&self, filename: P) -> Status;
    fn schedule<F: Fn() + Send + 'static>(&self, function: F);

    fn start_thread<F: Fn() + Send + 'static>(&self, function: F);

    fn get_test_directory(&self) -> Result<String, Status>;

    fn now_micros(&self) -> u64;

    fn sleep_for_microseconds(&self, micros: u64);
}

struct StdEnv {
    mmap_limiter_: Arc<Limiter>,
    fd_limiter_: Arc<Limiter>,
}

impl Env for StdEnv {
    fn new() -> Self {
        todo!()
    }

    fn new_sequential_file<T: SequentialFile, P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<T, Status> {
        let sequential_file = T::new(filename.as_ref());
        if let Ok(sequential) = sequential_file {
            Ok(sequential)
        } else {
            Err(Status::io_error(
                "new sequential file error",
                Some(&filename.as_ref().to_string_lossy().into_owned()),
            ))
        }
    }

    fn new_random_access_file<P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<Arc<dyn RandomAccessFile>, Status> {
        if !self.mmap_limiter_.acquire() {
            let res = StdRandomAccessFile::new(filename.as_ref(), self.fd_limiter_.clone());
            if let Ok(random_access_file) = res {
                Ok(Arc::new(random_access_file))
            } else {
                Err(Status::io_error(
                    "new StdRandomAccessFile random access file error",
                    Some(&filename.as_ref().to_string_lossy().into_owned()),
                ))
            }
        } else {
            let res = PosixMmapReadableFile::new(filename.as_ref(), self.mmap_limiter_.clone());
            if let Ok(random_access_file) = res {
                Ok(Arc::new(random_access_file))
            } else {
                Err(Status::io_error(
                    "new PosixMmapReadableFile random access file error",
                    Some(&filename.as_ref().to_string_lossy().into_owned()),
                ))
            }
        }
    }

    fn new_writable_file<T: WritableFile, P: AsRef<Path>>(&self, filename: P) -> Result<T, Status> {
        todo!()
    }

    fn new_appendable_file<T: WritableFile, P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<T, Status> {
        todo!()
    }

    fn file_exists<P: AsRef<Path>>(&self, filename: P) -> bool {
        todo!()
    }

    fn get_children<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<String>, Status> {
        todo!()
    }

    fn remove_file<P: AsRef<Path>>(&self, filename: P) -> Status {
        todo!()
    }

    fn delete_file<P: AsRef<Path>>(&self, filename: P) -> Status {
        todo!()
    }

    fn create_dir<P: AsRef<Path>>(&self, dir: P) -> Status {
        todo!()
    }

    fn remove_dir<P: AsRef<Path>>(&self, dir: P) -> Status {
        todo!()
    }

    fn delete_dir<P: AsRef<Path>>(&self, dir: P) -> Status {
        todo!()
    }

    fn get_file_size<P: AsRef<Path>>(&self, filename: P) -> Result<u64, Status> {
        todo!()
    }

    fn rename_file<P: AsRef<Path>>(
        &self,
        src_filename: P,
        target_filename: P,
    ) -> Result<u64, Status> {
        todo!()
    }

    fn lock_file<P: AsRef<Path>>(&self, filename: P) -> Status {
        todo!()
    }

    fn unlock_file<P: AsRef<Path>>(&self, filename: P) -> Status {
        todo!()
    }

    fn schedule<F: Fn() + Send + 'static>(&self, function: F) {
        todo!()
    }

    fn start_thread<F: Fn() + Send + 'static>(&self, function: F) {
        todo!()
    }

    fn get_test_directory(&self) -> Result<String, Status> {
        todo!()
    }

    fn now_micros(&self) -> u64 {
        todo!()
    }

    fn sleep_for_microseconds(&self, micros: u64) {
        todo!()
    }
}
