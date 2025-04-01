use std::fs::File;
use std::io::{self};
use std::os::fd::BorrowedFd;
// 类 Unix 系统相关依赖
#[cfg(unix)]
use rustix::fs::fcntl_lock;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

// Windows 系统相关依赖
use crate::obj::status_rs::Status;
use crate::util::sequential_file::{SequentialFile, StdSequentialFile};
use rustix::fs::FlockOperation;
#[cfg(windows)]
use rustix::windows::lock_file; // 假设 rustix 提供此函数
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

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

pub fn new_sequential_file<T: SequentialFile>(filename: &str) -> Result<T, Status> {
    let sequential_file = T::new(filename);
    if let Ok(sequential) = sequential_file {
        Ok(sequential)
    } else {
        Err(Status::io_error(
            "new sequential file error",
            Some(filename),
        ))
    }
}
