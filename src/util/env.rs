use crate::obj::status_rs::Status;
use crate::util::random_access_file::{
    Limiter, PosixMmapReadableFile, RandomAccessFile, StdRandomAccessFile,
};
use crate::util::sequential_file::SequentialFile;
use crate::util::thread_pool::ThreadPool;
use crate::util::writable_file::WritableFile;
use crate::util::K_OPEN_BASE_FLAGS;
use libc::{gettimeofday, timeval};
use rustix::fs::FlockOperation;
use std::fs::{remove_file, DirBuilder, File, OpenOptions};
use std::os::fd::BorrowedFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::{env, fs, io, thread};
#[cfg(unix)]
use {rustix::fs::fcntl_lock, std::os::unix::io::AsRawFd};
#[cfg(windows)]
use {rustix::windows::lock_file, std::os::windows::io::AsRawHandle};

#[cfg(target_pointer_width = "64")]
pub const DEFAULT_MMAP_LIMIT: i64 = 1000;

#[cfg(not(target_pointer_width = "64"))]
pub const DEFAULT_MMAP_LIMIT: i64 = 0;

#[cfg(target_os = "windows")]
fn compute_max_open_files() -> i64 {
    // Windows 平台下，文件句柄由系统管理，我们认为是无限制的，
    // 使用 i32::MAX 来表示无限制的效果。
    std::i64::MAX
}

#[cfg(all(not(target_os = "windows"), target_os = "fuchsia"))]
fn compute_max_open_files() -> i64 {
    // Fuchsia 平台没有实现 getrlimit，直接返回 50。
    50
}

#[cfg(unix)]
fn compute_max_open_files() -> i64 {
    unsafe {
        // 获取当前进程可打开文件描述符的数量限制
        let mut rlim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) != 0 {
            // 获取失败，使用默认值 50。
            return 50;
        }
        // 如果限制为无限大，则返回 i32::MAX。
        if rlim.rlim_cur == libc::RLIM_INFINITY {
            return i64::MAX;
        }
        // 否则，允许使用 20% 的文件描述符数作为只读文件的上限。
        (rlim.rlim_cur / 5) as i64
    }
}

// 使用 OnceLock 来缓存计算结果，避免多次调用系统函数
fn max_open_files() -> i64 {
    static MAX_OPEN_FILES: OnceLock<i64> = OnceLock::new();
    *MAX_OPEN_FILES.get_or_init(|| compute_max_open_files())
}

#[cfg(unix)]
fn default_test_path() -> String {
    let uid = unsafe { libc::geteuid() };
    format!("/tmp/leveldbtest-{}", uid)
}
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

pub struct FileLock {
    file: File,
    file_name: String,
}

impl FileLock {
    fn new(file: File, name: &str) -> FileLock {
        FileLock {
            file,
            file_name: name.to_string(),
        }
    }
}

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

    fn rename_file<P: AsRef<Path>>(&self, src_filename: P, target_filename: P) -> Status;

    fn lock_file<P: AsRef<Path>>(&self, filename: P) -> Result<FileLock, Status>;
    fn unlock_file(&self, file_lock: &FileLock) -> Status;
    fn schedule<F: FnOnce() + Send + 'static>(&self, function: F);

    fn start_thread<F: FnOnce() + Send + 'static>(&self, function: F);

    fn get_test_directory(&self) -> Result<String, Status>;

    fn now_micros(&self) -> u64;

    fn sleep_for_microseconds(&self, micros: u64);
}

struct StdEnv {
    mmap_limiter_: Arc<Limiter>,
    fd_limiter_: Arc<Limiter>,
    thread_pool: ThreadPool,
}

impl Env for StdEnv {
    fn new() -> Self {
        StdEnv {
            mmap_limiter_: Arc::new(Limiter::new(DEFAULT_MMAP_LIMIT)),
            fd_limiter_: Arc::new(Limiter::new(max_open_files())),
            thread_pool: ThreadPool::new(1),
        }
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
        match T::new(filename.as_ref(), true) {
            Ok(writable_file) => Ok(writable_file),
            Err(err) => Err(Status::io_error(
                "new StdWritableFile writable file error",
                Some(&filename.as_ref().to_string_lossy().into_owned()),
            )),
        }
    }

    fn new_appendable_file<T: WritableFile, P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<T, Status> {
        match T::new(filename.as_ref(), false) {
            Ok(writable_file) => Ok(writable_file),
            Err(err) => Err(Status::io_error(
                "new StdWritableFile writable file error",
                Some(&filename.as_ref().to_string_lossy().into_owned()),
            )),
        }
    }

    fn file_exists<P: AsRef<Path>>(&self, filename: P) -> bool {
        Path::new(filename.as_ref()).exists()
    }

    fn get_children<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<String>, Status> {
        let mut result = Vec::new();
        match fs::read_dir(Path::new(dir.as_ref())) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    if let Some(name) = entry.file_name().to_str() {
                        result.push(name.to_string());
                    }
                }
                Ok(result)
            }
            Err(err) => Err(Status::from_io_error(
                err,
                &dir.as_ref().to_string_lossy().into_owned(),
            )),
        }
    }

    fn remove_file<P: AsRef<Path>>(&self, filename: P) -> Status {
        match remove_file(filename.as_ref()) {
            Ok(_) => Status::ok(),
            Err(err) => {
                Status::from_io_error(err, &filename.as_ref().to_string_lossy().into_owned())
            }
        }
    }

    fn delete_file<P: AsRef<Path>>(&self, filename: P) -> Status {
        self.remove_file(filename)
    }

    fn create_dir<P: AsRef<Path>>(&self, dir: P) -> Status {
        let mut builder = DirBuilder::new();

        #[cfg(unix)]
        {
            builder.mode(0o755);
        }
        match builder.create(dir.as_ref()) {
            Ok(_) => Status::ok(),
            Err(err) => Status::from_io_error(err, &dir.as_ref().to_string_lossy().into_owned()),
        }
    }

    fn remove_dir<P: AsRef<Path>>(&self, dir: P) -> Status {
        match fs::remove_dir(dir.as_ref()) {
            Ok(_) => Status::ok(),
            Err(err) => Status::from_io_error(err, &dir.as_ref().to_string_lossy().into_owned()),
        }
    }

    fn delete_dir<P: AsRef<Path>>(&self, dir: P) -> Status {
        self.remove_dir(dir)
    }

    fn get_file_size<P: AsRef<Path>>(&self, filename: P) -> Result<u64, Status> {
        match fs::metadata(filename.as_ref()) {
            Ok(size) => Ok(size.len()),
            Err(e) => Err(Status::from_io_error(
                e,
                &filename.as_ref().to_string_lossy().into_owned(),
            )),
        }
    }

    fn rename_file<P: AsRef<Path>>(&self, src_filename: P, target_filename: P) -> Status {
        match fs::rename(src_filename.as_ref(), target_filename.as_ref()) {
            Ok(_) => Status::ok(),
            Err(e) => {
                Status::from_io_error(e, &src_filename.as_ref().to_string_lossy().into_owned())
            }
        }
    }

    fn lock_file<P: AsRef<Path>>(&self, filename: P) -> Result<FileLock, Status> {
        let mut option = OpenOptions::new();
        option.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            option.mode(0o644);
            option.custom_flags(K_OPEN_BASE_FLAGS); // 对应 O_CLOEXEC
        }
        match option.open(filename.as_ref()) {
            Ok(file) => {
                if let Ok(_) = lock_file_exclusive(&file) {
                    Ok(FileLock::new(file, &filename.as_ref().to_string_lossy()))
                } else {
                    Err(Status::io_error(
                        "lock file error",
                        Some(&filename.as_ref().to_string_lossy().into_owned()),
                    ))
                }
            }
            Err(err) => {
                log::error!(
                    "lock file {} error: {}",
                    filename.as_ref().to_string_lossy(),
                    err
                );
                Err(Status::from_io_error(
                    err,
                    &filename.as_ref().to_string_lossy().into_owned(),
                ))
            }
        }
    }

    fn unlock_file(&self, file_lock: &FileLock) -> Status {
        match unlock_file(&file_lock.file) {
            Ok(_) => Status::ok(),
            Err(err) => {
                log::error!("unlock file {} error: {}", file_lock.file_name, err);
                Status::from_io_error(err, &file_lock.file_name)
            }
        }
    }

    fn schedule<F: FnOnce() + Send + 'static>(&self, function: F) {
        self.thread_pool.execute(function)
    }

    fn start_thread<F: FnOnce() + Send + 'static>(&self, function: F) {
        self.thread_pool.execute(function)
    }

    fn get_test_directory(&self) -> Result<String, Status> {
        let mut dir = String::new();
        if let Ok(env_val) = env::var("TEST_TMPDIR") {
            if !env_val.is_empty() {
                dir = env_val;
            } else {
                dir = default_test_path();
            }
        } else {
            dir = default_test_path();
        }
        let status = self.create_dir(&dir);
        if status.is_ok() {
            Ok(dir)
        } else {
            Err(status)
        }
    }

    fn now_micros(&self) -> u64 {
        let mut tv = timeval {
            tv_sec: 0,
            tv_usec: 0,
        };
        unsafe {
            gettimeofday(&mut tv, std::ptr::null_mut());
        }
        (tv.tv_sec as u64) * 1_000_000 + (tv.tv_usec as u64)
    }

    fn sleep_for_microseconds(&self, micros: u64) {
        thread::sleep(Duration::from_micros(micros));
    }
}
