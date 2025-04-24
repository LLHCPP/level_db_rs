use crate::obj::status_rs::Status;
use crate::util::random_access_file::{
    Limiter, PosixMmapReadableFile, RandomAccessFile, StdRandomAccessFile,
};
use crate::util::sequential_file::{SequentialFile, StdSequentialFile};
use crate::util::thread_pool::ThreadPool;
use crate::util::writable_file::WritableFile;
use crate::util::K_OPEN_BASE_FLAGS;
use bytes::{BufMut, BytesMut};
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
use tracing::error;
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

pub fn get_env<T: Env>() -> T {
    T::new()
}

fn read_file_to_string<T: Env, P: AsRef<Path>>(
    env: &T,
    filename: P,
    data: &mut BytesMut,
) -> Status {
    data.clear();
    let mut res = Status::ok();
    let file = env.new_sequential_file::<StdSequentialFile, P>(filename);
    if let Err(e) = file {
        e
    } else {
        let mut file = file.unwrap();
        const K_BUFFER_SIZE: usize = 8192;
        let space = [0; K_BUFFER_SIZE];
        loop {
            let s = file.read(K_BUFFER_SIZE);
            if !s.is_ok() {
                res = s.unwrap_err();
                break;
            }
            let fragment = s.unwrap();
            data.put(fragment.data());
            if fragment.len() == 0 {
                break;
            }
        }
        res
    }
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
            Err(_) => Err(Status::io_error(
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
            Err(_) => Err(Status::io_error(
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
        let path = dir.as_ref();

        // 如果文件夹已存在，直接返回成功
        if path.exists() {
            return Status::ok();
        }
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
                error!(
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
                error!("unlock file {} error: {}", file_lock.file_name, err);
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

#[cfg(test)]
mod tests {
    use crate::obj::slice::Slice;
    use crate::util::env::{get_env, read_file_to_string, Env, StdEnv};
    use crate::util::random::Random;
    use crate::util::sequential_file::{SequentialFile, StdSequentialFile};
    use crate::util::test_util::{random_seed, random_string};
    use crate::util::writable_file::{StdWritableFile, WritableFile};
    use bytes::{BufMut, BytesMut};
    use std::cmp::min;
    use std::sync::{Arc, Condvar, Mutex};

    #[test]
    fn test_read_write() {
        let env = get_env::<StdEnv>();
        let mut rnd = Random::new(random_seed());
        let test_dir = env.get_test_directory().unwrap();
        let test_file_name = format!("{}/open_on_read.txt", test_dir);
        let mut writable_file = env
            .new_writable_file::<StdWritableFile, &str>(&test_file_name)
            .unwrap();
        static K_DATA_SIZE: usize = 10 * 1048576;
        let mut data = BytesMut::new();
        while data.len() < K_DATA_SIZE {
            let len = rnd.skewed(18);
            let mut r = BytesMut::new();
            random_string(&mut rnd, len as usize, &mut r);
            assert!(writable_file.append(&Slice::new_from_mut(&r)).is_ok());
            data.put(&r[..]);
            if rnd.one_in(10) {
                assert!(writable_file.flush().is_ok());
            }
        }
        assert!(writable_file.sync().is_ok());
        drop(writable_file);

        let mut sequential_file = env
            .new_sequential_file::<StdSequentialFile, &str>(&test_file_name)
            .unwrap();

        let mut read_result = BytesMut::new();
        while read_result.len() < data.len() {
            let len = min(rnd.skewed(18), (data.len() - read_result.len()) as u32) as usize;
            let temp = sequential_file.read(len);
            assert!(temp.is_ok());
            let read = temp.unwrap();
            if len > 0 {
                assert!(read.len() > 0);
            }
            assert!(read.size() <= len);
            read_result.put_slice(read.data())
        }
        assert_eq!(data, read_result);
        drop(sequential_file)
    }

    #[test]
    fn test_run_immediately() {
        #[cfg(test)]
        struct RunState {
            called: Mutex<bool>,
            cvar: Condvar,
        }
        impl RunState {
            fn run(arg: Arc<RunState>) {
                let mut called = arg.called.lock().unwrap();
                assert_eq!(*called, false);
                *called = true;
                arg.cvar.notify_all();
                drop(called);
            }
        }
        let env = get_env::<StdEnv>();
        let state = Arc::new(RunState {
            called: Mutex::new(false),
            cvar: Condvar::new(),
        });
        let state_clone = state.clone();
        env.schedule(move || RunState::run(state_clone));
        let mut called = state.called.lock().unwrap();
        while !(*called) {
            called = state.cvar.wait(called).unwrap();
        }
        assert_eq!(*called, true);
    }

    #[test]
    fn test_run_many() {
        #[cfg(test)]
        struct RunState {
            run_count: Mutex<i32>,
            cvar: Condvar,
        }
        #[cfg(test)]
        struct Callback {
            state_: Arc<RunState>,
            run: bool,
        }
        impl Callback {
            fn new(s: Arc<RunState>) -> Callback {
                Callback {
                    state_: s,
                    run: false,
                }
            }
            fn run(arg: Arc<Mutex<Callback>>) {
                let state = arg.lock().unwrap().state_.clone();
                let mut run_count = state.run_count.lock().unwrap();
                *run_count += 1;
                arg.lock().unwrap().run = true;
                state.cvar.notify_all();
                drop(run_count);
            }
        }

        let state = Arc::new(RunState {
            run_count: Mutex::new(0),
            cvar: Condvar::new(),
        });

        // 使用 Vec 管理多个回调
        let callbacks: Vec<_> = (0..4)
            .map(|_| Arc::new(Mutex::new(Callback::new(state.clone()))))
            .collect();

        // 调度所有回调
        let env = get_env::<StdEnv>();
        for cb in callbacks.iter() {
            let cb_clone = cb.clone();
            env.schedule(move || Callback::run(cb_clone));
        }

        // 等待所有回调执行完成
        let mut run_count = state.run_count.lock().unwrap();
        while *run_count < 4 {
            run_count = state.cvar.wait(run_count).unwrap();
        }

        // 验证所有回调是否运行
        for cb in &callbacks {
            assert!(cb.lock().unwrap().run);
        }
    }

    struct State {
        val_num_runing: Mutex<(i32, i32)>,
        cvar: Condvar,
    }
    fn thread_body(arg: Arc<State>) {
        let mut val_num_runing = arg.val_num_runing.lock().unwrap();
        (*val_num_runing).0 += 1;
        (*val_num_runing).1 -= 1;
        arg.cvar.notify_all();
        drop(val_num_runing);
    }
    #[test]
    fn test_start_thread() {
        let env = get_env::<StdEnv>();
        let state = Arc::new(State {
            val_num_runing: Mutex::new((0, 3)),
            cvar: Condvar::new(),
        });
        for _ in 0..3 {
            let temp = state.clone();
            env.start_thread(move || thread_body(temp));
        }
        let mut val_num_runing = state.val_num_runing.lock().unwrap();
        while (*val_num_runing).1 != 0 {
            val_num_runing = state.cvar.wait(val_num_runing).unwrap();
        }
        drop(val_num_runing);
        assert_eq!(state.val_num_runing.lock().unwrap().0, 3);
    }

    #[test]
    fn test_open_non_existent_file() {
        let env = get_env::<StdEnv>();
        let test_dir = env.get_test_directory().unwrap();
        let non_existent_file = format!("{}/non_existent_file", test_dir);
        assert!(!env.file_exists(&non_existent_file));

        let random_access_file = env.new_random_access_file(&non_existent_file);
        assert!(random_access_file.err().unwrap().is_io_error());
        let sequential_file =
            env.new_sequential_file::<StdSequentialFile, &str>(&non_existent_file);
        assert!(sequential_file.err().unwrap().is_io_error())
    }

    #[test]
    fn test_reopen_writable_file() {
        let env = get_env::<StdEnv>();
        let test_dir = env.get_test_directory().unwrap();
        let test_file_name = format!("{}/reopen_writable_file.txt", test_dir);
        env.remove_file(&test_file_name);
        let mut writable_file = env
            .new_writable_file::<StdWritableFile, &str>(&test_file_name)
            .unwrap();
        let mut data = "Hello, World!";
        assert!(writable_file.append(&Slice::new_from_str(data)).is_ok());
        drop(writable_file);

        writable_file = env
            .new_writable_file::<StdWritableFile, &str>(&test_file_name)
            .unwrap();
        data = "42";
        assert!(writable_file.append(&Slice::new_from_str(data)).is_ok());
        drop(writable_file);

        let mut temp_data = BytesMut::new();
        assert!(read_file_to_string(&env, &test_file_name, &mut temp_data).is_ok());

        assert_eq!(temp_data, BytesMut::from("42"));
        env.remove_file(&test_file_name);
    }

    #[test]
    fn test_reopen_appendable_file() {
        let env = get_env::<StdEnv>();
        let test_dir = env.get_test_directory().unwrap();
        let test_file_name = format!("{}/reopen_appendable_file.txt", test_dir);
        env.remove_file(&test_file_name);

        let mut appendable_file = env
            .new_appendable_file::<StdWritableFile, &str>(&test_file_name)
            .unwrap();

        let mut data = "Hello, World!";
        assert!(appendable_file.append(&Slice::new_from_str(data)).is_ok());
        drop(appendable_file);

        appendable_file = env
            .new_appendable_file::<StdWritableFile, &str>(&test_file_name)
            .unwrap();
        data = "42";
        assert!(appendable_file.append(&Slice::new_from_str(data)).is_ok());
        drop(appendable_file);
        let mut temp_data = BytesMut::new();
        assert!(read_file_to_string(&env, &test_file_name, &mut temp_data).is_ok());
        assert_eq!(temp_data, BytesMut::from("Hello, World!42"));
        env.remove_file(&test_file_name);
    }
}
