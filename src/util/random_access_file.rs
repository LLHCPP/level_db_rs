use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use bytes::BytesMut;
use memmap2::Mmap;
use positioned_io;
use positioned_io::ReadAt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

pub trait RandomAccessFile: Send + Sync {
    fn read(&mut self, offset: u64, n: usize) -> Result<Slice, Status>;
}

pub struct Limiter {
    #[cfg(debug_assertions)]
    max_acquires: i64,
    acquires_allowed: AtomicI64,
}

impl Limiter {
    /// Limit maximum number of resources to `max_acquires`.
    pub fn new(max_acquires: i64) -> Self {
        debug_assert!(max_acquires >= 0);
        Limiter {
            #[cfg(debug_assertions)]
            max_acquires,
            acquires_allowed: AtomicI64::new(max_acquires),
        }
    }

    // If another resource is available, acquire it and return true.
    // Else return false.
    pub fn acquire(&self) -> bool {
        let old_acquires_allowed = self.acquires_allowed.fetch_sub(1, Ordering::Relaxed);

        if old_acquires_allowed > 0 {
            return true;
        }

        let pre_increment_acquires_allowed = self.acquires_allowed.fetch_add(1, Ordering::Relaxed);

        // Silence compiler warnings about unused arguments when NDEBUG is defined.
        let _ = pre_increment_acquires_allowed;
        // If the check below fails, Release() was called more times than acquire.
        #[cfg(debug_assertions)]
        debug_assert!(pre_increment_acquires_allowed < self.max_acquires);

        false
    }

    /// Release a resource acquired by a previous call to Acquire() that returned
    /// true.
    pub fn release(&self) {
        let old_acquires_allowed = self.acquires_allowed.fetch_add(1, Ordering::Relaxed);

        // Silence compiler warnings about unused arguments when NDEBUG is defined.
        let _ = old_acquires_allowed;
        // If the check below fails, Release() was called more times than acquire.
        #[cfg(debug_assertions)]
        debug_assert!(old_acquires_allowed < self.max_acquires);
    }
}

struct StdRandomAccessFile {
    limiter: Arc<Limiter>,
    has_permanent_fd_: bool,
    file: Option<positioned_io::RandomAccessFile>,
    filename: String, // 用于错误报告
}

impl StdRandomAccessFile {
    pub fn new<P: AsRef<std::path::Path>>(
        filename: P,
        limiter: Arc<Limiter>,
    ) -> StdRandomAccessFile {
        if limiter.acquire() {
            let file = match positioned_io::RandomAccessFile::open(filename.as_ref()) {
                Ok(file) => Some(file),
                Err(_) => None,
            };
            StdRandomAccessFile {
                limiter,
                has_permanent_fd_: true,
                file,
                filename: filename.as_ref().to_string_lossy().into_owned(),
            }
        } else {
            StdRandomAccessFile {
                limiter,
                has_permanent_fd_: false,
                file: None,
                filename: filename.as_ref().to_string_lossy().into_owned(),
            }
        }
    }
}

impl Drop for StdRandomAccessFile {
    fn drop(&mut self) {
        if self.has_permanent_fd_ {
            self.limiter.release();
        }
    }
}

impl RandomAccessFile for StdRandomAccessFile {
    fn read(&mut self, offset: u64, n: usize) -> Result<Slice, Status> {
        let temp_file = if let Some(file) = &self.file {
            file // 如果文件已存在，直接使用
        } else {
            // 创建一个临时变量并确保其生命周期足够长
            match positioned_io::RandomAccessFile::open(&self.filename) {
                Ok(temp_file) => {
                    self.file = Some(temp_file);
                    self.file.as_ref().unwrap()
                }
                Err(err) => {
                    return Err(Status::from_io_error(err, &self.filename));
                }
            }
        };
        let mut buffer = BytesMut::with_capacity(n);
        buffer.resize(n, 0);
        let res = match temp_file.read_at(offset, &mut buffer) {
            Ok(len) => {
                let res = buffer.split_off(len).freeze();
                Ok(Slice::new(res))
            }
            Err(err) => Err(Status::from_io_error(err, &self.filename)),
        };
        if !self.has_permanent_fd_ {
            self.file = None;
        };
        res
    }
}

struct PosixMmapReadableFile {
    limiter: Arc<Limiter>,
    m_map: Arc<Mmap>,
    filename: String,
}

impl PosixMmapReadableFile {
    fn new(m_map_base: Mmap, filename: String, limiter: Arc<Limiter>) -> PosixMmapReadableFile {
        PosixMmapReadableFile {
            limiter,
            m_map: Arc::new(m_map_base),
            filename,
        }
    }
}

impl RandomAccessFile for PosixMmapReadableFile {
    fn read(&mut self, offset: u64, n: usize) -> Result<Slice, Status> {
        if offset + n as u64 > self.m_map.len() as u64 {
            Err(Status::io_error(
                &self.filename,
                Some("offset + n out of range"),
            ))
        } else {
            Ok(Slice::new_from_mmap(self.m_map.clone(), offset as usize, n))
        }
    }
}
