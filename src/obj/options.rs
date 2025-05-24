use crate::db::snap_shot::Snapshot;
use crate::obj::slice::Slice;
use crate::table::block::Block;
use crate::util::cache::ShardedLRUCache;
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use num_derive::{FromPrimitive, ToPrimitive};
use std::sync::Arc;

#[derive(FromPrimitive, ToPrimitive)]
pub enum CompressionType {
    None = 0x0,
    Snappy = 0x1,
    Zstd = 0x2,
}

pub struct Options<E>
where
    E: Env,
{
    pub(crate) comparator: Arc<dyn Comparator>,
    create_if_missing: bool,
    error_if_exists: bool,
    pub(crate) paranoid_checks: bool,
    pub(crate) env: Arc<E>,
    write_buffer_size: usize,
    max_open_files: u64,
    pub(crate) block_cache: Option<ShardedLRUCache<Slice, Block>>,
    block_size: usize,
    pub(crate) block_restart_interval: u32,
    max_file_size: usize,
    compression: CompressionType,
    zstd_compression_level: u32,
    reuse_logs: bool,
    pub(crate) filter_policy: Option<Arc<dyn FilterPolicy>>,
}

pub struct ReadOptions {
    pub(crate) verify_checksums: bool,
    pub(crate) fill_cache: bool,
    snapshot: Option<Arc<dyn Snapshot>>,
}
impl ReadOptions {
    pub fn new() -> ReadOptions {
        ReadOptions {
            verify_checksums: false,
            fill_cache: true,
            snapshot: None,
        }
    }
}
