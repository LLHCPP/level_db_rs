use crate::db::snap_shot::Snapshot;
use crate::util::cache::ShardedLRUCache;
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use num_derive::{FromPrimitive, ToPrimitive};
use std::hash::Hash;
use std::sync::Arc;
#[derive(FromPrimitive, ToPrimitive)]
pub enum CompressionType {
    None = 0x0,
    Snappy = 0x1,
    Zstd = 0x2,
}

pub struct Options<C, E, K, V, F>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
    F: FilterPolicy,
{
    pub(crate) comparator: C,
    create_if_missing: bool,
    error_if_exists: bool,
    pub(crate) paranoid_checks: bool,
    pub(crate) env: Arc<E>,
    write_buffer_size: usize,
    max_open_files: u64,
    pub(crate) block_cache: Option<ShardedLRUCache<K, V>>,
    block_size: usize,
    pub(crate) block_restart_interval: u32,
    max_file_size: usize,
    compression: CompressionType,
    zstd_compression_level: u32,
    reuse_logs: bool,
    filter_policy: F,
}

pub struct ReadOptions {
    pub(crate) verify_checksums: bool,
    fill_cache: bool,
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
