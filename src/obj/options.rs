use crate::util::cache::ShardedLRUCache;
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use std::hash::Hash;

enum CompressionType {
    None,
    Snappy,
    Zstd,
}

struct Options<C, E, K, V, F>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
    F: FilterPolicy,
{
    comparator: C,
    create_if_missing: bool,
    error_if_exists: bool,
    paranoid_checks: bool,
    env: E,
    write_buffer_size: usize,
    max_open_files: u64,
    block_cache: ShardedLRUCache<K, V>,
    block_size: usize,
    block_restart_interval: u32,
    max_file_size: usize,
    compression: CompressionType,
    zstd_compression_level: u32,
    reuse_logs: bool,
    filter_policy: F,
}

/*struct ReadOptions<C, E, K, V, F>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
    F: FilterPolicy,
{
    options: Options<C, E, K, V, F>,
    max_background_compactions: u32,
    max_background_flushes:
}*/
