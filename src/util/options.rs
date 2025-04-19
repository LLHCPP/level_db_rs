use crate::util::comparator::Comparator;
use crate::util::env::Env;

enum CompressionType {
    KnoCompression = 0x0,
    KSnappyCompression = 0x1,
    KZstdCompression = 0x2,
}

struct Options<T: Comparator, S: Env> {
    comparator: T,
    create_if_missing: bool,
    error_if_exists: bool,
    paranoid_checks: bool,
    env: S,
    write_buffer_size: usize,
    max_open_files: usize,
}
