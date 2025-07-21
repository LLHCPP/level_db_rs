#[repr(usize)]
#[derive(Debug, Clone)]
pub enum RecordType {
    KZeroType = 0,
    KFullType = 1,
    // For fragments
    KFirstType = 2,
    KMiddleType = 3,
    KLastType = 4,
}

pub const K_MAX_RECORD_TYPE: usize = RecordType::KLastType as usize;
pub const K_BLOCK_SIZE: usize = 32768;
pub const K_HEADER_SIZE: usize = 4 + 2 + 1;
