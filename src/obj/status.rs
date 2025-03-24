use bytes::BytesMut;
#[derive(Debug, Clone)]
#[repr(u8)]
pub enum StatusCode {
    Kok = 0,
    KnotFound = 1,
    KCorruption = 2,
    KnotSupported = 3,
    KInvalidArgument = 4,
    KIoError = 5,
}

#[derive(Debug, Clone)]
struct Status {
    code: StatusCode,
    state: BytesMut,
}
impl Status {
    fn copy_state(state: &BytesMut) -> BytesMut {
        debug_assert!(state.len() >= 4);
        let len = u32::from_le_bytes(state[0..size_of::<u32>()].try_into().unwrap());
        let total_len = (len + 5) as usize;
        let mut result = BytesMut::with_capacity(total_len);
        result.extend_from_slice(&state[0..total_len]);
        result
    }
    fn ok() -> Status {
        Status {
            code: StatusCode::Kok,
            state: BytesMut::new(),
        }
    }
}
