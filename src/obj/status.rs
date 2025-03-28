use crate::obj::slice::Slice;
use bytes::{BufMut, BytesMut};
use std::cmp::PartialEq;

#[derive(Debug, Clone, Eq, PartialEq, Copy)]
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
    state: Option<BytesMut>,
}

impl Status {
    /*fn copy_state(state: &BytesMut) -> BytesMut {
        debug_assert!(state.len() >= 4);
        let len = u32::from_le_bytes(state[0..size_of::<u32>()].try_into().unwrap());
        let total_len = (len + 5) as usize;
        let mut result = BytesMut::with_capacity(total_len);
        result.extend_from_slice(&state[0..total_len]);
        result
    }*/

    fn new(code: StatusCode, msg: &Slice, msg2: &Slice) -> Status {
        debug_assert!(code != StatusCode::Kok);
        let len1 = msg.len();
        let len2 = msg2.len();
        let size = match len2 {
            0 => len1,
            _ => len1 + len2 + 2,
        };
        let mut result = BytesMut::with_capacity(size + 1);
        /*result.extend_from_slice(&(size as u32).to_le_bytes());*/
        result.put_u8(code as u8);
        result.extend_from_slice(msg.data());
        if len2 > 0 {
            result.put_u8(b':');
            result.put_u8(b' ');
            result.extend_from_slice(msg2.data());
        }
        Status {
            state: Option::from(result),
        }
    }

    fn to_string(&self) -> BytesMut {
        if self.state.is_none() {
            "OK".into()
        } else {
            let mut res = BytesMut::new();
            let code_str = match self.code() {
                StatusCode::Kok => "OK",
                StatusCode::KnotFound => "NotFound: ",
                StatusCode::KCorruption => "Corruption: ",
                StatusCode::KnotSupported => "Not implemented: ",
                StatusCode::KInvalidArgument => "Invalid argument: ",
                StatusCode::KIoError => "IO error: ",
            };
            res.extend_from_slice(code_str.as_ref());
            let msg = &self.state.as_ref().unwrap()[1..];
            res.extend_from_slice(msg);
            res
        }
    }

    fn code(&self) -> StatusCode {
        match self.state {
            None => StatusCode::Kok,
            Some(ref state) => {
                let code = state[0];
                match code {
                    0 => StatusCode::Kok,
                    1 => StatusCode::KnotFound,
                    2 => StatusCode::KCorruption,
                    3 => StatusCode::KnotSupported,
                    4 => StatusCode::KInvalidArgument,
                    5 => StatusCode::KIoError,
                    _ => panic!("unknown code"),
                }
            }
        }
    }
    fn ok() -> Status {
        Status { state: None }
    }

    fn is_ok(&self) -> bool {
        self.state.is_none()
    }
    fn not_found(msg: &Slice, msg2: &Slice) -> Status {
        Status::new(StatusCode::KnotFound, msg, msg2)
    }

    fn is_not_found(&self) -> bool {
        self.code() == StatusCode::KnotFound
    }
    fn corruption(msg: &Slice, msg2: &Slice) -> Status {
        Status::new(StatusCode::KCorruption, msg, msg2)
    }
    fn is_corruption(&self) -> bool {
        self.code() == StatusCode::KCorruption
    }
    fn not_supported(msg: &Slice, msg2: &Slice) -> Status {
        Status::new(StatusCode::KnotSupported, msg, msg2)
    }
    fn is_not_supported(&self) -> bool {
        self.code() == StatusCode::KnotSupported
    }
    fn invalid_argument(msg: &Slice, msg2: &Slice) -> Status {
        Status::new(StatusCode::KInvalidArgument, msg, msg2)
    }
    fn is_invalid_argument(&self) -> bool {
        self.code() == StatusCode::KInvalidArgument
    }
    fn io_error(msg: &Slice, msg2: &Slice) -> Status {
        Status::new(StatusCode::KIoError, msg, msg2)
    }

    fn is_io_error(&self) -> bool {
        self.code() == StatusCode::KIoError
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_move_constructor() {
        let status = Status::ok();
        let status2 = status;
        assert!(status2.is_ok());
        let status = Status::not_found(&"custom NotFound status message".into(), &"".into());
        let status2 = status;
        assert!(status2.is_not_found());
        assert_eq!(
            b"NotFound: custom NotFound status message",
            &status2.to_string()[..]
        );
        let _self_moved = Status::io_error(&"custom IOError status message".into(), &"".into());
    }
}
