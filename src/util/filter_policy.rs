use crate::obj::slice::Slice;
use bytes::{Bytes, BytesMut};
pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn create_filter(&self, keys: &[Slice], dst: &mut BytesMut);
    fn key_may_match(&self, key: &Slice, filter: &Slice) -> bool
    where
        Self: Sized;
}
