use crate::obj::slice::Slice;
use bytes::Bytes;
pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn create_filter(&self, keys: &[Slice], dst: &mut Bytes);
    fn key_may_match(&self, key: &Slice, filter: &Bytes) -> bool
    where
        Self: Sized;
}
