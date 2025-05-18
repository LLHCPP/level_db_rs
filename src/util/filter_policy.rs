use crate::obj::slice::Slice;
use bytes::BytesMut;
pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn create_filter(&self, keys: &[Slice], dst: &mut BytesMut);
    fn key_may_match(&self, key: &Slice, filter: &Slice) -> bool;
}
