use crate::obj::slice::Slice;

pub trait FilterPolicy {
    fn name() -> &'static str;
    fn create_filter(&self, keys: &[Slice], n:usize, dst: &mut Vec<u8>);
    fn key_may_match(key: &Slice, filter: &Slice) -> bool;
}