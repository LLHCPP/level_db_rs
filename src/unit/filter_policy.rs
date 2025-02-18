use crate::obj::slice::Slice;

pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn create_filter(&self, keys: &[Slice], n:usize, dst: &mut Vec<u8>);
    fn key_may_match(&self, key: &Slice, filter: &Vec<u8>) -> bool where Self: Sized;
}