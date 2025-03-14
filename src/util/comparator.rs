use crate::obj::slice::Slice;
use bytes::BytesMut;
use std::cmp::Ordering;

pub trait Comparator: Send + Sync {
    fn compare(&self, a: &Slice, b: &Slice) -> Ordering;
    fn name(&self) -> &'static str;
    fn find_shortest_separator(&self, start: &mut BytesMut, limit: &Slice);
    fn find_short_successor(&self, key: &mut BytesMut);
}
