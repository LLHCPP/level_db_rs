use crate::obj::slice::Slice;
use bytes::BytesMut;
use std::cmp::Ordering;

pub trait Comparator: Send + Sync {
    fn compare(&mut self, a: &Slice, b: &Slice) -> Ordering;
    fn name(&self) -> &'static str;
    fn find_shortest_separator(&mut self, start: &mut BytesMut, limit: &Slice);
    fn find_short_successor(&mut self, key: &mut BytesMut);
}
