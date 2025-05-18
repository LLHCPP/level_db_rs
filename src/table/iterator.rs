use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;

pub trait Iter {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    fn seek(&mut self, target: &Slice);
    fn next(&mut self);
    fn prev(&mut self);
    fn key(&self) -> Slice;
    fn value(&self) -> Slice;
    fn status(&self) -> Status;
}

pub struct EmptyIterator {
    status: Status,
}

impl Iter for EmptyIterator {
    fn valid(&self) -> bool {
        false
    }
    fn seek_to_first(&mut self) {}
    fn seek_to_last(&mut self) {}
    fn seek(&mut self, target: &Slice) {}

    fn next(&mut self) {}

    fn prev(&mut self) {}

    fn key(&self) -> Slice {
        Slice::new_from_str("")
    }

    fn value(&self) -> Slice {
        Slice::new_from_str("")
    }

    fn status(&self) -> Status {
        self.status.clone()
    }
}

pub fn new_empty_iterator() -> EmptyIterator {
    EmptyIterator {
        status: Status::ok(),
    }
}
pub fn new_error_iterator(status: Status) -> EmptyIterator {
    EmptyIterator { status }
}
