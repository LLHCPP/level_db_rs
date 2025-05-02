use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;

pub trait Iterator {
    fn valid() -> bool;
    fn seek_to_first();
    fn seek_to_last();
    fn seek(target: &Slice);
    fn next();
    fn prev();
    fn key(&self) -> Slice;
    fn value(&self) -> Slice;
    fn status(&self) -> Status;
}

struct EmptyIterator {
    status: Status,
}

impl Iterator for EmptyIterator {
    fn valid() -> bool {
        false
    }
    fn seek_to_first() {}
    fn seek_to_last() {}
    fn seek(target: &Slice) {}

    fn next() {}

    fn prev() {}

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

fn new_empty_iterator() -> EmptyIterator {
    EmptyIterator {
        status: Status::ok(),
    }
}
fn new_error_iterator(status: Status) -> EmptyIterator {
    EmptyIterator { status }
}
