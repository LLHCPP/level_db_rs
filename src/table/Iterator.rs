use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;

pub trait Iter {
    fn valid(&self) -> bool;
    fn seek_to_first(&self);
    fn seek_to_last(&self);
    fn seek(&self, target: &Slice);
    fn next(&self);
    fn prev(&self);
    fn key(&self) -> Slice;
    fn value(&self) -> Slice;
    fn status(&self) -> Status;
}

struct EmptyIterator {
    status: Status,
}

impl Iter for EmptyIterator {
    fn valid(&self) -> bool {
        false
    }
    fn seek_to_first(&self) {}
    fn seek_to_last(&self) {}
    fn seek(&self,target: &Slice) {}

    fn next(&self) {}

    fn prev(&self) {}

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
