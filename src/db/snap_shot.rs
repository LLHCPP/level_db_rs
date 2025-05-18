use std::ptr::null_mut;

pub trait Snapshot {}

struct SnapshotImpl {
    prev_: *mut SnapshotImpl,
    next_: *mut SnapshotImpl,
    sequence_number_: u64,
}
impl Snapshot for SnapshotImpl {}

impl SnapshotImpl {
    fn new(sequence_number: u64) -> SnapshotImpl {
        SnapshotImpl {
            prev_: null_mut(),
            next_: null_mut(),
            sequence_number_: sequence_number,
        }
    }
}

struct SnapshotList {
    head: SnapshotImpl,
}

impl SnapshotList {
    fn new() -> SnapshotList {
        let mut head = SnapshotImpl::new(0);
        head.prev_ = &mut head;
        head.next_ = &mut head;
        SnapshotList { head }
    }
    fn empty(&self) -> bool {
        self.head.next_ as *const SnapshotImpl == &self.head
    }
    fn oldest(&self) -> *mut SnapshotImpl {
        assert!(!self.empty());
        self.head.next_
    }

    fn newest(&self) -> *mut SnapshotImpl {
        assert!(!self.empty());
        self.head.prev_
    }

    fn new_node(&mut self, sequence_number: u64) -> *mut SnapshotImpl {
        unsafe {
            assert!(self.empty() || (*self.newest()).sequence_number_ <= sequence_number);
        }
        let snapshot = Box::new(SnapshotImpl::new(sequence_number));
        let snapshot = Box::into_raw(snapshot);
        unsafe {
            (*snapshot).next_ = &mut self.head;
        }
        unsafe {
            (*snapshot).prev_ = self.head.prev_;
        }
        unsafe {
            (*(*snapshot).prev_).next_ = snapshot;
        }
        unsafe {
            (*(*snapshot).next_).prev_ = snapshot;
        }
        snapshot
    }

    fn delete(&mut self, snapshot: *mut SnapshotImpl) {
        unsafe {
            (*(*snapshot).prev_).next_ = (*snapshot).next_;
        }
        unsafe {
            (*(*snapshot).next_).prev_ = (*snapshot).prev_;
        }
        unsafe {
            let _ = Box::from_raw(snapshot);
        }
    }
}
