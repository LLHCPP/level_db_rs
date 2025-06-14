use crate::db::snap_shot::Snapshot;
use crate::db::write_options::WriteOptions;
use crate::obj::options::{Options, ReadOptions};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::iterator::Iter;
use crate::util::env::Env;
use std::sync::Arc;

struct Range {
    start: Slice,
    limit: Slice,
}

impl Range {
    fn new(start: Slice, limit: Slice) -> Self {
        Range { start, limit }
    }
}

trait DB<E>
where
    E: Env,
{
    fn open(options: Arc<Options<E>>, name: &String) -> Result<Arc<dyn DB<E>>, Status>
    where
        Self: Sized;

    fn put(&self, options: &WriteOptions, key: &Slice, value: &Slice) -> Status;
    fn delete(&self, options: &WriteOptions, key: &Slice) -> Status;

    fn get(&self, options: &ReadOptions, key: &Slice) -> Result<Slice, Status>;

    fn new_iterator(&self, options: &ReadOptions) -> Arc<dyn Iter>;

    fn get_snapshot(&self) -> Arc<dyn Snapshot>;
    fn release_snapshot(&self, snapshot: Arc<dyn Snapshot>);

    fn get_property(&self, property: &Slice, value: &mut String) -> bool;

    fn get_approximate_sizes(&self, range: &Range, n: i64, sizes: &mut u64);

    fn compact_range(&self, begin: &Slice, end: &Slice);
}
