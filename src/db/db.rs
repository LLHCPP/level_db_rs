use crate::db::mem_table::MemTable;
use crate::db::table_cache::TableCache;
use crate::db::write_options::WriteOptions;
use crate::obj::options::{Options, ReadOptions};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::iterator::Iter;
use crate::util::comparator::Comparator;
use crate::util::env::{Env, FileLock};
use crate::util::filter_policy::FilterPolicy;
use crate::util::writable_file::WritableFile;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
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
    fn open(options: Arc<Options<E>>, name: &String) -> Result<Arc<Self>, Status>
    where
        Self: Sized;

    fn put(&self, options: &WriteOptions, key: &Slice, value: &Slice) -> Status;
    fn delete(&self, options: &WriteOptions, key: &Slice) -> Status;

    fn get(&self, options: &ReadOptions, key: &Slice) -> Result<Slice, Status>;

    fn new_iterator(&self, options: &ReadOptions) -> Arc<dyn Iter>;
    

    fn get_property(&self, property: &Slice, value: &mut String) -> bool;

    fn get_approximate_sizes(&self, range: &Range, n: i64, sizes: &mut u64);

    fn compact_range(&self, begin: &Slice, end: &Slice);
}

const K_NUM_NON_TABLE_CACHE_FILES: usize = 10;
struct DBImpl<E>
where
    E: Env,
{
    internal_comparator_: Arc<dyn Comparator>,
    internal_filter_policy_: Option<Arc<dyn FilterPolicy>>,
    options_: Arc<Options<E>>,
    dbname_: String,
    table_cache_: Arc<TableCache<E>>,
    db_lock: Option<Arc<FileLock>>,
    shutting_down: AtomicBool,
    mem_: Option<Arc<MemTable>>,
    imm_: Option<Arc<MemTable>>,
    logfile_: Option<Arc<dyn WritableFile>>,
    logfile_number_: u64,
}

fn table_cache_size(max_open_files: usize) -> usize {
    // Reserve ten files or so for other uses and give the rest to TableCache.
    max_open_files - K_NUM_NON_TABLE_CACHE_FILES
}

impl<E> DBImpl<E>
where
    E: Env + 'static,
{
    fn new(options: Arc<Options<E>>, dbname: String) -> DBImpl<E> {
        DBImpl {
            internal_comparator_: options.comparator.clone(),
            internal_filter_policy_: options.filter_policy.clone(),
            options_: options.clone(),
            dbname_: dbname.clone(),
            table_cache_: Arc::new(TableCache::new(
                dbname.clone(),
                options.clone(),
                NonZeroUsize::try_from(table_cache_size(options.max_file_size)).unwrap(),
            )),
            db_lock: None,
            shutting_down: Default::default(),
            mem_: None,
            imm_: None,
            logfile_: None,
            logfile_number_: 0,
        }
    }
}

impl<E> DB<E> for DBImpl<E>
where
    E: Env,
{
    fn open(options: Arc<Options<E>>, name: &String) -> Result<Arc<Self>, Status>
    where
        Self: Sized,
    {
        todo!()
    }

    fn put(&self, options: &WriteOptions, key: &Slice, value: &Slice) -> Status {
        todo!()
    }

    fn delete(&self, options: &WriteOptions, key: &Slice) -> Status {
        todo!()
    }

    fn get(&self, options: &ReadOptions, key: &Slice) -> Result<Slice, Status> {
        todo!()
    }

    fn new_iterator(&self, options: &ReadOptions) -> Arc<dyn Iter> {
        todo!()
    }
    
    

    fn get_property(&self, property: &Slice, value: &mut String) -> bool {
        todo!()
    }

    fn get_approximate_sizes(&self, range: &Range, n: i64, sizes: &mut u64) {
        todo!()
    }

    fn compact_range(&self, begin: &Slice, end: &Slice) {
        todo!()
    }
}
