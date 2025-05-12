use crate::db::file_name::{sst_table_file_name, table_file_name};
use crate::obj::options::Options;
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::table::Table;
use crate::util::cache::{LruRes, ShardedLRUCache};
use crate::util::coding::encode_fixed64;
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use crate::util::random_access_file::RandomAccessFile;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;

struct TableAndFile<'a, C, E, K, V, F, R>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
    F: FilterPolicy,
    R: RandomAccessFile,
{
    random_access_file: Arc<R>,
    table: Arc<Table<'a, C, E, K, V, F, R>>,
}

impl<'a, C, E, K, V, F, R> Clone for TableAndFile<'a, C, E, K, V, F, R>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
    F: FilterPolicy,
    R: RandomAccessFile,
{
    fn clone(&self) -> Self {
        TableAndFile {
            random_access_file: Arc::clone(&self.random_access_file),
            table: Arc::clone(&self.table),
        }
    }
}

struct TableCache<'a, C, E, K, V, F, R>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
    F: FilterPolicy,
    R: RandomAccessFile,
{
    env_: Arc<E>,
    db_name: String,
    options: Arc<Options<C, E, K, V, F>>,
    cache_: ShardedLRUCache<Slice, TableAndFile<'a, C, E, K, V, F, R>>,
}

impl<'a, C, E, K, V, F, R> TableCache<'a, C, E, K, V, F, R>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
    F: FilterPolicy,
    R: RandomAccessFile,
{
    fn new(db_name: String, options: Arc<Options<C, E, K, V, F>>, entries: NonZeroUsize) -> Self {
        TableCache {
            env_: options.env.clone(),
            db_name,
            options,
            cache_: ShardedLRUCache::<Slice, TableAndFile<'a, C, E, K, V, F, R>>::new(entries),
        }
    }

    fn find_table(
        &mut self,
        file_number: u64,
        file_size: usize,
    ) -> Result<TableAndFile<'a, C, E, K, V, F, R>, Status> {
        let mut buf = [0; size_of::<u64>()];
        encode_fixed64(&mut buf, file_number);
        let key = Slice::new_from_ptr(buf.as_ref());
        match self.cache_.get(&key) {
            Some(table_and_file) => {
                let res = table_and_file.value().clone();
                return Ok(res);
            }
            None => {
                let file_name = table_file_name(&self.db_name, file_number);
                let mut file = self.env_.new_random_access_file(file_name);
                if file.is_err() {
                    let old_filename = sst_table_file_name(&self.db_name, file_number);
                    file = self.env_.new_random_access_file(old_filename);
                    if file.is_err() {
                        return Err(file.err().unwrap());
                    }
                }
                /*let mut s = */
            }
        }

        Err(Status::not_found("", None))
    }
}
