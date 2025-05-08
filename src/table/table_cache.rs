use crate::obj::options::Options;
use crate::obj::slice::Slice;
use crate::table::table::Table;
use crate::util::cache::ShardedLRUCache;
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use crate::util::random_access_file::RandomAccessFile;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Debug, Default)]
struct TableAndFile<R>
where
    R: RandomAccessFile,
{
    random_access_file: Arc<R>,
    table: Arc<Table>,
}

impl<R: RandomAccessFile> Clone for TableAndFile<R> {
    fn clone(&self) -> Self {
        TableAndFile {
            random_access_file: Arc::clone(&self.random_access_file),
            table: Arc::clone(&self.table),
        }
    }
}

struct TableCache<C, E, K, V, F, R>
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
    cache_: ShardedLRUCache<Slice, TableAndFile<R>>,
}

impl<C, E, K, V, F, R> TableCache<C, E, K, V, F, R>
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
            cache_: ShardedLRUCache::<Slice, TableAndFile<R>>::new(entries),
        }
    }
    
    
    /*fn findtable(&self, key: &Slice)*/
    
}
