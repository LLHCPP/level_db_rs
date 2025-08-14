use crate::db::file_name::{sst_table_file_name, table_file_name};
use crate::obj::options::{Options, ReadOptions};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::table::{HandleResult, Table};
use crate::util::cache::ShardedLRUCache;
use crate::util::coding::encode_fixed64;
use crate::util::env::Env;
use crate::util::random_access_file::RandomAccessFile;
use std::any::Any;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

struct TableAndFile<E>
where
    E: Env,
{
    random_access_file: Arc<Mutex<dyn RandomAccessFile>>,
    table: Arc<Table<E>>,
}

impl<'a, E> Clone for TableAndFile<E>
where
    E: Env,
{
    fn clone(&self) -> Self {
        TableAndFile {
            random_access_file: Arc::clone(&self.random_access_file),
            table: Arc::clone(&self.table),
        }
    }
}

pub struct TableCache<E>
where
    E: Env,
{
    env_: Arc<E>,
    db_name: String,
    options: Arc<Options<E>>,
    cache_: ShardedLRUCache<Slice, TableAndFile<E>>,
}

impl<'a, E> TableCache<E>
where
    E: Env + 'static,
{
    fn new(db_name: String, options: Arc<Options<E>>, entries: NonZeroUsize) -> Self {
        TableCache {
            env_: options.env.clone(),
            db_name,
            options,
            cache_: ShardedLRUCache::<Slice, TableAndFile<E>>::new(entries),
        }
    }

    fn find_table(
        &mut self,
        file_number: u64,
        file_size: usize,
    ) -> Result<TableAndFile<E>, Status> {
        let mut buf = [0; size_of::<u64>()];
        encode_fixed64(&mut buf, file_number);
        let key = Slice::new_from_ptr(buf.as_ref());
        match self.cache_.get(&key) {
            Some(table_and_file) => {
                let res = table_and_file.value().clone();
                Ok(res)
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
                let s = Table::open(self.options.clone(), file.clone()?, file_size as u64)?;
                let tf = TableAndFile {
                    random_access_file: file?,
                    table: s,
                };
                self.cache_.insert(&key, tf.clone());
                Ok(tf)
            }
        }
    }

    fn get(
        &mut self,
        options: &ReadOptions,
        file_number: u64,
        file_size: u64,
        K: &Slice,
        arg: Box<dyn Any>,
        handle_result: HandleResult,
    ) -> Result<(), Status> {
        let table_file = self.find_table(file_number, file_size as usize)?;
        let table = table_file.table;
        let s = table.internal_get(options, K, arg, handle_result);
        Ok(())
    }

    fn evict(&mut self, file_number: u64) {
        let mut buf = [0; size_of::<u64>()];
        encode_fixed64(&mut buf, file_number);
        let key = Slice::new_from_ptr(buf.as_ref());
        self.cache_.erase(&key);
    }
}
