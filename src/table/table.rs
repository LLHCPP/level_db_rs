use crate::obj::options::Options;
use crate::obj::status_rs::Status;
use crate::table::filter_block::FilterBlockReader;
use crate::table::format::{BlockHandle, K_ENCODED_LENGTH};
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use crate::util::random_access_file::RandomAccessFile;
use bytes::BytesMut;
use std::hash::Hash;
use std::sync::Arc;

struct Rep<'a, C, E, K, V, F, R>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
    F: FilterPolicy,
    R: RandomAccessFile,
{
    options: Options<C, E, K, V, F>,
    status: Status,
    file: Arc<R>,
    cache_id: u64,
    filter: Arc<FilterBlockReader<'a, F>>,
    filter_data: BytesMut,
    meta_index_handle: BlockHandle,
    index_handle: BlockHandle,
}

pub struct Table<'a, C, E, K, V, F, R>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
    F: FilterPolicy,
    R: RandomAccessFile,
{
    rep: Arc<Rep<'a, C, E, K, V, F, R>>,
}

impl<'a, C, E, K, V, F, R> Table<'a, C, E, K, V, F, R>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
    F: FilterPolicy,
    R: RandomAccessFile,
{
    fn new(rep: Arc<Rep<'a, C, E, K, V, F, R>>) -> Self<'a, C, E, K, V, F, R> {
        Table { rep }
    }
    fn open(
        &mut self,
        options: Arc<Options<C, E, K, V, F>>,
        mut file: Arc<dyn RandomAccessFile>,
        size: u64,
    ) -> Result<Arc<Table<'a, C, E, K, V, F, R>>, Status> {
        if size < K_ENCODED_LENGTH {
          return   Err(Status::corruption("file is too short to be an ss_table", None));
        }
        let mut footer_space = [0;K_ENCODED_LENGTH as usize];
        let s = file.read(size - K_ENCODED_LENGTH, K_ENCODED_LENGTH as usize, &mut footer_space);





    }
}
