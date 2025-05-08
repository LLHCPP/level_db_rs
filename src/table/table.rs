use crate::obj::options::Options;
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use std::hash::Hash;
use bytes::BytesMut;
use crate::obj::status_rs::Status;
use crate::table::filter_block::FilterBlockReader;
use crate::table::format::BlockHandle;
use crate::util::random_access_file::RandomAccessFile;

struct Rep<'a, C, E, K, V, F, R>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Default + Clone,
    F: FilterPolicy,
    R:RandomAccessFile
{
    options: Options<C, E, K, V, F>,
    status: Status,
    file:R,
    cache_id:u64,
    filter:FilterBlockReader<'a, F>,
    filter_data:BytesMut,
    meta_index_handle: BlockHandle,
    index_handle: BlockHandle,
}

#[derive(Debug, Clone, Default)]
pub struct Table {
    
    
}
