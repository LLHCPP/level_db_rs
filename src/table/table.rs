use crate::obj::byte_buffer::ByteBuffer;
use crate::obj::options::{Options, ReadOptions};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::block::Block;
use crate::table::filter_block::FilterBlockReader;
use crate::table::format::{read_block, BlockContents, BlockHandle, Footer, K_ENCODED_LENGTH};
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use crate::util::random_access_file::RandomAccessFile;
use bytes::BytesMut;
use intrusive_collections::rbtree::Color::Black;
use std::hash::Hash;
use std::net::Shutdown::Read;
use std::sync::{Arc, Mutex};

struct Rep<'a, C, E, K, V, F>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
    F: FilterPolicy,
{
    options: Arc<Options<C, E, K, V, F>>,
    status: Status,
    file: Arc<Mutex<dyn RandomAccessFile>>,
    cache_id: u64,
    filter: Option<Arc<FilterBlockReader<'a, F>>>,
    filter_data: Option<BytesMut>,
    meta_index_handle: BlockHandle,
    index_block: Arc<Block>,
}

pub struct Table<'a, C, E, K, V, F>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
    F: FilterPolicy,
{
    rep: Arc<Rep<'a, C, E, K, V, F>>,
}

impl<'a, C, E, K, V, F> Table<'a, C, E, K, V, F>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
    F: FilterPolicy,
{
    fn new(rep: Arc<Rep<'a, C, E, K, V, F>>) -> Table<'a, C, E, K, V, F> {
        Table { rep }
    }
    fn open(
        &mut self,
        options: Arc<Options<C, E, K, V, F>>,
        mut file: Arc<Mutex<dyn RandomAccessFile>>,
        size: u64,
    ) -> Result<Arc<Table<'a, C, E, K, V, F>>, Status> {
        if size < K_ENCODED_LENGTH {
            return Err(Status::corruption(
                "file is too short to be an ss_table",
                None,
            ));
        }
        let mut footer_space = [0; K_ENCODED_LENGTH as usize];
        let mut readFile = file.lock().unwrap();
        let s = readFile.read(
            size - K_ENCODED_LENGTH,
            K_ENCODED_LENGTH as usize,
            Some(&mut footer_space),
        );
        drop(readFile);
        let mut data = s?;
        let mut footer = Footer::new();
        let mut s = footer.decode_from(&mut data);
        if !s.is_ok() {
            return Err(s);
        }
        let mut opt = ReadOptions::new();
        if options.paranoid_checks {
            opt.verify_checksums = true;
        }
        let s = read_block(file.clone(), &opt, footer.index_handle())?;
        let index_block = Block::new(s);
        let cache_id = match options.block_cache {
            Some(ref cache) => cache.new_id(),
            None => 0,
        };
        let rep = Rep {
            options: options.clone(),
            status: Status::ok(),
            file,
            cache_id,
            filter: None,
            filter_data: None,
            meta_index_handle: footer.meta_index_handle().clone(),
            index_block: Arc::new(index_block),
        };
        let table = Arc::new(Table::new(Arc::new(rep)));
        Ok(table)
    }
}
