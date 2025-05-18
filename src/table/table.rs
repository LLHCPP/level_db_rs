use crate::obj::options::{Options, ReadOptions};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::block::Block;
use crate::table::filter_block::FilterBlockReader;
use crate::table::format::{read_block, BlockHandle, Footer, K_ENCODED_LENGTH};
use crate::util::bytewise_comparator_impl::byte_wise_comparator;
use crate::util::comparator::Comparator;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::hash::LocalHash;
use crate::util::random_access_file::RandomAccessFile;
use bytes::BytesMut;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

struct Rep<C, E, K, V>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
{
    options: Arc<Options<C, E, K, V>>,
    status: Status,
    file: Arc<Mutex<dyn RandomAccessFile>>,
    cache_id: u64,
    filter: Option<Arc<FilterBlockReader>>,
    meta_index_handle: BlockHandle,
    index_block: Arc<Block>,
}

/*+---------------------+
| Data Block 1        |
+---------------------+
| Data Block 2        |
+---------------------+
| ...                 |
+---------------------+
| Data Block N        |
+---------------------+
| Filter Block (可选) |
+---------------------+
| Metaindex Block     |获取 filter block（如果存在）在table file的offset和size
+---------------------+
| Index Block         |存储 Data Block 的索引，读取时候，由Index Block索引定位数据属于哪个block，再由block内部的restart_index_定位具体位置
+---------------------+
| Footer (固定长度)     | 记录 metaindex block 和 index block 的位置
+---------------------+*/
pub struct Table<C, E, K, V>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
{
    rep: Arc<Mutex<Rep<C, E, K, V>>>,
}

impl<'a, C, E, K, V> Table<C, E, K, V>
where
    C: Comparator,
    E: Env,
    K: Hash + Eq + PartialEq + Default + Clone + LocalHash,
    V: Clone,
{
    fn new(rep: Arc<Mutex<Rep<C, E, K, V>>>) -> Table<C, E, K, V> {
        Table { rep }
    }
    fn open(
        &mut self,
        options: Arc<Options<C, E, K, V>>,
        file: Arc<Mutex<dyn RandomAccessFile>>,
        size: u64,
    ) -> Result<Arc<Table<C, E, K, V>>, Status> {
        if size < K_ENCODED_LENGTH {
            return Err(Status::corruption(
                "file is too short to be an ss_table",
                None,
            ));
        }
        let mut footer_space = [0; K_ENCODED_LENGTH as usize];
        let mut read_file = file.lock().unwrap();
        let s = read_file.read(
            size - K_ENCODED_LENGTH,
            K_ENCODED_LENGTH as usize,
            Some(&mut footer_space),
        );
        drop(read_file);
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
            meta_index_handle: footer.meta_index_handle().clone(),
            index_block: Arc::new(index_block),
        };
        let table = Arc::new(Table::new(Arc::new(Mutex::new(rep))));
        Ok(table)
    }

    fn read_filter(&mut self, filter_handle_value: &mut Slice) {
        let mut filter_handle = BlockHandle::new();
        if !filter_handle.decode_from(filter_handle_value).is_ok() {
            return;
        }

        let mut opt = ReadOptions::new();
        let mut rep = self.rep.lock().unwrap();
        if rep.options.paranoid_checks {
            opt.verify_checksums = true;
        }
        let s = read_block(rep.file.clone(), &opt, &filter_handle);
        if s.is_err() {
            return;
        }
        let s = s.unwrap();
        let filter_policy = rep.options.filter_policy.clone();
        let filter_reader = FilterBlockReader::new(filter_policy.unwrap(), s.data);
        rep.filter = Some(Arc::new(filter_reader))
    }

    fn read_meta(&mut self, footer: &Footer) {
        let rep = self.rep.lock().unwrap();
        if rep.options.filter_policy.is_none() {
            return;
        }
        let mut opt = ReadOptions::new();
        if rep.options.paranoid_checks {
            opt.verify_checksums = true;
        }
        let contents = read_block(rep.file.clone(), &opt, &footer.meta_index_handle());
        if contents.is_err() {
            return;
        }
        let contents = contents.unwrap();
        let meta_block = Block::new(contents);
        let mut iter = meta_block.new_iterator(byte_wise_comparator());
        let name = match rep.options.filter_policy {
            Some(ref filter_policy) => filter_policy.name(),
            None => "",
        };
        drop(rep);
        let key = Slice::new_from_string(format!("filter.{}", name));
        iter.seek(&key);
        if iter.valid() && iter.key() == key {
            self.read_filter(&mut iter.value());
        }
    }
}
