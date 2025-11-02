use crate::obj::byte_buffer::ByteBuffer;
use crate::obj::options::{Options, ReadOptions};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::block::Block;
use crate::table::filter_block::FilterBlockReader;
use crate::table::format::{read_block, BlockHandle, Footer, K_ENCODED_LENGTH};
use crate::table::iterator::{new_error_iterator, Iter};
use crate::table::two_level_iterator::TwoLevelIterator;
use crate::util::bytewise_comparator_impl::byte_wise_comparator;
use crate::util::coding::encode_fixed64;
use crate::util::env::Env;
use crate::util::filter_policy::FilterPolicy;
use crate::util::random_access_file::RandomAccessFile;
use std::any::Any;
use std::sync::{Arc, Mutex};
pub type HandleResult = Box<dyn Fn(Box<dyn Any>, &Slice, &Slice)>;
struct Rep<E>
where
    E: Env,
{
    options: Arc<Options<E>>,
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
| Meta_index Block     |获取 filter block（如果存在）在table file的offset和size
+---------------------+
| Index Block         |存储 Data Block 的索引，读取时候，由Index Block索引定位数据属于哪个block，再由block内部的restart_index_定位具体位置
+---------------------+
| Footer (固定长度)     | 记录 meta_index block 和 index block 的位置
+---------------------+*/
pub struct Table<E>
where
    E: Env,
{
    rep: Arc<Mutex<Rep<E>>>,
}

impl<'a, E> Table<E>
where
    E: Env + 'static,
{
    fn new(rep: Arc<Mutex<Rep<E>>>) -> Table<E> {
        Table { rep }
    }
    pub fn open(
        options: Arc<Options<E>>,
        file: Arc<Mutex<dyn RandomAccessFile>>,
        size: u64,
    ) -> Result<Arc<Table<E>>, Status> {
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
        let s = footer.decode_from(&mut data);
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

    fn block_reader(
        table: &Table<E>,
        read_options: &ReadOptions,
        index_value: &Slice,
    ) -> Box<dyn Iter> {
        let rep = table.rep.lock().unwrap();
        let block_cache = &rep.options.block_cache;
        let mut handle = BlockHandle::new();
        let mut input = index_value.clone();
        let mut status = handle.decode_from(&mut input);
        let mut block = None;
        if status.is_ok() {
            match block_cache {
                Some(ref cache) => {
                    let mut cache_key_buffer = [0u8; 16];
                    encode_fixed64(cache_key_buffer.as_mut_slice(), rep.cache_id);
                    encode_fixed64(&mut cache_key_buffer.as_mut_slice()[8..], handle.offset());
                    let key =
                        Slice::new_from_buff(ByteBuffer::from_ptr(cache_key_buffer.as_slice()));
                    let cache_handle = cache.get(&key);
                    if cache_handle.is_some() {
                        let cache_handle = cache_handle.unwrap().value().clone();
                        block = Some(cache_handle);
                    } else {
                        let s = read_block(rep.file.clone(), read_options, &handle);
                        if s.is_ok() {
                            let contents = s.unwrap();
                            let need_cache = contents.cachable && read_options.fill_cache;
                            let cache_block = Block::new(contents);
                            block = Some(cache_block.clone());
                            if need_cache {
                                let _ = cache.insert(&key, cache_block);
                            };
                        } else {
                            status = s.err().unwrap();
                        }
                    }
                }
                _ => {
                    let s = read_block(rep.file.clone(), read_options, &handle);
                    if s.is_ok() {
                        block = Some(Block::new(s.unwrap()));
                    } else {
                        status = s.err().unwrap();
                    }
                }
            }
        }
        match block {
            Some(ref block) => {
                let iter = block.new_iterator(rep.options.comparator.clone());
                iter
            }
            _ => {
                let err_iter: Box<dyn Iter> = Box::new(new_error_iterator(status));
                err_iter
            }
        }
    }

    fn new_iterator(&'a self, options: ReadOptions) -> Box<dyn Iter + 'a> {
        let rep = self.rep.lock().unwrap();
        let index_block_iter = rep.index_block.new_iterator(rep.options.comparator.clone());
        let block_function = Box::new(Table::<E>::block_reader);

        let res: Box<dyn Iter> = Box::new(TwoLevelIterator::<'a, E>::new(
            index_block_iter,
            block_function,
            self,
            options,
        ));
        res
    }

    pub fn internal_get(
        &self,
        options: &ReadOptions,
        key: &Slice,
        arg: Box<dyn Any>,
        handle_result: HandleResult,
    ) -> Status {
        let mut s = Status::ok();
        let rep = self.rep.lock().unwrap();
        let mut iiter = rep.index_block.new_iterator(rep.options.comparator.clone());
        iiter.seek(key);
        if iiter.valid() {
            let mut handle_value = iiter.value();
            let filter = rep.filter.clone();
            let mut handle = BlockHandle::new();
            if filter.is_some()
                && handle.decode_from(&mut handle_value).is_ok()
                && !filter
                    .map(|filter| filter.key_may_match(handle.offset(), key))
                    .unwrap_or(true)
            {
            } else {
                let mut block_iter = Self::block_reader(self, options, &iiter.value());
                block_iter.seek(key);
                if block_iter.valid() {
                    handle_result(arg, &block_iter.key(), &block_iter.value());
                }
                s = block_iter.status();
            }
        }
        if s.is_ok() {
            s = iiter.status();
        }
        s
    }

    fn approximate_offset_of(&self, key: &Slice) -> u64 {
        let rep = self.rep.lock().unwrap();
        let mut index_iter = rep.index_block.new_iterator(rep.options.comparator.clone());
        index_iter.seek(key);
        let mut result = 0u64;
        if index_iter.valid() {
            let mut handle = BlockHandle::new();
            let mut input = index_iter.value();
            let s = handle.decode_from(&mut input);
            if s.is_ok() {
                result = handle.offset();
            } else {
                result = rep.meta_index_handle.offset();
            }
        } else {
            result = rep.meta_index_handle.offset();
        }
        result
    }
}
