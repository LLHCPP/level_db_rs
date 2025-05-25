use crate::obj::options::ReadOptions;
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::iterator::Iter;
use crate::table::iterator_wrapper::IteratorWrapper;
use crate::table::table::Table;
use crate::util::env::Env;
use std::cmp::Ordering;
use bytes::BufMut;

type BlockFunction<E> = Box<dyn Fn(&mut Table<E>, &ReadOptions, &Slice) -> Box<dyn Iter>>;
struct TwoLevelIterator<E>
where
    E: Env,
{
    block_function: BlockFunction<E>,
    arg: Box<Table<E>>,
    read_options: ReadOptions,
    status: Status,
    index_iter_: IteratorWrapper,
    data_iter_: IteratorWrapper,
    data_block_handle_: Vec<u8>,
}

impl<E> TwoLevelIterator<E>
where
    E: Env,
{
    fn new(
        index_iter: Box<dyn Iter>,
        block_function: BlockFunction<E>,
        table: Box<Table<E>>,
        read_options: ReadOptions,
    ) -> TwoLevelIterator<E> {
        TwoLevelIterator {
            block_function,
            arg: table,
            read_options,
            status: Status::ok(),
            index_iter_: IteratorWrapper::new(Some(index_iter)),
            data_iter_: IteratorWrapper::new(None),
            data_block_handle_: Vec::new(),
        }
    }
    fn SaveError(&mut self, s: &Status) {
        if self.status.is_ok() && !s.is_ok() {
            self.status = s.clone();
        }
    }
    fn set_data_iterator(&mut self, data_iter: Option<Box<dyn Iter>>) {
        if self.data_iter_.iter.is_some() {
            self.SaveError(
                &data_iter
                    .as_ref()
                    .map(|iter| iter.status())
                    .unwrap_or(Status::ok()),
            );
        }
        self.data_iter_.set(data_iter)
    }
    fn init_data_block(&mut self) {
        if !self.index_iter_.valid() {
            self.set_data_iterator(None)
        } else {
            let handle = self.index_iter_.value();
            let data_block_handle_ = Slice::new_from_ptr(self.data_block_handle_.as_ref());
            if self.data_iter_.iter.is_some()
                && handle.compare(&data_block_handle_) == Ordering::Equal
            {
            } else {
                let iter = (self.block_function)(&mut self.arg, &self.read_options, &handle);
                self.data_block_handle_.clear();
                self.data_block_handle_.put_slice(handle.data());
                self.set_data_iterator(Some(iter))
            }
        }
    }

    fn skip_empty_data_blocks_forward(&mut self) {
        while self.data_iter_.iter.is_none() || !self.data_iter_.valid() {
            if !self.index_iter_.valid() {
                self.set_data_iterator(None);
                return;
            }
            self.index_iter_.next();
            self.init_data_block();
            if self.data_iter_.iter.is_some() {
                //block的迭代器定位到块的第一个kv
                self.data_iter_.seek_to_first();
            }
        }
    }

    fn skip_empty_data_blocks_backward(&mut self) {
        while self.data_iter_.iter.is_none() || !self.data_iter_.valid() {
            if !self.index_iter_.valid() {
                self.set_data_iterator(None);
                return;
            }
            self.index_iter_.prev();
            self.init_data_block();
            if self.data_iter_.iter.is_some() {
                self.data_iter_.seek_to_last();
            }
        }
    }
}
impl<E> Iter for TwoLevelIterator<E>
where
    E: Env,
{
    fn valid(&self) -> bool {
        self.data_iter_.valid()
    }

    fn seek_to_first(&mut self) {
        self.index_iter_.seek_to_first();
        self.init_data_block();
        if self.data_iter_.iter.is_some() {
            self.data_iter_.seek_to_first();
        }
        self.skip_empty_data_blocks_forward();
    }

    fn seek_to_last(&mut self) {
        self.index_iter_.seek_to_last();
        self.init_data_block();
        if self.data_iter_.iter.is_some() {
            self.data_iter_.seek_to_last();
        }
        self.skip_empty_data_blocks_backward();
    }

    fn seek(&mut self, target: &Slice) {
        self.index_iter_.seek(target);
        self.init_data_block();
        if self.data_iter_.iter.is_some() {
            self.data_iter_.seek(target);
        }
        self.skip_empty_data_blocks_forward();
    }

    fn next(&mut self) {
        assert!(self.valid());
        self.data_iter_.next();
        self.skip_empty_data_blocks_forward();
    }

    fn prev(&mut self) {
        assert!(self.valid());
        self.data_iter_.prev();
        self.skip_empty_data_blocks_backward();
    }

    fn key(&self) -> Slice {
        self.data_iter_.key()
    }

    fn value(&self) -> Slice {
        self.data_iter_.value()
    }

    fn status(&self) -> Status {
        if !self.index_iter_.status().is_ok() {
            self.index_iter_.status()
        } else if self.data_iter_.iter.is_some() && !self.data_iter_.status().is_ok() {
            return self.data_iter_.status();
        } else {
            return self.status.clone();
        }
    }
}
