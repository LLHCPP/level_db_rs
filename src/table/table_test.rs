use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::bytewise_comparator_impl::byte_wise_comparator;
use crate::util::comparator::Comparator;
use crate::util::random_access_file::{Limiter, RandomAccessFile};
use crate::util::writable_file::WritableFile;
use bytes::{BufMut, BytesMut};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use crate::obj::options::Options;
use crate::table::iterator::Iter;
use crate::util::env::Env;

fn reverse(key : &Slice) -> Slice {
    let str = key.to_string();
    let mut str2 = BytesMut::new();
    for i in str.bytes().rev() {
        str2.put_u8(i);
    }
   Slice::new_bytes_mut(str2)
}

struct ReverseKeyComparator {

}
impl Comparator for  ReverseKeyComparator {
    fn compare(&self, a: &Slice, b: &Slice) -> Ordering {
        return byte_wise_comparator().compare(&reverse(a), &reverse(b))
    }

    fn name(&self) -> &'static str {
        return "leveldb.ReverseBytewiseComparator";
    }

    fn find_shortest_separator(&self, start: &mut BytesMut, limit: &Slice) {
        let s = reverse(&Slice::new_from_mut(start));
        let l =  reverse(limit);
        let mut s = BytesMut::from(s.data());
        byte_wise_comparator().find_shortest_separator(&mut s, &l);
        *start = s;
    }

    fn find_short_successor(&self, key: &mut BytesMut) {
        let s = reverse(&Slice::new_from_mut(key));
         let mut s = BytesMut::from(s.data());
         byte_wise_comparator().find_short_successor(&mut s);
         let res = reverse( &Slice::new_from_mut(&s));
        *key = BytesMut::from(res.data());
    }
}

static REVERSE_KEY_COMPARATOR: OnceLock<Arc<dyn Comparator>> = OnceLock::new();
fn get_reverse_key_comparator() -> &'static Arc<dyn Comparator> {
    REVERSE_KEY_COMPARATOR.get_or_init(|| Arc::new(ReverseKeyComparator {}))
}

fn increment(cmp: Arc<dyn  Comparator>, key: &mut BytesMut) {
    if Arc::ptr_eq(&cmp, &byte_wise_comparator()) {
        key.put_u8(b'\0');
    } else {
        assert!(Arc::ptr_eq(&cmp, &get_reverse_key_comparator()));
        let rev = reverse(&Slice::new_from_mut(key));
        let mut rev = BytesMut::from( rev.data());
        rev.put_u8(b'\0');
        let res = reverse( &Slice::new_from_mut(&rev));
         *key = BytesMut::from(res.data());
    }
}

struct STLLessThan {
     cmp: Arc<dyn  Comparator>,
}
impl STLLessThan  {
    fn new(cmp: Arc<dyn  Comparator>) -> STLLessThan {
        STLLessThan {
            cmp,
        }
    }
    fn new_bytewise() -> STLLessThan {
        STLLessThan {
            cmp: byte_wise_comparator(),
        }
    }
    fn compare( &self, a: &Slice, b: &Slice) -> bool {
        self.cmp.compare(a, b) == Ordering::Less
    }
}

struct StringSink {
    contents_ : BytesMut,
}

impl WritableFile  for StringSink{
    fn new<P: AsRef<Path>>(filename: P, truncate: bool) -> std::io::Result<Self>
    where
        Self: Sized
    {
        Ok( StringSink {
             contents_: BytesMut::new(),
        })
    }

    fn append(&mut self, data: &Slice) -> Status {
       self.contents_.put(data.data());
        Status::ok()
    }

    fn flush(&mut self) -> Status {
        Status::ok()
    }

    fn sync(&mut self) -> Status {
        Status::ok()
    }
}


#[derive(Debug)]
struct StringSource {
    contents_ : BytesMut,
}

impl StringSource  {
    fn new_contents(s: BytesMut) -> StringSource {
        StringSource {
            contents_: s,
        }
    }
}

impl RandomAccessFile for  StringSource{
    fn new<P: AsRef<Path>>(filename: P, limiter: Arc<Limiter>) -> std::io::Result<Self>
    where
        Self: Sized
    {
        Ok( StringSource {
             contents_: BytesMut::new(),
        })
    }

    fn read(&mut self, offset: u64, mut n: usize, scratch: Option<&mut [u8]>) -> Result<Slice, Status> {
        if offset >= self.contents_.len() as u64 {
          return Err(Status::invalid_argument("invalid offset", None));
        }
        if offset + n as u64 > self.contents_.len() as u64 {
           n = (self.contents_.len() as u64 - offset as u64) as usize;
        }
        match scratch {
            Some(scratch) => {
                scratch[..n].copy_from_slice(&self.contents_[offset as usize..offset as usize + n]);
                Ok(Slice::new_from_ptr(scratch))
            },
            None => {
                let mut buf = BytesMut::new();
                let temp = &self.contents_[offset as usize..(offset as usize + n)];
                buf.put(temp);
                Ok(Slice::new_bytes_mut(buf))
            }
        }

    }
}

type KVMap = BTreeMap<Slice, Slice>;
trait Constructor{
    fn finish_impl<E:Env>(&self, option: &Options<E>, data: &KVMap)  -> Status;
    fn new_iterator(&self) -> Box<dyn Iter>;
/*    fn db() -> D*/
}


