use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::table::iterator::Iter;

pub(crate) struct IteratorWrapper {
    pub(crate) iter: Option<Box<dyn Iter>>,
    valid: bool,
    key: Slice,
}

impl IteratorWrapper {
    pub fn new(iter: Option<Box<dyn Iter>>) -> IteratorWrapper {
        let mut res = IteratorWrapper {
            iter,
            valid: false,
            key: Slice::new_empty(),
        };
        res.update();
        res
    }


    pub fn update(&mut self) {
        self.valid = self.iter.as_ref().map(|iter| iter.valid()).unwrap_or(false);
        if self.valid {
            self.key = self.iter.as_ref().unwrap().key();
        }
    }
    pub fn set(&mut self, iter: Option<Box<dyn Iter>>) {
        match iter {
            Some(iter) => {
                self.iter = Some(iter);
                self.update();
            }
            None => {
                self.iter = None;
                self.valid = false;
            }
        }
    }
    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> Slice {
        assert!(self.valid());
        self.key.clone()
    }
    pub  fn value(&self) -> Slice {
        assert!(self.valid());
        self.iter.as_ref().unwrap().value()
    }

    pub fn status(&self) -> Status {
        assert!(self.iter.is_some());
        self.iter.as_ref().unwrap().status()
    }
    pub fn next(&mut self) {
        assert!(self.iter.is_some());
        self.iter.as_mut().map(|iter| iter.next());
    }
    pub fn prev(&mut self) {
        assert!(self.iter.is_some());
        self.iter.as_mut().map(|iter| iter.prev());
    }

    pub fn seek(&mut self, target: &Slice) {
        assert!(self.iter.is_some());
        self.iter.as_mut().map(|iter| iter.seek(target));
        self.update();
    }

    pub  fn seek_to_first(&mut self) {
        assert!(self.iter.is_some());
        self.iter.as_mut().map(|iter| iter.seek_to_first());
        self.update();
    }
    pub fn seek_to_last(&mut self) {
        assert!(self.iter.is_some());
        self.iter.as_mut().map(|iter| iter.seek_to_last());
        self.update();
    }
}
