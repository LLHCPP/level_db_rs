use crate::db::internal_key::InternalKey;
use crate::db::internal_key_comparator::InternalKeyComparator;
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::arena::Arena;
use crossbeam_skiplist::SkipMap;

struct KeyComparator {
    comparator: InternalKeyComparator,
}

struct MemTable {
    table: SkipMap<InternalKey, Slice>,
    arena: Arena,
}

impl MemTable {
    fn Add(&self, seq: u64, key: &Slice, value: &Slice) {}

    fn Get(&self, key: &Slice) -> Result<Slice, Status> {
        Ok(Slice::new_empty())
    }
}
