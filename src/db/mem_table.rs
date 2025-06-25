use crate::db::internal_key_comparator::InternalKeyComparator;
use crate::obj::slice::Slice;
use crossbeam_skiplist::SkipMap;

struct KeyComparator {
    comparator: InternalKeyComparator,
}

struct MemTable {
    table: SkipMap<Slice, Slice>,
}
