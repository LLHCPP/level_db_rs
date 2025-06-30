use crate::db::internal_key::InternalKey;
use crate::db::internal_key_comparator::{InternalKeyComparator, ValueType};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::arena::Arena;
use crate::util::coding::{encode_fixed64, encode_varint32, encode_varint64, varint_length};
use crossbeam_skiplist::SkipMap;

struct KeyComparator {
    comparator: InternalKeyComparator,
}

struct MemTable {
    table: SkipMap<InternalKey, Slice>,
    arena: Arena,
}

impl MemTable {
    fn add(&self, seq: u64, value_type: ValueType, key: &Slice, value: &Slice) {
        let key_size = key.len();
        let val_size = value.len();
        let internal_key_size = key_size + 8;
        let encode_len = varint_length(internal_key_size as u64) as usize
            + internal_key_size
            + varint_length(val_size as u64) as usize
            + val_size;
       let buf =  self.arena.alloc_array::<u8>(encode_len);
        let mut p = encode_varint32(buf, internal_key_size as u32);
        p.copy_from_slice(key.data());
        p = &mut p[key_size..];
        encode_fixed64(p, (seq <<8 | value_type as u64));
        p = &mut p[8..];
        p = encode_varint32(p, val_size as u32);
        p.copy_from_slice(value.data());
        self.table.insert(InternalKey::new(key.clone(), seq, value_type), Slice::new_from_ptr(buf));
    }

    fn Get(&self, key: &Slice) -> Result<Slice, Status> {
        Ok(Slice::new_empty())
    }
}
