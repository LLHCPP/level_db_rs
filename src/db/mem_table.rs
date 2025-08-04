use crate::db::internal_key_comparator::{InternalKeyComparator, ValueType};
use crate::obj::slice::Slice;
use crate::obj::status_rs::Status;
use crate::util::arena::Arena;
use crate::util::coding::{encode_fixed64, encode_varint32, get_varint32ptr, varint_length};
use crossbeam_skiplist::SkipMap;

struct KeyComparator {
    comparator: InternalKeyComparator,
}

pub struct MemTable {
    table: SkipMap<Slice, (u64, ValueType, Slice)>,
    arena: Arena,
}

fn get_length_prefixed_slice(data: &[u8]) -> Slice {
    let mut len = 0;
    let p = get_varint32ptr(data, &mut len).unwrap();
    Slice::new_from_ptr(&p[..(len as usize)])
}

impl MemTable {
    fn add(&self, seq: u64, value_type: ValueType, key: &Slice, value: Option<&Slice>) {
        match value_type {
            ValueType::KTypeDeletion => {
                self.table
                    .insert(key.clone(), (seq, value_type, Slice::new_from_empty()));
            }
            ValueType::KTypeValue => match value {
                None => {}
                Some(value) => {
                    let key_size = key.len();
                    let val_size = value.len();
                    let internal_key_size = key_size + 8;
                    let encode_len = varint_length(internal_key_size as u64) as usize
                        + internal_key_size
                        + varint_length(val_size as u64) as usize
                        + val_size;
                    let buf = self.arena.alloc_array::<u8>(encode_len);
                    let mut p = encode_varint32(buf, internal_key_size as u32);
                    p.copy_from_slice(key.data());
                    p = &mut p[key_size..];
                    encode_fixed64(p, seq << 8 | value_type as u64);
                    p = &mut p[8..];
                    p = encode_varint32(p, val_size as u32);
                    p.copy_from_slice(value.data());
                    self.table
                        .insert(key.clone(), (seq, value_type, Slice::new_from_ptr(buf)));
                }
            },
        }
    }

    fn get(&self, key: &Slice) -> Result<Slice, Status> {
        match self.table.get(key) {
            Some(v) => {
                let (_seq, value_type, value) = v.value();
                match value_type {
                    ValueType::KTypeDeletion => Err(Status::not_found("not found", None)),
                    _ => {
                        let data = value.data();
                        let mut key_len = 0;
                        let key_ptr = get_varint32ptr(data, &mut key_len).unwrap();
                        let v = get_length_prefixed_slice(&key_ptr[..key_len as usize]);
                        Ok(v)
                    }
                }
            }
            None => Err(Status::not_found("not found", None)),
        }
    }
}
