use crate::obj::slice::Slice;
use crate::util::coding;
use crate::util::coding::decode_fixed64;
use crate::util::comparator::Comparator;
use bytes::{BufMut, BytesMut};
use std::cmp::{Ordering, PartialEq};

#[derive(Debug, PartialEq, PartialOrd, Clone, Eq)]
#[repr(u8)]
enum ValueType {
    KTypeDeletion = 0x0,
    KTypeValue = 0x1,
}
impl TryFrom<u8> for ValueType {
    type Error = &'static str;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x0 => Ok(ValueType::KTypeDeletion),
            0x1 => Ok(ValueType::KTypeValue),
            _ => Err("Invalid value for ValueType"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
struct ParsedInternalKey {
    user_key: Slice,
    sequence: u64,
    value_type: ValueType,
}

const K_MAX_SEQUENCE_NUMBER: u64 = ((0x1u64 << 56) - 1);
const K_VALUE_TYPE_FOR_SEEK: ValueType = ValueType::KTypeValue;
#[inline]
fn extract_user_key(internal_key: &Slice) -> Slice {
    debug_assert!(internal_key.len() >= 8);
    Slice::new_from_slice(internal_key, 0..internal_key.len() - 8)
}

#[inline(always)]
fn pack_sequence_and_type(seq: u64, t: ValueType) -> u64 {
    debug_assert!(seq <= K_MAX_SEQUENCE_NUMBER);
    debug_assert!(t <= K_VALUE_TYPE_FOR_SEEK);
    (seq << 8) | (t as u64)
}
#[inline]
fn append_internal_key(result: &mut BytesMut, key: ParsedInternalKey) {
    result.put_slice(&key.user_key.data());
    coding::put_fixed64(result, pack_sequence_and_type(key.sequence, key.value_type))
}

#[inline]
fn parse_internal_key(internal_key: &Slice, result: &mut ParsedInternalKey) -> bool {
    let n = internal_key.len();
    if n < 8 {
        return false;
    }
    let num = decode_fixed64(&internal_key.data()[n - 8..]);
    let c = (num as u8) & 0xff;
    result.sequence = num >> 8;
    result.value_type = ValueType::try_from(c).unwrap();
    result.user_key = internal_key.slice(n - 8);
    c <= ValueType::KTypeValue as u8
}

struct InternalKeyComparator<T: Comparator> {
    user_comparator_: T,
}
impl<T: Comparator> Comparator for InternalKeyComparator<T> {
    fn compare(&self, akey: &Slice, bkey: &Slice) -> Ordering {
        let mut r = self
            .user_comparator_
            .compare(&extract_user_key(akey), &extract_user_key(bkey));
        if r == Ordering::Equal {
            let a_num = coding::decode_fixed64(&akey.data()[akey.len() - 8..]);
            let b_num = coding::decode_fixed64(&bkey.data()[akey.len() - 8..]);
            if a_num > b_num {
                r = Ordering::Less;
            } else if a_num < b_num {
                r = Ordering::Greater;
            }
        }
        r
    }

    fn name(&self) -> &'static str {
        "leveldb.InternalKeyComparator"
    }

    fn find_shortest_separator(&self, start: &mut BytesMut, limit: &Slice) {
        let user_start = extract_user_key(&(Slice::new_from_mut(start)));
        let user_limit = extract_user_key(limit);
        let mut tmp = start.clone();
        self.user_comparator_
            .find_shortest_separator(&mut tmp, &user_limit);
        if tmp.len() < user_start.size()
            && self
                .user_comparator_
                .compare(&user_start, &Slice::new_from_mut(&tmp))
                == Ordering::Less
        {
            coding::put_fixed64(
                &mut tmp,
                pack_sequence_and_type(K_MAX_SEQUENCE_NUMBER, K_VALUE_TYPE_FOR_SEEK),
            );
            debug_assert!(
                self.compare(&(Slice::new_from_mut(start)), &(Slice::new_from_mut(&tmp)))
                    == Ordering::Less
            );
            debug_assert!(self.compare(&(Slice::new_from_mut(&tmp)), limit) == Ordering::Less);
            *start = tmp;
        }
    }

    fn find_short_successor(&self, key: &mut BytesMut) {
        let user_key = extract_user_key(&(Slice::new_from_mut(key)));
        let mut tmp = key.clone();
        self.user_comparator_.find_short_successor(&mut tmp);
        if (tmp.len() < user_key.len())
            && self
                .user_comparator_
                .compare(&user_key, &Slice::new_from_mut(&tmp))
                == Ordering::Less
        {
            coding::put_fixed64(
                &mut tmp,
                pack_sequence_and_type(K_MAX_SEQUENCE_NUMBER, K_VALUE_TYPE_FOR_SEEK),
            );
            debug_assert!(
                self.compare(&(Slice::new_from_mut(key)), &(Slice::new_from_mut(&tmp)))
                    == Ordering::Less
            );
            *key = tmp;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::internal_key_comparator::ValueType::KTypeValue;
    use crate::db::internal_key_comparator::{
        append_internal_key, parse_internal_key, InternalKeyComparator, ParsedInternalKey,
        ValueType,
    };
    use crate::obj::slice::Slice;
    use crate::util::bytewise_comparator_impl;
    use crate::util::comparator::Comparator;
    use bytes::BytesMut;
    fn i_key(user_key: &BytesMut, seq: u64, vt: ValueType) -> BytesMut {
        let mut encode = BytesMut::new();
        append_internal_key(
            &mut encode,
            ParsedInternalKey {
                user_key: Slice::new_from_mut(user_key),
                sequence: seq,
                value_type: vt,
            },
        );
        encode
    }
    fn shorten(s: &BytesMut, l: &BytesMut) -> BytesMut {
        let mut result = BytesMut::new();
        let mut internal_key_comparator = InternalKeyComparator {
            user_comparator_: bytewise_comparator_impl::BytewiseComparatorImpl {},
        };
        internal_key_comparator.find_shortest_separator(&mut result, &Slice::new_from_mut(l));
        result
    }
    fn shortsuccessor(s: &BytesMut) -> BytesMut {
        let mut result = BytesMut::new();
        let mut internal_key_comparator = InternalKeyComparator {
            user_comparator_: bytewise_comparator_impl::BytewiseComparatorImpl {},
        };
        internal_key_comparator.find_short_successor(&mut result);
        result
    }

    fn test_key(key: &BytesMut, seq: u64, vt: ValueType) {
        let encode = i_key(key, seq, vt.clone());
        let in_slice = Slice::new_from_mut(&encode);
        let mut decode = ParsedInternalKey {
            user_key: Slice::new_from_str(""),
            sequence: 0,
            value_type: KTypeValue,
        };
        assert!(parse_internal_key(&in_slice, &mut decode));
        assert_eq!(&key[..], decode.user_key.data());
        assert_eq!(seq, decode.sequence);
        assert_eq!(vt, decode.value_type);
        assert!(!parse_internal_key(
            &Slice::new_from_str("bar"),
            &mut decode
        ))
    }

    #[test]
    fn test_InternalKey_EncodeDecode() {
        let keys: [&'static str; 4] = ["", "k", "hello", "longggggggggggggggggggggg"];
        let seq: [u64; 12] = [
            1,
            2,
            3,
            (1u64 << 8) - 1,
            1u64 << 8,
            (1u64 << 8) + 1,
            (1u64 << 16) - 1,
            1u64 << 16,
            (1u64 << 16) + 1,
            (1u64 << 32) - 1,
            1u64 << 32,
            (1u64 << 32) + 1,
        ];
        for key in keys.iter() {
            for s in seq.iter() {
                test_key(&BytesMut::from(*key), *s, KTypeValue);
                test_key(&BytesMut::from("hello"), 1, ValueType::KTypeDeletion);
            }
        }
    }
}
