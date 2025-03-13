use crate::obj::slice::Slice;
use crate::util::coding;
use crate::util::comparator::Comparator;
use bytes::{BufMut, BytesMut};
use std::cmp::Ordering;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
enum ValueType {
    KTypeDeletion = 0x0,
    KTypeValue = 0x1,
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

struct InternalKeyComparator<T: Comparator> {
    user_comparator_: T,
}
impl<T: Comparator> Comparator for InternalKeyComparator<T> {
    fn compare(&mut self, akey: &Slice, bkey: &Slice) -> Ordering {
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

    fn find_shortest_separator(&mut self, start: &mut BytesMut, limit: &Slice) {
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

    fn find_short_successor(&mut self, key: &mut BytesMut) {
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
