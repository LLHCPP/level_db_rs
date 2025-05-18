use crate::obj::slice::Slice;
use crate::util::comparator::Comparator;
use bytes::BytesMut;
use std::cmp::Ordering;
use std::sync::{Arc, OnceLock};

static BYTEWISE_COMPARATOR: BytewiseComparatorImpl = BytewiseComparatorImpl {};
static BYTEWISE: OnceLock<Arc<dyn Comparator>> = OnceLock::new();
pub struct BytewiseComparatorImpl {}
impl Comparator for BytewiseComparatorImpl {
    fn compare(&self, a: &Slice, b: &Slice) -> Ordering {
        a.compare(b)
    }

    fn name(&self) -> &'static str {
        "leveldb.BytewiseComparator"
    }

    fn find_shortest_separator(&self, start: &mut BytesMut, limit: &Slice) {
        let min_length = std::cmp::min(start.len(), limit.len());
        let mut diff_index = 0usize;
        while diff_index < min_length && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }
        if diff_index < min_length {
            let diff_byte = start[diff_index];
            if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
                start[diff_index] += 1;
                start.truncate(diff_index + 1);
                debug_assert!(
                    self.compare(&Slice::new_from_array(&start[..]), limit) == Ordering::Less
                )
            }
        }
    }

    fn find_short_successor(&self, key: &mut BytesMut) {
        let mut len = 0usize;
        for byte in key.iter_mut() {
            if *byte != 0xff {
                *byte += 1;
                key.truncate(len + 1);
                return;
            }
            len += 1;
        }
    }
}

pub fn byte_wise_comparator() -> Arc<dyn Comparator> {
    BYTEWISE
        .get_or_init(|| Arc::new(BytewiseComparatorImpl {}))
        .clone()
}
