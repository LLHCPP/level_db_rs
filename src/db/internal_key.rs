use crate::db::internal_key_comparator::{
    append_internal_key, extract_user_key, ParsedInternalKey, ValueType,
};
use crate::obj::slice::Slice;
use bytes::BytesMut;

struct InternalKey {
    pub rep_: BytesMut,
}

impl InternalKey {
    fn new(user_key: Slice, sequence_number: u64, t: ValueType) -> InternalKey {
        let mut result = InternalKey {
            rep_: BytesMut::with_capacity(user_key.len() + 8),
        };
        append_internal_key(
            &mut result.rep_,
            &ParsedInternalKey {
                user_key,
                sequence: sequence_number,
                value_type: t,
            },
        );
        result
    }
    fn decode_from(&mut self, s: &Slice) -> bool {
        self.rep_ = BytesMut::from(s.data());
        !self.rep_.is_empty()
    }
    fn encode(&self) -> Slice {
        debug_assert!(!self.rep_.is_empty());
        Slice::new_from_mut(&self.rep_)
    }
    fn user_key(&self) -> Slice {
        extract_user_key(&Slice::new_from_mut(&self.rep_))
    }

    fn set_from(&mut self, p: &ParsedInternalKey) {
        self.rep_.clear();
        append_internal_key(&mut self.rep_, p);
    }

    fn clear(&mut self) {
        self.rep_.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::db::internal_key::InternalKey;
    use crate::obj::slice::Slice;
    use bytes::BytesMut;
    #[test]
    fn test_internal_key_decode_from_empty() {
        let mut internal_key = InternalKey {
            rep_: BytesMut::new(),
        };
        assert!(!internal_key.decode_from(&Slice::new_from_str("")))
    }
}
