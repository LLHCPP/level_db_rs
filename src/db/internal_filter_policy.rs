use crate::obj::slice::Slice;
use crate::util::filter_policy::FilterPolicy;
use bytes::BytesMut;
use std::sync::Arc;

struct InternalFilterPolicy {
    user_policy_: Arc<dyn FilterPolicy>,
}

fn extract_user_key(internal_key: &Slice) -> Slice {
    assert!(internal_key.len() > 8);
    Slice::new_from_ptr(&internal_key.data()[0..internal_key.len() - 8])
}

impl FilterPolicy for InternalFilterPolicy {
    fn name(&self) -> &'static str {
        self.user_policy_.name()
    }

    fn create_filter(&self, keys: &[Slice], dst: &mut BytesMut) {
        let m_keys = keys
            .iter()
            .map(|key| extract_user_key(key))
            .collect::<Vec<_>>();
        self.user_policy_.create_filter(&m_keys, dst)
    }

    fn key_may_match(&self, key: &Slice, filter: &Slice) -> bool {
        self.user_policy_
            .key_may_match(&extract_user_key(key), filter)
    }
}
