use std::sync::Arc;
use bytes::BytesMut;
use crate::obj::slice::Slice;
use crate::util::filter_policy::FilterPolicy;

struct InternalFilterPolicy {
    user_policy_: Arc<dyn FilterPolicy>
}

impl FilterPolicy for InternalFilterPolicy {
    fn name(&self) -> &'static str {
        self.user_policy_.name()
    }

    fn create_filter(&self, keys: &[Slice], dst: &mut BytesMut) {
        todo!()
    }

    fn key_may_match(&self, key: &Slice, filter: &Slice) -> bool {
        todo!()
    }
}