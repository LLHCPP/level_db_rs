use crate::util::coding::decode_fixed32;
use bytes::Bytes;

struct Block {
    data: Bytes,
    restart_offset_: usize,
}

impl Block {
    fn num_restarts(&self) -> u32 {
        debug_assert!(self.data.len() >= size_of::<u32>());
        let pos = self.data.len() - size_of::<u32>();
        decode_fixed32(&self.data[pos..])
    }
    
    
    
    
}
