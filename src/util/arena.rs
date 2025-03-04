use bumpalo::Bump;
struct Arena {
    bump: Bump,
}
impl Arena {
    fn new() -> Self {
        Self {
            bump: Bump::new(),
        }
    }
    fn alloc<T>(&mut self, value:T) -> &mut T
    {
        self.bump.alloc(value)
    }
    // 专门为数组优化的版本
    fn alloc_array<T>(&mut self, len: usize) -> &mut [T] {
        let layout = std::alloc::Layout::array::<T>(len).unwrap();
        let slice = {
            let ptr = self.bump.alloc_layout(layout).as_ptr() as *mut T;
            unsafe { std::slice::from_raw_parts_mut(ptr, len) }
        };
        slice
    }
}
#[cfg(test)]
mod tests {
    use crate::util::arena::Arena;
    #[test]
    fn test_alloc() {
        let mut test = Arena::new();
        let buffer = test.alloc([0; 4]);
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer[0], 0);
    }

    #[test]
    fn test_alloc_array() {
        let mut test = Arena::new();
        let buffer = test.alloc_array::<u8>(12);
        assert_eq!(buffer.len(), 12)
    }
}