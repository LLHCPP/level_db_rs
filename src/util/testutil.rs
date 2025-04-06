use crate::obj::slice::Slice;
use crate::util::random::Random;
use bytes::{BufMut, BytesMut};
use rand::Rng;
use std::mem;

#[inline]
pub fn random_seed() -> u32 {
    let mut rng = rand::rng();
    rng.random::<u32>()
}

pub fn random_string(rnd: &mut Random, len: usize, dst: &mut BytesMut) -> Slice {
    dst.reserve(len);
    for i in 0..len {
        dst.put_u8(rnd.uniform(95) as u8 + b' ')
    }
    Slice::new_from_mut(dst)
}

pub fn random_key(rnd: &mut Random, len: usize) -> BytesMut {
    static K_TEST_CHARS: [u8; 10] = [0, 1, b'a', b'b', b'c', b'd', b'e', 0xfd, 0xfe, 0xff];
    let mut res = BytesMut::with_capacity(len);
    for _ in 0..len {
        res.put_u8(K_TEST_CHARS[rnd.uniform(mem::size_of_val(&K_TEST_CHARS) as u32) as usize])
    }
    res
}

pub fn compressible_string(
    rnd: &mut Random,
    compression_ratio: f64,
    len: usize,
    dst: &mut BytesMut,
) -> Slice {
    let mut raw = (len as f64 * compression_ratio) as usize;
    if raw < 1 {
        raw = 1;
    }
    let mut raw_data = BytesMut::with_capacity(raw);
    random_string(rnd, raw, &mut raw_data);
    dst.clear();
    while dst.len() < len {
        dst.put(&raw_data[..]);
    }
    dst.resize(len, 0);
    Slice::new_from_mut(dst)
}
