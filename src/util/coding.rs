use crate::obj::slice::Slice;
use bytes::{BufMut, BytesMut};

fn encode_fixed32(dst: &mut [u8], value: u32) {
    dst[0] = (value >> 0) as u8;
    dst[1] = (value >> 8) as u8;
    dst[2] = (value >> 16) as u8;
    dst[3] = (value >> 24) as u8;
}

fn decode_fixed32(src: &[u8]) -> u32 {
    ((src[0] as u32) << 0)
        | ((src[1] as u32) << 8)
        | ((src[2] as u32) << 16)
        | ((src[3] as u32) << 24)
}

fn put_fixed32(dst: &mut BytesMut, value: u32) {
    let mut buf: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
    encode_fixed32(&mut buf, value);
    dst.put(&buf[..]);
}

fn encode_fixed64(dst: &mut [u8], value: u64) {
    dst[0] = (value >> 0) as u8;
    dst[1] = (value >> 8) as u8;
    dst[2] = (value >> 16) as u8;
    dst[3] = (value >> 24) as u8;
    dst[4] = (value >> 32) as u8;
    dst[5] = (value >> 40) as u8;
    dst[6] = (value >> 48) as u8;
    dst[7] = (value >> 56) as u8;
}
fn decode_fixed64(src: &[u8]) -> u64 {
    src[0] as u64
        | ((src[1] as u64) << 8)
        | ((src[2] as u64) << 16)
        | ((src[3] as u64) << 24)
        | ((src[4] as u64) << 32)
        | ((src[5] as u64) << 40)
        | ((src[6] as u64) << 48)
        | ((src[7] as u64) << 56)
}
fn put_fixed64(dst: &mut BytesMut, value: u64) {
    let mut buf: [u8; size_of::<u64>()] = [0; size_of::<u64>()];
    encode_fixed64(&mut buf, value);
    dst.put_slice(&buf);
}

#[inline]
fn encode_varint32(dst: &mut [u8], v: u32) -> &[u8] {
    const B: u32 = 128;
    let num_bytes = if v < (1 << 7) {
        dst[0] = v as u8;
        1
    } else if v < (1 << 14) {
        dst[0] = (v | B) as u8;
        dst[1] = (v >> 7) as u8;
        2
    } else if v < (1 << 21) {
        dst[0] = (v | B) as u8;
        dst[1] = ((v >> 7) | B) as u8;
        dst[2] = (v >> 14) as u8;
        3
    } else if v < (1 << 28) {
        dst[0] = (v | B) as u8;
        dst[1] = ((v >> 7) | B) as u8;
        dst[2] = ((v >> 14) | B) as u8;
        dst[3] = (v >> 21) as u8;
        4
    } else {
        dst[0] = (v | B) as u8;
        dst[1] = ((v >> 7) | B) as u8;
        dst[2] = ((v >> 14) | B) as u8;
        dst[3] = ((v >> 21) | B) as u8;
        dst[4] = (v >> 28) as u8;
        5
    };
    &dst[..num_bytes]
}

fn put_var_int32(dst: &mut BytesMut, v: u32) {
    let mut buf: [u8; 5] = [0; 5];
    let append = encode_varint32(&mut buf, v);
    dst.put_slice(append);
}

#[inline]
fn encode_var_int64(dst: &mut [u8], mut v: u64) -> &mut [u8] {
    const B: u64 = 128;
    let mut pos: usize = 0;
    while v >= B {
        dst[pos] = (v | B) as u8;
        pos += 1;
        v >>= 7;
    }
    dst[pos] = v as u8;
    pos += 1;
    &mut dst[..pos]
}
fn put_var_int64(dst: &mut BytesMut, v: u64) {
    let mut buf: [u8; 10] = [0; 10];
    let append = encode_var_int64(&mut buf, v);
    dst.put_slice(append);
}

fn put_length_prefixed_slice(dst: &mut BytesMut, value: &Slice) {
    put_var_int32(dst, value.len() as u32);
    dst.put_slice(value.data());
}

fn var_int_length(mut v: u64) -> u32 {
    let mut len = 1u32;
    while v >= 128 {
        v >>= 7;
        len += 1;
    }
    len
}

#[inline]
fn get_var_int32ptr_fallback<'a>(ptr: &'a [u8], value: &mut u32) -> &'a [u8] {
    let mut result = 0u32;
    let mut shift = 0u32;
    let mut pos = 0usize;
    while shift <= 28 && pos < ptr.len() {
        let byte = ptr[pos] as u32;
        pos += 1;
        if (byte & 128u32) > 0 {
            result |= (byte & 127) << shift;
        } else {
            result |= byte << shift;
            *value = result;
            return &ptr[pos..];
        }
        shift += 7;
    }
    &[]
}

#[inline]
pub fn get_var_int32ptr<'a>(ptr: &'a [u8], value: &mut u32) -> &'a [u8] {
    if ptr.len() > 0 {
        let result = ptr[0] as u32;
        if result & 128 == 0 {
            *value = result;
            return &ptr[1..];
        }
    }
    get_var_int32ptr_fallback(ptr, value)
}

fn get_var_int32(input: &mut Slice, value: &mut u32) -> bool {
    let ptr = input.data();
    let limit = input.size();
    let g = get_var_int32ptr(ptr, value);
    if g.is_empty() {
        false
    } else {
        let remain = limit - g.len();
        *input = input.slice(remain);
        true
    }
}

#[inline]
fn get_var_int64ptr<'a>(ptr: &'a [u8], value: &mut u64) -> &'a [u8] {
    let mut result = 0u64;
    let mut shift = 0u32;
    let mut pos = 0usize;
    while shift <= 63 && pos < ptr.len() {
        let byte = ptr[pos] as u64;
        pos += 1;
        if (byte & 128u64) > 0 {
            result |= (byte & 127) << shift;
        } else {
            result |= byte << shift;
            *value = result;
            return &ptr[pos..];
        }
        shift += 7;
    }
    &[]
}

fn get_var_int64(input: &mut Slice, value: &mut u64) -> bool {
    let ptr = input.data();
    let limit = input.size();
    let g = get_var_int64ptr(ptr, value);
    if g.is_empty() {
        false
    } else {
        let remain = limit - g.len();
        *input = input.slice(remain);
        true
    }
}

fn get_length_prefixed_slice(input: &mut Slice, result: &mut Slice) -> bool {
    let mut len = 0u32;
    if get_var_int32(input, &mut len) && input.size() >= len as usize {
        *result = input.slice(len as usize);
        input.remove_prefix(len as usize);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    #[test]
    fn test_fixed32() {
        let mut bytes = BytesMut::new();
        for i in 0..100000 {
            put_fixed32(&mut bytes, i);
        }
        for i in 0..100000 {
            let actual = decode_fixed32(&bytes[..]);
            assert_eq!(actual, i);
            bytes.advance(size_of::<u32>());
        }
    }

    #[test]
    fn test_fixed64() {
        let mut bytes = BytesMut::new();
        for power in 0..=63 {
            let value = 1u64 << power;
            put_fixed64(&mut bytes, value - 1);
            put_fixed64(&mut bytes, value + 0);
            put_fixed64(&mut bytes, value + 1);
        }
        for power in 0..=63 {
            let value = 1u64 << power;
            let actual = decode_fixed64(&bytes[..]);
            assert_eq!(actual, value - 1);
            bytes.advance(size_of::<u64>());
            let actual = decode_fixed64(&bytes[..]);
            assert_eq!(actual, value + 0);
            bytes.advance(size_of::<u64>());
            let actual = decode_fixed64(&bytes[..]);
            assert_eq!(actual, value + 1);
            bytes.advance(size_of::<u64>());
        }
    }

    #[test]
    fn test_encoding_output() {
        let mut bytes = BytesMut::new();
        put_fixed32(&mut bytes, 0x04030201);
        assert_eq!(4, bytes.len());
        assert_eq!(0x01, bytes[0]);
        assert_eq!(0x02, bytes[1]);
        assert_eq!(0x03, bytes[2]);
        assert_eq!(0x04, bytes[3]);

        bytes.clear();

        put_fixed64(&mut bytes, 0x0807060504030201);
        assert_eq!(8, bytes.len());
        assert_eq!(0x01, bytes[0]);
        assert_eq!(0x02, bytes[1]);
        assert_eq!(0x03, bytes[2]);
        assert_eq!(0x04, bytes[3]);
        assert_eq!(0x05, bytes[4]);
        assert_eq!(0x06, bytes[5]);
        assert_eq!(0x07, bytes[6]);
        assert_eq!(0x08, bytes[7]);
    }

    #[test]
    fn test_varint32() {
        let mut bytes = BytesMut::new();
        for i in 0..32 * 32u32 {
            let v = (i / 32) << (i % 32);
            put_var_int32(&mut bytes, v);
        }

        let mut p = &bytes[..];
        for i in 0..32 * 32u32 {
            let expect = (i / 32) << (i % 32);
            let mut actual = 0;
            let start = p;
            p = get_var_int32ptr(p, &mut actual);
            assert_eq!(expect, actual);
            if i != 32 * 32u32 - 1 {
                assert!(!p.is_empty());
            }
            let length = (p.as_ptr() as usize - start.as_ptr() as usize) / size_of::<u8>();
            assert_eq!(var_int_length(actual as u64), length as u32);
        }
        assert!(p.is_empty())
    }

    #[test]
    fn test_varint64() {
        let mut values = Vec::new();
        values.push(0);
        values.push(100);
        values.push(!0u64);
        values.push(!0u64 - 1);

        for k in 0..64 {
            let power = 1u64 << k;
            values.push(power);
            values.push(power - 1);
            values.push(power + 1);
        }

        let mut bytes = BytesMut::new();

        for value in values.iter() {
            put_var_int64(&mut bytes, *value);
        }
        let mut p = &bytes[..];
        for value in values.iter() {
            let mut actual = 0u64;
            let start = p;
            p = get_var_int64ptr(p, &mut actual);
            assert_eq!(*value, actual);
            let length = (p.as_ptr() as usize - start.as_ptr() as usize) / size_of::<u8>();
            assert_eq!(var_int_length(actual), length as u32);
        }
        assert!(p.is_empty())
    }
}
