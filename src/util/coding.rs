use crate::obj::slice::Slice;
use bytes::{BufMut, BytesMut};

fn encode_fixed32(dst: &mut [u8], value: u32) {
    dst[0] = (value >> 0) as u8;
    dst[1] = (value >> 8) as u8;
    dst[2] = (value >> 16) as u8;
    dst[3] = (value >> 24) as u8;
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
fn put_fixed64(dst: &mut BytesMut, value: u64) {
    let mut buf: [u8; size_of::<u64>()] = [0; size_of::<u64>()];
    encode_fixed64(&mut buf, value);
    dst.put_slice(&buf);
}

#[inline]
fn encode_varint32(dst: &mut [u8], v: u32) -> &mut [u8] {
    const B: u32 = 128;
    let mut pos: usize = 0;
    if v < (1 << 7) {
        dst[pos] = v as u8;
        pos += 1;
    } else if v < (1 << 14) {
        dst[pos] = (v | B) as u8;
        pos += 1;
        dst[pos] = (v >> 7) as u8;
        pos += 1;
    } else if v < (1 << 21) {
        dst[pos] = (v | B) as u8;
        pos += 1;
        dst[pos] = (v >> 7 | B) as u8;
        pos += 1;
        dst[pos] = (v >> 14) as u8;
    } else if v < (1 << 28) {
        dst[pos] = (v | B) as u8;
        pos += 1;
        dst[pos] = (v >> 7 | B) as u8;
        pos += 1;
        dst[pos] = (v >> 14 | B) as u8;
        pos += 1;
        dst[pos] = (v >> 21) as u8;
    } else {
        dst[pos] = (v | B) as u8;
        pos += 1;
        dst[pos] = (v >> 7 | B) as u8;
        pos += 1;
        dst[pos] = (v >> 14 | B) as u8;
        pos += 1;
        dst[pos] = (v >> 21 | B) as u8;
        pos += 1;
        dst[pos] = (v >> 28) as u8;
    }
    &mut dst[..pos]
}

fn put_varint32(dst: &mut BytesMut, v: u32) {
    let mut buf: [u8; 5] = [0; 5];
    let append = encode_varint32(&mut buf, v);
    dst.put_slice(append);
}

#[inline]
fn encode_varint64(dst: &mut [u8], mut v: u64) -> &mut [u8] {
    const B: u64 = 128;
    let mut pos: usize = 0;
    while v >= B {
        dst[pos] = (v | B) as u8;
        pos += 1;
        v >>= 7;
    }
    &mut dst[..pos]
}
fn put_varint64(dst: &mut BytesMut, v: u64) {
    let mut buf: [u8; 10] = [0; 10];
    let append = encode_varint64(&mut buf, v);
    dst.put_slice(append);
}

fn put_length_prefixed_slice(dst: &mut BytesMut, value: &Slice) {
    put_varint32(dst, value.len() as u32);
    dst.put_slice(value.data());
}

fn varint_length(mut v: u64) -> u32 {
    let mut len = 1u32;
    while v >= 128 {
        v >>= 7;
        len += 1;
    }
    len
}

#[inline]
fn get_varint32ptr_fallback<'a>(ptr: &'a [u8], limit: usize, value: &mut u32) -> &'a [u8] {
    let mut result = 0u32;
    let mut shift = 0u32;
    let mut pos = 0usize;
    while shift <= 28 && pos < limit {
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
pub fn get_varint32ptr<'a>(ptr: &'a [u8], limit: usize, value: &mut u32) -> &'a [u8] {
    if limit > 0 {
        let result = ptr[0] as u32;
        if result & 128 == 0 {
            *value = result;
            return &ptr[1..];
        }
    }
    get_varint32ptr_fallback(ptr, limit, value)
}

fn get_varint32(input: &mut Slice, value: &mut u32) -> bool {
    let ptr = input.data();
    let limit = input.size();
    let g = get_varint32ptr(ptr, limit, value);
    if g.is_empty() {
        false
    } else {
        let remain = limit - g.len();
        *input = input.slice(remain);
        true
    }
}

#[inline]
fn get_varint64ptr<'a>(ptr: &'a [u8], limit: usize, value: &mut u64) -> &'a [u8] {
    let mut result = 0u64;
    let mut shift = 0u32;
    let mut pos = 0usize;
    while shift <= 63 && pos < limit {
        let byte = ptr[pos] as u64;
        pos += 1;
        if (byte & 128u64) > 0 {
            result |= (byte & 127) << shift;
        } else {
            result |= (byte << shift);
            *value = result;
            return &ptr[pos..];
        }
        shift += 7;
    }
    &[]
}

fn get_varint64(input: &mut Slice, value: &mut u64) -> bool {
    let ptr = input.data();
    let limit = input.size();
    let g = get_varint64ptr(ptr, limit, value);
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
    if get_varint32(input, &mut len) && input.size() >= len as usize {
        *result = input.slice(len as usize);
        input.remove_prefix(len as usize);
        true
    } else {
        false
    }
}
