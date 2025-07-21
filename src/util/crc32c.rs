pub(crate) use crc32c;
const K_MASK_DELTA: u32 = 0xa282ead8;
pub fn extend(init_crc: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(init_crc, data)
}
pub fn value(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}
#[inline]
pub fn mask(crc: u32) -> u32 {
    ((crc >> 15) | (crc << 17)) + K_MASK_DELTA
}

#[inline]
pub fn unmask(masked_crc: u32) -> u32 {
    let crc = masked_crc - K_MASK_DELTA;
    (crc >> 17) | (crc << 15)
}

#[cfg(test)]
mod tests {
    use crate::util::crc32c::{extend, mask, unmask, value};

    #[test]
    fn test_crc32c() {
        let mut buf: [u8; 32] = [0; 32];
        assert_eq!(0x8a9136aa, value(&buf));
        buf = [0xff; 32];
        assert_eq!(0x62a8ab43, value(&buf));
        let mut pos = 0;
        for i in buf.iter_mut() {
            *i = pos;
            pos += 1;
        }
        assert_eq!(0x46dd794e, value(&buf));
        for i in 0..32u8 {
            buf[i as usize] = 31 - i;
        }
        assert_eq!(0x113fdb5c, value(&buf));

        let data: [u8; 48] = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(0xd9963a56, value(&data))
    }
    #[test]
    fn test_values() {
        assert_ne!(value(b"a"), value(b"foo"));
    }

    #[test]
    fn test_extend() {
        assert_eq!(value(b"hello world"), extend(value(b"hello"), b" world"));
    }

    #[test]
    fn test_mask() {
        let crc = value(b"foo");
        assert_ne!(crc, mask(crc));
        assert_ne!(crc, mask(mask(crc)));
        assert_eq!(crc, unmask(mask(crc)));
        assert_eq!(crc, unmask(unmask(mask(mask(crc)))));
    }
}
