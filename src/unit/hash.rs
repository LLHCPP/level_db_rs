use std::num::Wrapping;

fn hash(data_bytes:&[u8], seed:u32) ->u32 {
    let m: Wrapping<u32> = Wrapping(0xc6a4a793);
    let r:u32 = 24;
    let mut i = 0;
    let len = data_bytes.len();
    let mut h =  m * Wrapping(len as u32) ^ Wrapping(seed);
    while i + 4 <= len {
        let w = u32::from_le_bytes([data_bytes[i], data_bytes[i + 1], data_bytes[i + 2], data_bytes[i + 3]]);
        i += 4;
        h += w;
        h *= m;
        h ^= h >> 16;
    }
    let remaining = len - i;
    match remaining {
        3 => {
            h += u32::from_le_bytes([data_bytes[i], data_bytes[i + 1], data_bytes[i + 2], 0]);
            h *= m;
            h ^= h.0 >> r;
        }
        2 => {
            h += u32::from_le_bytes([data_bytes[i], data_bytes[i + 1], 0, 0]);
            h *= m;
            h ^= h.0 >> r;
        }
        1 => {
            h += u32::from_le_bytes([data_bytes[i], 0, 0, 0]);
            h *= m;
            h ^= h.0 >> r;
        }
        _ => {}
    }
    h.0
}
fn hash_string(data:&str, seed:u32) ->u32 {
    hash(data.as_bytes(), seed)
}
#[cfg(test)]
mod tests {
    use crate::unit::hash::hash;

    #[test]
    fn test_add() {
        let data0: &[u8] = &[];
        let data1: &[u8] = &[0x62];
        let data2: &[u8] = &[0xc3, 0x97];
        let data3: &[u8] = &[0xe2, 0x99, 0xa5];
        let data4: &[u8] = &[0xe1, 0x80, 0xb9, 0x32];
        let data5: &[u8] = &[0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(hash(data0, 0xbc9f1d34), 0xbc9f1d34);
        assert_eq!(hash(data1, 0xbc9f1d34), 0xef1345c4);
        assert_eq!(hash(data2, 0xbc9f1d34), 0x5b663814);
        assert_eq!(hash(data3, 0xbc9f1d34), 0x323c078f);
        assert_eq!(hash(data4, 0xbc9f1d34), 0xed21633a);
        assert_eq!(hash(data5, 0x12345678), 0xf333dabb);
    }
}
