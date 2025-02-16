use std::cell::RefCell;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use level_db_rs::unit;
use rand::{thread_rng, Rng};

fn bench_fib(c: &mut Criterion) {
    c.bench_function("test_hash", |b| b.iter(||{
        let testinput: [u8;48] = [0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        unit::hash(black_box(&testinput), black_box(0x12345678));
    }
    ));
}

// 注册测试组
criterion_group!(benches, bench_fib);
criterion_main!(benches);