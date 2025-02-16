use criterion::{black_box, criterion_group, criterion_main, Criterion};
use level_db_rs::unit;
fn bench_fib(c: &mut Criterion) {
    c.bench_function("test_hash", |b| b.iter(||
    unit::hash_string(black_box("test_hashtest_hashtest_hashtest_hashtest_hashtest_hashtest_hashtest_hashtest_hashtest_hash"), black_box(0x12345678))));
}

// 注册测试组
criterion_group!(benches, bench_fib);
criterion_main!(benches);