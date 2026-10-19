use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use vertebra_core::router::{RouteId, Router};

fn bench_router_match(c: &mut Criterion) {
    let mut router = Router::new();
    router.insert("/hello", RouteId(1)).unwrap();
    router.insert("/users/{id:int}", RouteId(2)).unwrap();
    router
        .insert("/users/{id:int}/posts/{post_id:uuid}", RouteId(3))
        .unwrap();

    let mut group = c.benchmark_group("router_match");
    let cases = [
        "/hello",
        "/users/42",
        "/users/42/posts/550e8400-e29b-41d4-a716-446655440000",
    ];

    for path in cases {
        group.bench_with_input(BenchmarkId::from_parameter(path), &path, |b, path| {
            b.iter(|| {
                let result = router.match_path(black_box(path));
                black_box(result);
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_router_match);
criterion_main!(benches);
