//! Criterion benchmark for SSRPool rendering (cached vs uncached paths).

use _bundler::SSRPool;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::io::Write;
use tempfile::NamedTempFile;

const BENCH_BUNDLE: &str = r#"
    globalThis.__ssr_render = function(component, props) {
        return {
            head: ["<title>" + (props.title || "Bench") + "</title>"],
            body: "<main><h1>" + component + "</h1><p>" + (props.content || "") + "</p></main>"
        };
    };
"#;

fn bench_ssr_render(c: &mut Criterion) {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(BENCH_BUNDLE.as_bytes())
        .expect("Failed to write bundle");
    file.flush().expect("Failed to flush bundle");

    let pool = SSRPool::new(file.path().to_str().unwrap(), 4).expect("Failed to create SSRPool");

    c.bench_function("ssr_render_cached", |b| {
        let props = r#"{"title":"Benchmark","content":"Cached SSR payload"}"#;
        let _ = pool.render("BenchComponent", props);
        b.iter(|| {
            let res = pool.render("BenchComponent", props).unwrap();
            black_box(res);
        });
    });

    c.bench_function("ssr_render_uncached", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let props = format!(r#"{{"title":"Benchmark","content":"{counter}"}}"#);
            let res = pool.render("BenchComponent", &props).unwrap();
            black_box(res);
        });
    });
}

criterion_group!(benches, bench_ssr_render);
criterion_main!(benches);
