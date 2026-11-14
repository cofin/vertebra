use std::hint::black_box;

use bytes::{BufMut, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use _postgres::codecs::{
    CodecContext, INT4_OID, INT8_OID, JSONB_OID, NUMERIC_OID, TEXT_OID, TypeCatalog, TypeInfo,
    TypeKind, decode_value, decode_value_with_catalog, encode_value, encode_value_with_catalog,
};
use _postgres::copy::{BinaryCopyDecoder, BinaryCopyEncoder};
use _postgres::extended::{Parameter, encode_bind};
use _postgres::replication::pgoutput::PgOutputDecoder;
use _postgres::result::{Format, PgNumeric, PgValue};

fn build_vector_value() -> PgValue {
    PgValue::Vector {
        kind: _postgres::result::VectorKind::Vector,
        values: (0..128).map(|idx| (idx as f32) * 0.125).collect(),
    }
}

fn bench_postgres_codecs(c: &mut Criterion) {
    let mut group = c.benchmark_group("postgres/codecs");
    let ctx = CodecContext::default();
    let mut buf = BytesMut::with_capacity(1024);

    let int_val = PgValue::Int8(9_223_372_036_854_775);
    group.bench_function("encode_decode_int8_binary", |b| {
        b.iter(|| {
            buf.clear();
            encode_value(&int_val, INT8_OID, Format::Binary, &mut buf, &ctx).unwrap();
            let decoded = decode_value(INT8_OID, Format::Binary, Some(buf.as_ref()), &ctx).unwrap();
            black_box(decoded)
        });
    });

    let numeric_val = PgValue::Numeric(
        PgNumeric::from_decimal_parts(
            false,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            -10,
            false,
        )
        .unwrap(),
    );
    group.bench_function("encode_decode_numeric_binary", |b| {
        b.iter(|| {
            buf.clear();
            encode_value(&numeric_val, NUMERIC_OID, Format::Binary, &mut buf, &ctx).unwrap();
            let decoded =
                decode_value(NUMERIC_OID, Format::Binary, Some(buf.as_ref()), &ctx).unwrap();
            black_box(decoded)
        });
    });

    let json_val =
        PgValue::Json(br#"{"driver":"vertebra-postgres","rows":[1,2,3,4],"ok":true}"#.to_vec());
    group.bench_function("encode_decode_jsonb_binary", |b| {
        b.iter(|| {
            buf.clear();
            encode_value(&json_val, JSONB_OID, Format::Binary, &mut buf, &ctx).unwrap();
            let decoded =
                decode_value(JSONB_OID, Format::Binary, Some(buf.as_ref()), &ctx).unwrap();
            black_box(decoded)
        });
    });

    let mut vector_catalog = TypeCatalog::new_with_builtins();
    let vector_oid = 16_390;
    vector_catalog.insert_custom_type(TypeInfo {
        oid: vector_oid,
        schema: "public".to_string(),
        name: "vector".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "vector".to_string(),
        },
        delimiter: b',',
    });
    let vec_val = build_vector_value();
    group.bench_function("pgvector_128d_binary_roundtrip", |b| {
        b.iter(|| {
            buf.clear();
            encode_value_with_catalog(
                &vec_val,
                vector_oid,
                Format::Binary,
                &mut buf,
                &ctx,
                &vector_catalog,
            )
            .unwrap();
            let decoded = decode_value_with_catalog(
                vector_oid,
                Format::Binary,
                Some(buf.as_ref()),
                &ctx,
                &vector_catalog,
            )
            .unwrap();
            black_box(decoded)
        });
    });
    group.finish();
}

fn bench_postgres_wire_bind(c: &mut Criterion) {
    let ctx = CodecContext::default();
    let catalog = TypeCatalog::new_with_builtins();
    let params = vec![
        Parameter::new(PgValue::Int8(1)),
        Parameter::new(PgValue::Text("alpha".to_string())),
        Parameter::new(PgValue::Int4(42)),
        Parameter::new(PgValue::Bool(true)),
    ];
    let oids = [INT8_OID, TEXT_OID, INT4_OID, 16];
    let param_formats = [Format::Binary; 4];
    let result_formats = [Format::Binary];
    let mut out = BytesMut::with_capacity(512);

    c.bench_function("postgres/wire/encode_bind_4p", |b| {
        b.iter(|| {
            out.clear();
            encode_bind(
                "",
                "stmt_bench",
                &params,
                &oids,
                &param_formats,
                &result_formats,
                &ctx,
                &catalog,
                &mut out,
            )
            .unwrap();
            black_box(out.len())
        });
    });
}

fn bench_postgres_pgcopy_100_rows(c: &mut Criterion) {
    let ctx = CodecContext::default();
    let catalog = TypeCatalog::new_with_builtins();
    let col_oids = [INT4_OID, TEXT_OID];
    let rows: Vec<[PgValue; 2]> = (0..100)
        .map(|i| [PgValue::Int4(i), PgValue::Text(format!("copy_row_{i:04}"))])
        .collect();

    let mut group = c.benchmark_group("postgres/pgcopy");
    group.throughput(Throughput::Elements(100));
    group.bench_function(BenchmarkId::from_parameter("encode_decode_100x2"), |b| {
        b.iter(|| {
            let mut encoder = BinaryCopyEncoder::new(2).unwrap();
            let mut wire = BytesMut::with_capacity(4096);
            encoder.write_header(&mut wire);
            for row in &rows {
                encoder
                    .encode_row_values(&col_oids, row, &ctx, &catalog, &mut wire)
                    .unwrap();
            }
            encoder.finish_trailer(&mut wire).unwrap();

            let mut decoder = BinaryCopyDecoder::new(2);
            decoder.push(&wire).unwrap();
            let mut count = 0usize;
            while decoder.next_row().unwrap().is_some() {
                count += 1;
            }
            let total = decoder.finish().unwrap();
            black_box((count, total))
        });
    });
    group.finish();
}

fn build_synthetic_pgoutput_tx() -> Vec<u8> {
    let mut stream = BytesMut::with_capacity(256);

    stream.put_u8(b'R');
    stream.put_u32(42);
    stream.extend_from_slice(b"public\0");
    stream.extend_from_slice(b"bench_items\0");
    stream.put_u8(b'd');
    stream.put_u16(2);
    stream.put_u8(1);
    stream.extend_from_slice(b"id\0");
    stream.put_u32(INT4_OID);
    stream.put_i32(-1);
    stream.put_u8(0);
    stream.extend_from_slice(b"label\0");
    stream.put_u32(TEXT_OID);
    stream.put_i32(-1);

    stream.put_u8(b'B');
    stream.put_u64(0x0000_0001_0000_1000);
    stream.put_i64(1_000_000);
    stream.put_u32(701);

    stream.put_u8(b'I');
    stream.put_u32(42);
    stream.put_u8(b'N');
    stream.put_u16(2);
    stream.put_u8(b't');
    stream.put_u32(2);
    stream.extend_from_slice(b"11");
    stream.put_u8(b't');
    stream.put_u32(5);
    stream.extend_from_slice(b"hello");

    stream.put_u8(b'C');
    stream.put_u8(0);
    stream.put_u64(0x0000_0001_0000_0F00);
    stream.put_u64(0x0000_0001_0000_1000);
    stream.put_i64(1_000_001);

    stream.to_vec()
}

fn bench_postgres_pgoutput_decode(c: &mut Criterion) {
    let payload = build_synthetic_pgoutput_tx();
    let ctx = CodecContext::default();

    let mut group = c.benchmark_group("postgres/pgoutput");
    group.throughput(Throughput::Bytes(payload.len() as u64));
    group.bench_function("decode_relation_tx", |b| {
        b.iter(|| {
            let mut catalog = TypeCatalog::new_with_builtins();
            let mut decoder = PgOutputDecoder::new(64, 1024 * 1024);
            decoder.push(&payload).unwrap();
            let mut frames = 0usize;
            while decoder
                .decode_next_frame(&mut catalog, &ctx)
                .unwrap()
                .is_some()
            {
                frames += 1;
            }
            decoder.finish_clean_eof().unwrap();
            black_box(frames)
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_postgres_codecs,
    bench_postgres_wire_bind,
    bench_postgres_pgcopy_100_rows,
    bench_postgres_pgoutput_decode,
);
criterion_main!(benches);
