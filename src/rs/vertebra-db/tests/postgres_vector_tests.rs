use vertebra_db::postgres::{
    decode_pgvector_bit, decode_pgvector_f16, decode_pgvector_f32, encode_pgvector_bit,
    encode_pgvector_f16, encode_pgvector_f32,
};

#[test]
#[allow(clippy::excessive_precision)]
fn test_halfvec_ties_to_even_signed_zero_subnormal_and_max_finite() {
    let inputs = [
        1.00048828125_f32,
        1.00146484375_f32,
        -0.0_f32,
        5.9604645e-8_f32,
        65504.0_f32,
    ];
    let encoded = encode_pgvector_f16(&inputs).expect("encode halfvec");
    assert_eq!(encoded.len(), 4 + inputs.len() * 2);
    assert_eq!(&encoded[0..4], &[0x00, 0x05, 0x00, 0x00]);
    let expected_words: [u16; 5] = [0x3c00, 0x3c02, 0x8000, 0x0001, 0x7bff];
    for (i, &expected) in expected_words.iter().enumerate() {
        let actual = u16::from_be_bytes([encoded[4 + i * 2], encoded[5 + i * 2]]);
        assert_eq!(
            actual, expected,
            "mismatch at index {i}: actual=0x{actual:04x}, expected=0x{expected:04x}"
        );
    }

    let decoded = decode_pgvector_f16(&encoded).expect("decode halfvec");
    assert_eq!(decoded[0], 1.0_f32);
    assert_eq!(decoded[1], 1.001953125_f32);
    assert!(decoded[2] == 0.0 && decoded[2].is_sign_negative());
    assert_eq!(decoded[3], 5.9604645e-8_f32);
    assert_eq!(decoded[4], 65504.0_f32);
}

#[test]
fn test_halfvec_finite_overflow_and_non_finite_wire_rejection() {
    assert!(encode_pgvector_f16(&[65520.0_f32]).is_err());
    assert!(encode_pgvector_f16(&[-65520.0_f32]).is_err());
    assert!(encode_pgvector_f16(&[f32::NAN]).is_err());
    assert!(encode_pgvector_f16(&[f32::INFINITY]).is_err());
    assert!(encode_pgvector_f16(&[f32::NEG_INFINITY]).is_err());

    assert!(decode_pgvector_f16(&[0, 1, 0, 0, 0x7c, 0x00]).is_err());
    assert!(decode_pgvector_f16(&[0, 1, 0, 0, 0xfc, 0x00]).is_err());
    assert!(decode_pgvector_f16(&[0, 1, 0, 0, 0x7e, 0x00]).is_err());

    assert!(decode_pgvector_f32(&[0, 1, 0, 0, 0x7f, 0xc0, 0x00, 0x00]).is_err());
    assert!(decode_pgvector_f32(&[0, 1, 0, 0, 0x7f, 0x80, 0x00, 0x00]).is_err());
    assert!(decode_pgvector_f32(&[0, 1, 0, 0, 0xff, 0x80, 0x00, 0x00]).is_err());
}

#[test]
fn test_pgvector_dimension_and_unused_header_bounds() {
    let max_vec = vec![1.0_f32; 16_000];
    let enc32 = encode_pgvector_f32(&max_vec).expect("16000-dim f32 allowed");
    assert_eq!(
        decode_pgvector_f32(&enc32).expect("decode 16000-dim").len(),
        16_000
    );

    let enc16 = encode_pgvector_f16(&max_vec).expect("16000-dim f16 allowed");
    assert_eq!(
        decode_pgvector_f16(&enc16).expect("decode 16000-dim").len(),
        16_000
    );

    let over_vec = vec![1.0_f32; 16_001];
    assert!(encode_pgvector_f32(&over_vec).is_err());
    assert!(encode_pgvector_f16(&over_vec).is_err());

    assert!(decode_pgvector_f32(&[0, 0, 0, 0]).is_err());
    assert!(decode_pgvector_f16(&[0, 0, 0, 0]).is_err());

    let mut over_wire_32 = Vec::with_capacity(4 + 16_001 * 4);
    over_wire_32.extend_from_slice(&16_001_u16.to_be_bytes());
    over_wire_32.extend_from_slice(&0_u16.to_be_bytes());
    over_wire_32.resize(4 + 16_001 * 4, 0);
    assert!(decode_pgvector_f32(&over_wire_32).is_err());

    let mut over_wire_16 = Vec::with_capacity(4 + 16_001 * 2);
    over_wire_16.extend_from_slice(&16_001_u16.to_be_bytes());
    over_wire_16.extend_from_slice(&0_u16.to_be_bytes());
    over_wire_16.resize(4 + 16_001 * 2, 0);
    assert!(decode_pgvector_f16(&over_wire_16).is_err());

    assert!(decode_pgvector_f32(&[0, 1, 0, 1, 0, 0, 0, 0]).is_err());
    assert!(decode_pgvector_f16(&[0, 1, 0, 1, 0, 0]).is_err());
}

#[test]
fn test_pgvector_bit_zero_length_padding_and_large_varbit() {
    let enc_zero = encode_pgvector_bit(b"", 0).expect("0-length bitstring allowed");
    assert_eq!(enc_zero, vec![0, 0, 0, 0]);
    let (dec_zero, len_zero) = decode_pgvector_bit(&enc_zero).expect("decode 0-length bitstring");
    assert!(dec_zero.is_empty());
    assert_eq!(len_zero, 0);

    let enc_three = encode_pgvector_bit(&[0xa0], 3).expect("3-bit MSB-padded bitstring");
    assert_eq!(enc_three, vec![0, 0, 0, 3, 0xa0]);
    let (dec_three, len_three) = decode_pgvector_bit(&enc_three).expect("decode 3-bit");
    assert_eq!(dec_three, vec![0xa0]);
    assert_eq!(len_three, 3);

    assert!(encode_pgvector_bit(&[0xa8], 3).is_err());
    assert!(decode_pgvector_bit(&[0, 0, 0, 3, 0xa8]).is_err());
    assert!(decode_pgvector_bit(&[0xff, 0xff, 0xff, 0xff]).is_err());

    let large_bits = vec![0xaa_u8; 2_500];
    let enc_large = encode_pgvector_bit(&large_bits, 20_000).expect("20000-bit varbit allowed");
    let (dec_large, len_large) = decode_pgvector_bit(&enc_large).expect("decode 20000-bit varbit");
    assert_eq!(len_large, 20_000);
    assert_eq!(dec_large, large_bits);
}
