//! High-throughput RFC 9562 UUIDv4, monotonic UUIDv7, and URL-safe NanoID generation.

use std::cell::RefCell;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";
const NANOID_ALPHABET: &[u8; 64] =
    b"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
const ENTROPY_POOL_BYTES: usize = 256;

static UUID7_STATE: AtomicU64 = AtomicU64::new(0);

struct EntropyPool {
    buf: [u8; ENTROPY_POOL_BYTES],
    pos: usize,
}

impl EntropyPool {
    fn new() -> Self {
        let mut pool = Self {
            buf: [0u8; ENTROPY_POOL_BYTES],
            pos: ENTROPY_POOL_BYTES,
        };
        pool.refill();
        pool
    }

    fn refill(&mut self) {
        fill_os_random(&mut self.buf);
        self.pos = 0;
    }

    #[inline]
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        let mut offset = 0;
        while offset < dest.len() {
            if self.pos >= ENTROPY_POOL_BYTES {
                self.refill();
            }
            let available = ENTROPY_POOL_BYTES - self.pos;
            let needed = dest.len() - offset;
            let copy_len = available.min(needed);
            dest[offset..offset + copy_len]
                .copy_from_slice(&self.buf[self.pos..self.pos + copy_len]);
            self.pos += copy_len;
            offset += copy_len;
        }
    }

    #[inline]
    fn next_u16(&mut self) -> u16 {
        let mut bytes = [0u8; 2];
        self.fill_bytes(&mut bytes);
        u16::from_le_bytes(bytes)
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        let mut bytes = [0u8; 8];
        self.fill_bytes(&mut bytes);
        u64::from_le_bytes(bytes)
    }

    #[inline]
    fn next_u128(&mut self) -> u128 {
        let mut bytes = [0u8; 16];
        self.fill_bytes(&mut bytes);
        u128::from_be_bytes(bytes)
    }
}

thread_local! {
    static THREAD_ENTROPY: RefCell<EntropyPool> = RefCell::new(EntropyPool::new());
}

fn fill_os_random(dest: &mut [u8]) {
    #[cfg(target_os = "linux")]
    {
        let mut filled = 0;
        while filled < dest.len() {
            // SAFETY: `dest[filled..]` is a valid mutable byte slice in stack/heap memory
            // owned exclusively by the caller, and `getrandom` writes at most `len` bytes.
            let rc = unsafe {
                libc::getrandom(
                    dest[filled..].as_mut_ptr().cast::<libc::c_void>(),
                    dest.len() - filled,
                    0,
                )
            };
            if rc > 0 {
                filled += rc as usize;
            } else {
                break;
            }
        }
        if filled == dest.len() {
            return;
        }
    }

    #[cfg(target_os = "macos")]
    {
        // SAFETY: `dest` is a valid mutable byte slice of length `dest.len()`, and
        // `arc4random_buf` fills the buffer with cryptographic random bytes without failing.
        unsafe {
            libc::arc4random_buf(dest.as_mut_ptr().cast::<libc::c_void>(), dest.len());
        }
        return;
    }

    if let Ok(mut file) = std::fs::File::open("/dev/urandom")
        && file.read_exact(dest).is_ok()
    {
        return;
    }

    let mut state = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15)
        ^ (dest.as_ptr() as usize as u64);
    for byte in dest.iter_mut() {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        *byte = (z & 0xFF) as u8;
    }
}

/// Generate an RFC 9562 Version 4 UUID as a 128-bit integer.
#[inline]
pub fn uuid4_u128() -> u128 {
    let raw = THREAD_ENTROPY.with(|pool| pool.borrow_mut().next_u128());
    (raw & !(0xF000_u128 << 64) & !(0xC000_u128 << 48)) | (0x4000_u128 << 64) | (0x8000_u128 << 48)
}

/// Generate an RFC 9562 Version 4 UUID formatted as a 36-character hyphenated lowercase string.
#[inline]
pub fn uuid4_str() -> String {
    format_uuid_hyphenated(uuid4_u128())
}

/// Generate an RFC 9562 Version 4 UUID formatted as a 32-character lowercase hexadecimal string.
#[inline]
pub fn uuid4_hex() -> String {
    format_uuid_hex(uuid4_u128())
}

/// Generate a monotonically ordered RFC 9562 Version 7 UUID as a 128-bit integer.
#[inline]
pub fn uuid7_u128() -> u128 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
        & 0x0000_FFFF_FFFF_FFFF;

    let (rand_seq, rand_b) = THREAD_ENTROPY.with(|pool| {
        let mut p = pool.borrow_mut();
        (p.next_u16() & 0x03FF, p.next_u64())
    });

    let mut prev = UUID7_STATE.load(Ordering::Relaxed);
    let (ts_ms, seq) = loop {
        let prev_ts = prev >> 12;
        let prev_seq = prev & 0x0FFF;
        let (next_ts, next_seq) = if now_ms > prev_ts {
            (now_ms, u64::from(rand_seq))
        } else if prev_seq < 0x0FFF {
            (prev_ts, prev_seq + 1)
        } else {
            (prev_ts + 1, 0)
        };
        let next_state = (next_ts << 12) | next_seq;
        match UUID7_STATE.compare_exchange_weak(
            prev,
            next_state,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break (next_ts, next_seq),
            Err(actual) => prev = actual,
        }
    };

    let high = ((ts_ms & 0x0000_FFFF_FFFF_FFFF) << 16) | 0x7000 | (seq & 0x0FFF);
    let low = (rand_b & 0x3FFF_FFFF_FFFF_FFFF) | 0x8000_0000_0000_0000;
    ((high as u128) << 64) | (low as u128)
}

/// Generate a monotonically ordered RFC 9562 Version 7 UUID as a 36-character hyphenated string.
#[inline]
pub fn uuid7_str() -> String {
    format_uuid_hyphenated(uuid7_u128())
}

/// Generate a URL-safe NanoID string of length `size` (1..=1024, default 21).
pub fn nanoid(size: usize) -> Result<String, String> {
    if size == 0 || size > 1024 {
        return Err(format!(
            "nanoid size must be between 1 and 1024, got {size}"
        ));
    }
    let mut raw = vec![0u8; size];
    THREAD_ENTROPY.with(|pool| pool.borrow_mut().fill_bytes(&mut raw));
    let mut out = String::with_capacity(size);
    for byte in raw {
        out.push(NANOID_ALPHABET[(byte & 63) as usize] as char);
    }
    Ok(out)
}

#[inline]
fn format_uuid_hyphenated(value: u128) -> String {
    let bytes = value.to_be_bytes();
    let mut out = [0u8; 36];
    let mut out_idx = 0;
    for (idx, &b) in bytes.iter().enumerate() {
        if idx == 4 || idx == 6 || idx == 8 || idx == 10 {
            out[out_idx] = b'-';
            out_idx += 1;
        }
        out[out_idx] = HEX_LOWER[(b >> 4) as usize];
        out[out_idx + 1] = HEX_LOWER[(b & 0x0F) as usize];
        out_idx += 2;
    }
    String::from_utf8(out.to_vec()).unwrap_or_default()
}

#[inline]
fn format_uuid_hex(value: u128) -> String {
    let bytes = value.to_be_bytes();
    let mut out = [0u8; 32];
    for (idx, &b) in bytes.iter().enumerate() {
        out[idx * 2] = HEX_LOWER[(b >> 4) as usize];
        out[idx * 2 + 1] = HEX_LOWER[(b & 0x0F) as usize];
    }
    String::from_utf8(out.to_vec()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid4_conforms_to_rfc9562_version_and_variant() {
        let val = uuid4_u128();
        let version = (val >> 76) & 0xF;
        let variant = (val >> 62) & 0x3;
        assert_eq!(version, 4);
        assert_eq!(variant, 0b10);

        let hyphenated = uuid4_str();
        assert_eq!(hyphenated.len(), 36);
        assert_eq!(&hyphenated[14..15], "4");

        let hex_str = uuid4_hex();
        assert_eq!(hex_str.len(), 32);
    }

    #[test]
    fn uuid7_is_strictly_monotonic_and_version_7() {
        let mut prev = 0_u128;
        for _ in 0..2_000 {
            let current = uuid7_u128();
            let version = (current >> 76) & 0xF;
            let variant = (current >> 62) & 0x3;
            assert_eq!(version, 7);
            assert_eq!(variant, 0b10);
            assert!(current > prev, "expected {current:#x} > {prev:#x}");
            prev = current;
        }

        let s = uuid7_str();
        assert_eq!(s.len(), 36);
        assert_eq!(&s[14..15], "7");
    }

    #[test]
    fn nanoid_generates_url_safe_strings_and_validates_bounds() {
        let id = nanoid(21).unwrap();
        assert_eq!(id.len(), 21);
        assert!(
            id.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        );
        assert!(nanoid(0).is_err());
        assert!(nanoid(1025).is_err());
    }
}
