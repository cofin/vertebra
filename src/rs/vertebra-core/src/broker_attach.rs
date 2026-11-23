//! Peer-credential authenticated Unix domain socket handshake and `SCM_RIGHTS` SHM FD delivery.

use std::fmt;
#[cfg(unix)]
use std::io::{ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
#[cfg(all(unix, test))]
use std::sync::atomic::AtomicUsize;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::{Arc, Mutex};
#[cfg(unix)]
use std::thread::JoinHandle;
#[cfg(not(unix))]
use std::time::Duration;
#[cfg(unix)]
use std::time::{Duration, Instant};

#[cfg(unix)]
use sha2::{Digest, Sha256};

#[cfg(unix)]
fn validate_timeout(timeout: Duration) -> Result<(), AttachError> {
    if timeout.is_zero() {
        return Err(AttachError::Timeout(
            "attach timeout must be positive".to_string(),
        ));
    }
    Ok(())
}

const HANDSHAKE_MAGIC: [u8; 4] = *b"VTBA";
#[cfg(unix)]
const HANDSHAKE_VERSION: u16 = 1;
#[cfg(unix)]
const MAX_SECRET_BYTES: usize = 256;
#[cfg(unix)]
const ACK_STATUS_OK: u8 = 0;
#[cfg(unix)]
const ACK_STATUS_REJECTED: u8 = 1;

/// Typed errors returned during authenticated broker Unix domain socket attachment.
#[derive(Debug, thiserror::Error)]
pub enum AttachError {
    #[error("broker authentication failed: {0}")]
    AuthFailed(&'static str),
    #[error("insecure broker socket permissions: {0}")]
    InsecurePermissions(String),
    #[error("broker attach handshake timed out: {0}")]
    Timeout(String),
    #[error("invalid broker attach handshake protocol frame: {0}")]
    Protocol(String),
    #[error("broker attach I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("authenticated UDS broker attach is only supported on Unix platforms")]
    UnsupportedPlatform,
}

#[cfg(unix)]
#[derive(Clone)]
struct RedactedSecret {
    bytes: Vec<u8>,
}

#[cfg(unix)]
impl fmt::Debug for RedactedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

#[cfg(unix)]
impl RedactedSecret {
    fn new(secret: Vec<u8>) -> Result<Self, AttachError> {
        if secret.is_empty() || secret.len() > MAX_SECRET_BYTES {
            return Err(AttachError::AuthFailed(
                "attach secret length must be in 1..=256 bytes",
            ));
        }
        Ok(Self { bytes: secret })
    }

    fn matches_constant_time(&self, candidate: &[u8]) -> bool {
        let expected_hash = Sha256::digest(&self.bytes);
        let candidate_hash = Sha256::digest(candidate);
        let mut diff = u8::from(self.bytes.len() != candidate.len());
        for (a, b) in expected_hash.iter().zip(candidate_hash.iter()) {
            diff |= std::hint::black_box(a ^ b);
        }
        std::hint::black_box(diff) == 0
    }
}

#[cfg(unix)]
fn current_uid() -> u32 {
    // SAFETY: `geteuid()` has no preconditions and always succeeds on Unix.
    unsafe { libc::geteuid() }
}

#[cfg(unix)]
fn check_parent_dir_security(socket_path: &Path) -> Result<(), AttachError> {
    let parent = socket_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| {
            AttachError::InsecurePermissions(
                "socket path must include a parent directory".to_string(),
            )
        })?;
    let meta = std::fs::symlink_metadata(parent).map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            AttachError::InsecurePermissions(format!(
                "socket parent directory {} does not exist",
                parent.display()
            ))
        } else {
            AttachError::Io(e)
        }
    })?;
    if meta.file_type().is_symlink() {
        return Err(AttachError::InsecurePermissions(format!(
            "socket parent directory {} must not be a symlink",
            parent.display()
        )));
    }
    if !meta.is_dir() {
        return Err(AttachError::InsecurePermissions(format!(
            "socket parent {} is not a directory",
            parent.display()
        )));
    }
    let expected_uid = current_uid();
    if meta.uid() != expected_uid {
        return Err(AttachError::InsecurePermissions(format!(
            "socket parent directory {} uid {} does not match euid {expected_uid}",
            parent.display(),
            meta.uid()
        )));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode != 0o700 {
        return Err(AttachError::InsecurePermissions(format!(
            "socket parent directory {} must have mode 0700, found {mode:04o}",
            parent.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn check_socket_security(socket_path: &Path) -> Result<(), AttachError> {
    check_parent_dir_security(socket_path)?;
    let meta = std::fs::symlink_metadata(socket_path)?;
    if meta.file_type().is_symlink() {
        return Err(AttachError::InsecurePermissions(format!(
            "socket {} must not be a symlink",
            socket_path.display()
        )));
    }
    if !meta.file_type().is_socket() {
        return Err(AttachError::InsecurePermissions(format!(
            "path {} is not a Unix domain socket",
            socket_path.display()
        )));
    }
    let expected_uid = current_uid();
    if meta.uid() != expected_uid {
        return Err(AttachError::InsecurePermissions(format!(
            "socket {} uid {} does not match euid {expected_uid}",
            socket_path.display(),
            meta.uid()
        )));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(AttachError::InsecurePermissions(format!(
            "socket {} must have mode 0600, found {mode:04o}",
            socket_path.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_peer_uid(stream: &UnixStream) -> Result<(), AttachError> {
    let expected_uid = current_uid();
    #[cfg(target_os = "linux")]
    {
        // SAFETY: Zeroed `ucred` is valid for `getsockopt(SO_PEERCRED)`.
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        // SAFETY: `stream.as_raw_fd()` is an open Unix stream socket, and `cred` has `len` bytes.
        let rc = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast::<libc::c_void>(),
                &mut len,
            )
        };
        if rc != 0 {
            return Err(AttachError::Io(std::io::Error::last_os_error()));
        }
        if cred.uid != expected_uid {
            return Err(AttachError::AuthFailed("peer UID mismatch"));
        }
        Ok(())
    }
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        let mut uid: libc::uid_t = 0;
        let mut gid: libc::gid_t = 0;
        // SAFETY: `stream.as_raw_fd()` is an open Unix stream socket.
        let rc = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) };
        if rc != 0 {
            return Err(AttachError::Io(std::io::Error::last_os_error()));
        }
        if uid != expected_uid {
            return Err(AttachError::AuthFailed("peer UID mismatch"));
        }
        Ok(())
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    )))]
    {
        let _ = (stream, expected_uid);
        Err(AttachError::UnsupportedPlatform)
    }
}

#[cfg(unix)]
fn dup_cloexec(fd: RawFd) -> Result<OwnedFd, AttachError> {
    // SAFETY: `fd` must refer to a valid open descriptor supplied by caller.
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        return Err(AttachError::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: `duplicated` is newly allocated and uniquely owned.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

#[cfg(unix)]
fn send_ack_and_fd(stream: &UnixStream, shm_fd: RawFd, shm_size: u64) -> Result<(), AttachError> {
    let mut payload = [0u8; 10];
    payload[0] = ACK_STATUS_OK;
    payload[1] = 0;
    payload[2..10].copy_from_slice(&shm_size.to_le_bytes());

    let mut iov = libc::iovec {
        iov_base: payload.as_mut_ptr().cast::<libc::c_void>(),
        iov_len: payload.len(),
    };

    #[repr(C)]
    union CmsgSpace {
        hdr: libc::cmsghdr,
        buf: [u8; 128],
    }
    // SAFETY: Zeroed union satisfies alignment and initialization of `cmsghdr`.
    let mut cmsg_space = CmsgSpace { buf: [0u8; 128] };
    // SAFETY: `CMSG_SPACE` evaluates control message space for one `RawFd`.
    let needed = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    if needed > 128 {
        return Err(AttachError::Protocol(
            "control buffer too small".to_string(),
        ));
    }

    // SAFETY: Standard POSIX `msghdr` initialization referencing stack `iov` and `cmsg_space`.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    // SAFETY: Control pointer cast matches `CMSG_FIRSTHDR` expectations.
    msg.msg_control = unsafe { cmsg_space.buf.as_mut_ptr() }.cast::<libc::c_void>();
    msg.msg_controllen = needed as _;

    // SAFETY: `msg` has aligned `msg_control` capacity `needed`.
    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null() {
        return Err(AttachError::Protocol(
            "CMSG_FIRSTHDR returned null".to_string(),
        ));
    }
    // SAFETY: Populating `SCM_RIGHTS` header and payload with `shm_fd`.
    unsafe {
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<RawFd>() as u32) as _;
        let data_ptr = libc::CMSG_DATA(cmsg);
        std::ptr::copy_nonoverlapping(
            (&shm_fd as *const RawFd).cast::<u8>(),
            data_ptr,
            std::mem::size_of::<RawFd>(),
        );
    }

    let sent = loop {
        // SAFETY: `stream` and `msg` remain valid for the duration of `sendmsg`.
        let rc = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == ErrorKind::Interrupted {
                continue;
            }
            if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
                return Err(AttachError::Timeout(
                    "timed out sending SHM descriptor".to_string(),
                ));
            }
            return Err(AttachError::Io(err));
        }
        break rc as usize;
    };
    if sent == 0 {
        return Err(AttachError::Protocol(
            "zero-byte sendmsg while sending SHM descriptor".to_string(),
        ));
    }
    if sent < payload.len()
        && let Err(err) = (&*stream).write_all(&payload[sent..])
    {
        if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
            return Err(AttachError::Timeout(
                "timed out sending trailing attach ack bytes".to_string(),
            ));
        }
        return Err(AttachError::Io(err));
    }
    Ok(())
}

#[cfg(unix)]
fn recv_ack_and_fd(stream: &UnixStream) -> Result<(OwnedFd, usize), AttachError> {
    let mut payload = [0u8; 10];

    #[repr(C)]
    union CmsgSpace {
        hdr: libc::cmsghdr,
        buf: [u8; 128],
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    let recv_flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    )))]
    let recv_flags = 0;

    let (nread_usize, mut cmsg_space, msg_flags, msg_controllen) = loop {
        let mut iov = libc::iovec {
            iov_base: payload.as_mut_ptr().cast::<libc::c_void>(),
            iov_len: payload.len(),
        };
        let mut cmsg_space = CmsgSpace { buf: [0u8; 128] };
        // SAFETY: Zero-initialized `msghdr` pointing to valid `iov` and `cmsg_space`.
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        // SAFETY: Aligned control buffer matching `cmsghdr`.
        msg.msg_control = unsafe { cmsg_space.buf.as_mut_ptr() }.cast::<libc::c_void>();
        msg.msg_controllen = 128 as _;

        // SAFETY: Receiving handshake response + optional `SCM_RIGHTS` FD.
        let nread = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, recv_flags) };
        if nread < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == ErrorKind::Interrupted {
                continue;
            }
            if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
                return Err(AttachError::Timeout(
                    "timed out receiving SHM descriptor".to_string(),
                ));
            }
            return Err(AttachError::Io(err));
        }
        if nread == 0 {
            return Err(AttachError::Protocol(
                "server closed UDS connection during attach handshake".to_string(),
            ));
        }
        break (
            nread as usize,
            cmsg_space,
            msg.msg_flags,
            msg.msg_controllen,
        );
    };

    let mut received_fds: Vec<OwnedFd> = Vec::new();
    // SAFETY: Reading control header fields populated by `recvmsg`.
    let ctrl_base = unsafe { cmsg_space.buf.as_ptr() } as usize;
    // SAFETY: `buf` is 128 bytes long; `msg_controllen` is clamped to buffer capacity.
    #[allow(clippy::unnecessary_cast)]
    let ctrl_len = unsafe { (msg_controllen as usize).min(cmsg_space.buf.len()) };
    let ctrl_end = ctrl_base + ctrl_len;
    // SAFETY: `CMSG_LEN(0)` evaluates the aligned header size with zero data bytes.
    let hdr_data_offset = unsafe { libc::CMSG_LEN(0) } as usize;

    let mut probe_msg: libc::msghdr = unsafe { std::mem::zeroed() };
    // SAFETY: Pointing `probe_msg` only for `CMSG_FIRSTHDR`/`CMSG_NXTHDR` cursor traversal.
    probe_msg.msg_control = unsafe { cmsg_space.buf.as_mut_ptr() }.cast::<libc::c_void>();
    probe_msg.msg_controllen = ctrl_len as _;

    // SAFETY: Walking validated `cmsg` headers inside `cmsg_space`.
    let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&probe_msg) };
    while !cmsg.is_null() {
        let cmsg_addr = cmsg as usize;
        if cmsg_addr < ctrl_base || cmsg_addr + std::mem::size_of::<libc::cmsghdr>() > ctrl_end {
            break;
        }
        // SAFETY: `cmsg` lies within `cmsg_space.buf[..ctrl_len]`.
        let hdr = unsafe { &*cmsg };
        let cmsg_len = hdr.cmsg_len as usize;
        if hdr.cmsg_level == libc::SOL_SOCKET
            && hdr.cmsg_type == libc::SCM_RIGHTS
            && cmsg_len >= hdr_data_offset
        {
            // SAFETY: `cmsg` has valid length at least `CMSG_LEN(0)`.
            let data_ptr = unsafe { libc::CMSG_DATA(cmsg) };
            let max_avail = ctrl_end.saturating_sub(data_ptr as usize);
            let payload_bytes = cmsg_len.saturating_sub(hdr_data_offset).min(max_avail);
            let fd_count = payload_bytes / std::mem::size_of::<RawFd>();
            for i in 0..fd_count {
                let mut raw_fd: RawFd = -1;
                // SAFETY: `i * size_of::<RawFd>() + size_of::<RawFd>() <= max_avail`.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data_ptr.add(i * std::mem::size_of::<RawFd>()),
                        (&mut raw_fd as *mut RawFd).cast::<u8>(),
                        std::mem::size_of::<RawFd>(),
                    );
                }
                if raw_fd >= 0 {
                    // SAFETY: `raw_fd` is newly installed in this process via `SCM_RIGHTS`.
                    unsafe {
                        let _ = libc::fcntl(raw_fd, libc::F_SETFD, libc::FD_CLOEXEC);
                        received_fds.push(OwnedFd::from_raw_fd(raw_fd));
                    }
                }
            }
        }
        // SAFETY: Advancing to subsequent control message header within `msg_control`.
        cmsg = unsafe { libc::CMSG_NXTHDR(&probe_msg, cmsg) };
    }

    if (msg_flags & libc::MSG_CTRUNC) != 0 {
        return Err(AttachError::Protocol(
            "SCM_RIGHTS control data truncated (MSG_CTRUNC)".to_string(),
        ));
    }
    if payload[0] != ACK_STATUS_OK {
        return Err(AttachError::AuthFailed("attach secret verification failed"));
    }
    if nread_usize < payload.len()
        && let Err(err) = (&*stream).read_exact(&mut payload[nread_usize..])
    {
        if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
            return Err(AttachError::Timeout(
                "timed out receiving attach response frame".to_string(),
            ));
        }
        return Err(AttachError::Protocol(format!(
            "truncated attach response frame: {err}"
        )));
    }
    if received_fds.len() > 1 {
        return Err(AttachError::Protocol(format!(
            "expected single SCM_RIGHTS SHM descriptor, got {}",
            received_fds.len()
        )));
    }
    let shm_size_u64 = u64::from_le_bytes(payload[2..10].try_into().expect("8-byte slice"));
    let shm_size = usize::try_from(shm_size_u64)
        .map_err(|_| AttachError::Protocol("SHM size overflows usize".to_string()))?;

    let owned = received_fds.pop().ok_or_else(|| {
        AttachError::Protocol("server response lacked SCM_RIGHTS SHM descriptor".to_string())
    })?;
    Ok((owned, shm_size))
}

#[cfg(unix)]
fn read_exact_interruptible(
    stream: &mut UnixStream,
    buf: &mut [u8],
    deadline: Instant,
    stop_flag: Option<&AtomicBool>,
    timeout_msg: &str,
    eof_prefix: &str,
) -> Result<(), AttachError> {
    let mut offset = 0usize;
    while offset < buf.len() {
        if stop_flag.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            return Err(AttachError::Timeout("attach server stopping".to_string()));
        }
        match stream.read(&mut buf[offset..]) {
            Ok(0) => {
                return Err(AttachError::Protocol(format!(
                    "{eof_prefix}: unexpected EOF after {offset} of {} bytes",
                    buf.len()
                )));
            }
            Ok(n) => {
                offset += n;
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(AttachError::Timeout(timeout_msg.to_string()));
                }
                let rem = deadline.saturating_duration_since(now);
                let slice_ms = rem
                    .min(Duration::from_millis(50))
                    .as_millis()
                    .clamp(1, i32::MAX as u128) as i32;
                let mut pfd = libc::pollfd {
                    fd: stream.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: `pfd` covers the live socket descriptor for a bounded poll slice.
                let rc = unsafe { libc::poll(&mut pfd, 1, slice_ms) };
                if rc < 0 {
                    let io_err = std::io::Error::last_os_error();
                    if io_err.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(AttachError::Io(io_err));
                }
            }
            Err(err) => {
                return Err(AttachError::Protocol(format!("{eof_prefix}: {err}")));
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn handle_client_connection(
    mut stream: UnixStream,
    secret: &RedactedSecret,
    shm_fd: RawFd,
    shm_size: u64,
    timeout: Duration,
    stop_flag: Option<&AtomicBool>,
) -> Result<(), AttachError> {
    validate_timeout(timeout)?;
    let deadline = Instant::now() + timeout;
    verify_peer_uid(&stream)?;
    stream.set_nonblocking(true)?;

    let mut header = [0u8; 8];
    read_exact_interruptible(
        &mut stream,
        &mut header,
        deadline,
        stop_flag,
        "timed out reading attach handshake header",
        "truncated attach handshake header",
    )?;
    if header[0..4] != HANDSHAKE_MAGIC {
        return Err(AttachError::Protocol(
            "invalid attach handshake magic".to_string(),
        ));
    }
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != HANDSHAKE_VERSION {
        return Err(AttachError::Protocol(format!(
            "unsupported attach handshake version {version}"
        )));
    }
    let secret_len = usize::from(u16::from_le_bytes([header[6], header[7]]));
    if secret_len == 0 || secret_len > MAX_SECRET_BYTES {
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_write_timeout(Some(
            deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1)),
        ));
        let _ = stream.write_all(&[ACK_STATUS_REJECTED]);
        return Err(AttachError::AuthFailed("invalid attach secret length"));
    }

    let mut candidate = vec![0u8; secret_len];
    read_exact_interruptible(
        &mut stream,
        &mut candidate,
        deadline,
        stop_flag,
        "timed out reading attach secret payload",
        "truncated attach secret payload",
    )?;

    stream.set_nonblocking(false)?;
    let write_rem = deadline.saturating_duration_since(Instant::now());
    if write_rem.is_zero() {
        return Err(AttachError::Timeout(
            "timed out before writing attach handshake response".to_string(),
        ));
    }
    stream.set_write_timeout(Some(write_rem))?;

    if !secret.matches_constant_time(&candidate) {
        let _ = stream.write_all(&[ACK_STATUS_REJECTED]);
        return Err(AttachError::AuthFailed("attach secret verification failed"));
    }

    send_ack_and_fd(&stream, shm_fd, shm_size)
}

/// Authenticated Unix domain socket server distributing SHM descriptors via `SCM_RIGHTS`.
pub struct BrokerAttachServer {
    socket_path: PathBuf,
    #[cfg(unix)]
    listener: UnixListener,
    #[cfg(unix)]
    shm_fd: OwnedFd,
    #[cfg(unix)]
    shm_size: u64,
    #[cfg(unix)]
    secret: RedactedSecret,
}

impl fmt::Debug for BrokerAttachServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("BrokerAttachServer");
        s.field("socket_path", &self.socket_path);
        #[cfg(unix)]
        s.field("secret", &self.secret);
        s.finish()
    }
}

impl BrokerAttachServer {
    /// Validate permissions, bind a `0600` Unix domain socket, and hold an `FD_CLOEXEC` duplicate
    /// of `shm_fd`.
    #[cfg(unix)]
    pub fn bind(
        socket_path: impl AsRef<Path>,
        secret: &[u8],
        shm_fd: RawFd,
        shm_size: usize,
    ) -> Result<Self, AttachError> {
        let path = socket_path.as_ref().to_path_buf();
        let redacted = RedactedSecret::new(secret.to_vec())?;
        check_parent_dir_security(&path)?;
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if meta.file_type().is_socket() {
                std::fs::remove_file(&path)?;
            } else {
                return Err(AttachError::InsecurePermissions(format!(
                    "refusing to overwrite non-socket path {}",
                    path.display()
                )));
            }
        }
        let owned_fd = dup_cloexec(shm_fd)?;
        let effective_size = if shm_size > 0 {
            shm_size as u64
        } else {
            // SAFETY: `owned_fd` is an open file/SHM descriptor.
            let mut st: libc::stat = unsafe { std::mem::zeroed() };
            // SAFETY: `fstat` writes into initialized `st`.
            if unsafe { libc::fstat(owned_fd.as_raw_fd(), &mut st) } == 0 && st.st_size > 0 {
                st.st_size as u64
            } else {
                0
            }
        };
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        Ok(Self {
            socket_path: path,
            listener,
            shm_fd: owned_fd,
            shm_size: effective_size,
            secret: redacted,
        })
    }

    #[cfg(not(unix))]
    pub fn bind(
        _socket_path: impl AsRef<Path>,
        _secret: &[u8],
        _shm_fd: i32,
        _shm_size: usize,
    ) -> Result<Self, AttachError> {
        Err(AttachError::UnsupportedPlatform)
    }

    /// Return the filesystem path of the listening Unix domain socket.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Accept and authenticate a single client connection within `timeout`, delivering `shm_fd`.
    #[cfg(unix)]
    pub fn serve_one(&self, timeout: Duration) -> Result<(), AttachError> {
        validate_timeout(timeout)?;
        let stream = self.accept_with_timeout(timeout)?;
        handle_client_connection(
            stream,
            &self.secret,
            self.shm_fd.as_raw_fd(),
            self.shm_size,
            timeout,
            None,
        )
    }

    #[cfg(not(unix))]
    pub fn serve_one(&self, _timeout: Duration) -> Result<(), AttachError> {
        Err(AttachError::UnsupportedPlatform)
    }

    #[cfg(unix)]
    fn accept_with_timeout(&self, timeout: Duration) -> Result<UnixStream, AttachError> {
        let deadline = std::time::Instant::now() + timeout;
        self.listener.set_nonblocking(true)?;
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let _ = stream.set_nonblocking(false);
                    return Ok(stream);
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return Err(AttachError::Timeout(
                            "timed out waiting for UDS attach connection".to_string(),
                        ));
                    }
                    let rem = deadline.saturating_duration_since(now);
                    let ms = rem.as_millis().min(i32::MAX as u128) as i32;
                    let mut pfd = libc::pollfd {
                        fd: self.listener.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    };
                    // SAFETY: `pfd` points to one valid `pollfd` covering the open listener fd.
                    let rc = unsafe { libc::poll(&mut pfd, 1, ms.max(1)) };
                    if rc < 0 {
                        let io_err = std::io::Error::last_os_error();
                        if io_err.kind() == ErrorKind::Interrupted {
                            continue;
                        }
                        return Err(AttachError::Io(io_err));
                    }
                }
                Err(err) => return Err(AttachError::Io(err)),
            }
        }
    }

    /// Spawn a background accept loop servicing attach requests until [`AttachServerHandle::stop`].
    #[cfg(unix)]
    pub fn start_background(self) -> Result<AttachServerHandle, AttachError> {
        let stop_flag = Arc::new(AtomicBool::new(false));
        #[cfg(test)]
        let accepted_count = Arc::new(AtomicUsize::new(0));
        let stop_worker = Arc::clone(&stop_flag);
        #[cfg(test)]
        let accepted_worker = Arc::clone(&accepted_count);
        let socket_path = self.socket_path.clone();
        self.listener.set_nonblocking(true)?;
        let handle = std::thread::Builder::new()
            .name("vtb-broker-uds".to_string())
            .spawn(move || {
                while !stop_worker.load(Ordering::Acquire) {
                    let mut pfd = libc::pollfd {
                        fd: self.listener.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    };
                    // SAFETY: `pfd` covers the live listener descriptor for a 50ms poll slice.
                    let rc = unsafe { libc::poll(&mut pfd, 1, 50) };
                    if rc <= 0 || stop_worker.load(Ordering::Acquire) {
                        continue;
                    }
                    if let Ok((stream, _)) = self.listener.accept() {
                        #[cfg(test)]
                        accepted_worker.fetch_add(1, Ordering::Release);
                        let _ = handle_client_connection(
                            stream,
                            &self.secret,
                            self.shm_fd.as_raw_fd(),
                            self.shm_size,
                            Duration::from_secs(5),
                            Some(&stop_worker),
                        );
                    }
                }
            })?;
        Ok(AttachServerHandle {
            socket_path,
            stop_flag,
            #[cfg(test)]
            accepted_count,
            thread: Mutex::new(Some(handle)),
        })
    }

    #[cfg(not(unix))]
    pub fn start_background(self) -> Result<AttachServerHandle, AttachError> {
        Err(AttachError::UnsupportedPlatform)
    }
}

/// Lifecycle handle for a background [`BrokerAttachServer`].
#[derive(Debug)]
pub struct AttachServerHandle {
    socket_path: PathBuf,
    #[cfg(unix)]
    stop_flag: Arc<AtomicBool>,
    #[cfg(all(unix, test))]
    accepted_count: Arc<AtomicUsize>,
    #[cfg(unix)]
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl AttachServerHandle {
    /// Return the listening Unix domain socket path.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Stop the accept loop thread and remove the Unix domain socket file.
    pub fn stop(&self) {
        #[cfg(unix)]
        {
            self.stop_flag.store(true, Ordering::Release);
            if let Some(handle) = self
                .thread
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                let _ = handle.join();
            }
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}

impl Drop for AttachServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Client for authenticating to [`BrokerAttachServer`] and receiving the SHM descriptor.
pub struct BrokerAttachClient;

impl BrokerAttachClient {
    /// Verify local socket directory and socket permissions before connecting, authenticate peer
    /// UID + constant-time secret, and return `(OwnedFd, shm_size)`.
    #[cfg(unix)]
    pub fn attach(
        socket_path: impl AsRef<Path>,
        secret: &[u8],
        timeout: Duration,
    ) -> Result<(OwnedFd, usize), AttachError> {
        validate_timeout(timeout)?;
        let redacted = RedactedSecret::new(secret.to_vec())?;
        let path = socket_path.as_ref();
        check_socket_security(path)?;

        let mut stream = UnixStream::connect(path)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        verify_peer_uid(&stream)?;

        let mut frame = Vec::with_capacity(8 + redacted.bytes.len());
        frame.extend_from_slice(&HANDSHAKE_MAGIC);
        frame.extend_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
        frame.extend_from_slice(&(redacted.bytes.len() as u16).to_le_bytes());
        frame.extend_from_slice(&redacted.bytes);
        if let Err(err) = stream.write_all(&frame) {
            if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
                return Err(AttachError::Timeout(
                    "timed out sending attach handshake".to_string(),
                ));
            }
            return Err(AttachError::Io(err));
        }

        recv_ack_and_fd(&stream)
    }

    #[cfg(not(unix))]
    pub fn attach(
        _socket_path: impl AsRef<Path>,
        _secret: &[u8],
        _timeout: Duration,
    ) -> Result<(i32, usize), AttachError> {
        Err(AttachError::UnsupportedPlatform)
    }

    /// Encode a raw client handshake frame for protocol boundary testing.
    #[must_use]
    pub fn encode_handshake_frame(version: u16, secret: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(8 + secret.len());
        frame.extend_from_slice(&HANDSHAKE_MAGIC);
        frame.extend_from_slice(&version.to_le_bytes());
        frame.extend_from_slice(&(secret.len() as u16).to_le_bytes());
        frame.extend_from_slice(secret);
        frame
    }

    /// Extract the raw file descriptor after successful UNIX `attach`.
    #[cfg(unix)]
    #[must_use]
    pub fn into_raw_fd(owned: OwnedFd) -> RawFd {
        owned.into_raw_fd()
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn recv_ack_and_fd_rejects_multiple_fds_and_closes_all() {
        let (server, client) = UnixStream::pair().expect("unix stream pair");
        let mut pipe_fds: [RawFd; 2] = [-1; 2];
        // SAFETY: `pipe_fds` points to 2 writable `RawFd` slots.
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        // SAFETY: Both pipe descriptors were created cleanly by `libc::pipe`.
        let (read_fd, write_fd) = unsafe {
            (
                OwnedFd::from_raw_fd(pipe_fds[0]),
                OwnedFd::from_raw_fd(pipe_fds[1]),
            )
        };

        let mut payload = [0u8; 10];
        payload[0] = ACK_STATUS_OK;
        payload[2..10].copy_from_slice(&4096u64.to_le_bytes());

        let mut iov = libc::iovec {
            iov_base: payload.as_mut_ptr().cast::<libc::c_void>(),
            iov_len: payload.len(),
        };

        #[repr(C)]
        union CmsgSpace {
            hdr: libc::cmsghdr,
            buf: [u8; 128],
        }
        let mut cmsg_space = CmsgSpace { buf: [0u8; 128] };
        let two_fds = [write_fd.as_raw_fd(), write_fd.as_raw_fd()];
        let data_bytes = std::mem::size_of_val(&two_fds) as u32;
        // SAFETY: Evaluating `CMSG_SPACE` for two `RawFd` entries.
        let needed = unsafe { libc::CMSG_SPACE(data_bytes) } as usize;

        // SAFETY: Initializing `msghdr` with `iov` and `cmsg_space`.
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        // SAFETY: Control buffer matches `needed` bytes.
        msg.msg_control = unsafe { cmsg_space.buf.as_mut_ptr() }.cast::<libc::c_void>();
        msg.msg_controllen = needed as _;

        // SAFETY: Populating one `SCM_RIGHTS` header carrying two FDs.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            assert!(!cmsg.is_null());
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(data_bytes) as _;
            std::ptr::copy_nonoverlapping(
                two_fds.as_ptr().cast::<u8>(),
                libc::CMSG_DATA(cmsg),
                data_bytes as usize,
            );
        }

        // SAFETY: `server` and `msg` are valid.
        assert_eq!(unsafe { libc::sendmsg(server.as_raw_fd(), &msg, 0) }, 10);
        drop(write_fd);

        let err = recv_ack_and_fd(&client).expect_err("extra FDs must be rejected");
        assert!(matches!(err, AttachError::Protocol(_)));

        // SAFETY: `read_fd` is owned exclusively by this test.
        let mut pipe_reader = std::fs::File::from(read_fd);
        let mut probe = [0u8; 1];
        let n = pipe_reader.read(&mut probe).expect("read pipe EOF");
        assert_eq!(n, 0, "all received extra FDs must be closed on error");
    }

    #[test]
    fn recv_ack_and_fd_reassembles_split_ack_payload() {
        let (server, client) = UnixStream::pair().expect("unix stream pair");
        let shm = crate::shm::ShmRegion::create(4096).expect("anon shm");
        let raw_fd = shm.handle().expect("shm handle");

        let mut head = [0u8; 4];
        head[0] = ACK_STATUS_OK;
        let full_size = 8192u64.to_le_bytes();
        head[2..4].copy_from_slice(&full_size[0..2]);

        let mut iov = libc::iovec {
            iov_base: head.as_mut_ptr().cast::<libc::c_void>(),
            iov_len: head.len(),
        };
        #[repr(C)]
        union CmsgSpace {
            hdr: libc::cmsghdr,
            buf: [u8; 128],
        }
        let mut cmsg_space = CmsgSpace { buf: [0u8; 128] };
        let data_bytes = std::mem::size_of::<RawFd>() as u32;
        // SAFETY: Evaluating `CMSG_SPACE` for one `RawFd`.
        let needed = unsafe { libc::CMSG_SPACE(data_bytes) } as usize;
        // SAFETY: Zeroed `msghdr`.
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        // SAFETY: Control buffer aligned to `cmsghdr`.
        msg.msg_control = unsafe { cmsg_space.buf.as_mut_ptr() }.cast::<libc::c_void>();
        msg.msg_controllen = needed as _;
        // SAFETY: Writing single `SCM_RIGHTS` FD.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(data_bytes) as _;
            std::ptr::copy_nonoverlapping(
                (&raw_fd as *const RawFd).cast::<u8>(),
                libc::CMSG_DATA(cmsg),
                std::mem::size_of::<RawFd>(),
            );
            assert_eq!(libc::sendmsg(server.as_raw_fd(), &msg, 0), 4);
        }
        (&server).write_all(&full_size[2..]).expect("tail write");

        let (_fd, size) = recv_ack_and_fd(&client).expect("split ack reassembly");
        assert_eq!(size, 8192);
    }

    #[test]
    fn zero_timeout_returns_timeout_and_stop_interrupts_stalled_client() {
        let dir = format!("/tmp/vtb-att-unit-{}", std::process::id());
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create dir");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).expect("chmod dir");
        let sock = PathBuf::from(&dir).join("broker.sock");
        let shm = crate::shm::ShmRegion::create(4096).expect("anon shm");
        let server =
            BrokerAttachServer::bind(&sock, b"unit-secret", shm.handle().unwrap(), 4096).unwrap();

        let err = BrokerAttachClient::attach(&sock, b"unit-secret", Duration::ZERO).unwrap_err();
        assert!(matches!(err, AttachError::Timeout(_)));

        let handle = server.start_background().expect("start background");
        let stalled_client = UnixStream::connect(&sock).expect("connect stalled client");
        let accept_wait = Instant::now();
        while handle.accepted_count.load(Ordering::Acquire) == 0 {
            assert!(
                accept_wait.elapsed() < Duration::from_secs(1),
                "background server did not accept stalled client within 1s"
            );
            std::thread::yield_now();
        }

        let start = Instant::now();
        handle.stop();
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "stop() stalled waiting on idle client: {:?}",
            start.elapsed()
        );
        drop(stalled_client);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
