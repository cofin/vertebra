use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::Arc;

use vertebra_core::buffer::{ChannelMode, RingLayout};
use vertebra_core::channel::{SpscReceiver, SpscRing, SpscSender};
use vertebra_core::notify::Notifier;
use vertebra_core::shm::ShmRegion;

pub struct Worker {
    pub req_tx: SpscSender,
    pub resp_rx: SpscReceiver,
    pub pid: u32,
    pub process: std::process::Child,
    pub notifier: Notifier,
}

pub struct WorkerManager {
    workers: usize,
    app_path: String,
    python_path: String,
    worker_handles: Vec<Worker>,
}

impl WorkerManager {
    pub fn new(workers: usize, app_path: &str, python_path: &str) -> Self {
        Self {
            workers,
            app_path: app_path.to_string(),
            python_path: python_path.to_string(),
            worker_handles: Vec::new(),
        }
    }

    pub fn worker_count(&self) -> usize {
        self.workers
    }

    pub fn into_workers(self) -> Vec<Worker> {
        self.worker_handles
    }

    pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!(
            "Starting {} workers for app: {}",
            self.workers, self.app_path
        );

        let layout = RingLayout::new(1024 * 64, 128, ChannelMode::Spsc); // 64KB slots, 128 slots

        for i in 0..self.workers {
            let req_region = Arc::new(ShmRegion::create(layout.total_size)?);
            let resp_region = Arc::new(ShmRegion::create(layout.total_size)?);

            // Server side: Req Sender, Resp Receiver
            // Worker side: Req Receiver, Resp Sender
            let req_ring = unsafe { SpscRing::initialize(req_region.clone(), layout, 0, 0) }?;
            let resp_ring = unsafe { SpscRing::initialize(resp_region.clone(), layout, 0, 0) }?;

            let (req_tx, _req_rx) = req_ring.split();
            let (_resp_tx, resp_rx) = resp_ring.split();

            // Create Notifier for this worker
            let notifier = Notifier::new()?;
            let notify_fd = notifier.as_raw_fd();

            // Get Shm FDs
            // Note: On Linux/Unix, handle() returns Some(RawFd)
            let req_fd = req_region.handle().ok_or("No handle for request region")?;
            let resp_fd = resp_region
                .handle()
                .ok_or("No handle for response region")?;

            // Clear FD_CLOEXEC so child inherits them
            #[cfg(target_os = "linux")]
            unsafe {
                let flags = libc::fcntl(notify_fd, libc::F_GETFD);
                libc::fcntl(notify_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
                
                let flags = libc::fcntl(req_fd, libc::F_GETFD);
                libc::fcntl(req_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
                
                let flags = libc::fcntl(resp_fd, libc::F_GETFD);
                libc::fcntl(resp_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
            }

            let mut cmd = Command::new(&self.python_path);
            cmd.arg("-m")
                .arg("vertebra.server.worker")
                .arg("--app")
                .arg(&self.app_path)
                .arg("--worker-id")
                .arg(i.to_string())
                .arg("--slots")
                .arg(layout.slots.to_string())
                .arg("--slot-size")
                .arg(layout.slot_size.to_string())
                .env("VERTEBRA_NOTIFY_FD", notify_fd.to_string())
                .env("VERTEBRA_REQ_SHM_FD", req_fd.to_string())
                .env("VERTEBRA_RESP_SHM_FD", resp_fd.to_string())
                .env("VERTEBRA_SHM_SIZE", layout.total_size.to_string())
                .env("PYTHONUNBUFFERED", "1");

            #[cfg(target_os = "linux")]
            unsafe {
                cmd.pre_exec(|| {
                    // Send SIGTERM to child if parent dies
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                    Ok(())
                });
            }

            let child = cmd.spawn()?;
            println!("Spawned worker {} (PID: {}) notify_fd: {}", i, child.id(), notify_fd);

            self.worker_handles.push(Worker {
                req_tx,
                resp_rx,
                pid: child.id(),
                process: child,
                notifier,
            });
        }

        Ok(())
    }
}
