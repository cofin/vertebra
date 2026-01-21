use pyo3::prelude::*;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

#[pyclass]
pub struct NativeSupervisor {
    pids: Arc<Mutex<HashSet<i32>>>,
    running: Arc<AtomicBool>,
}

#[pymethods]
impl NativeSupervisor {
    #[new]
    fn new() -> Self {
        Self {
            pids: Arc::new(Mutex::new(HashSet::new())),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    fn add_child(&self, pid: i32) {
        self.pids.lock().unwrap().insert(pid);
    }

    fn remove_child(&self, pid: i32) {
        self.pids.lock().unwrap().remove(&pid);
    }

    fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    fn run(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            self.run_impl()
        })
    }
}

impl NativeSupervisor {
    #[cfg(unix)]
    fn run_impl(&self) -> PyResult<()> {
        use std::mem::MaybeUninit;

        // Block signals so we can use signalfd
        let mut mask = MaybeUninit::<libc::sigset_t>::uninit();
        let sfd = unsafe {
            libc::sigemptyset(mask.as_mut_ptr());
            let mut mask = mask.assume_init();
            libc::sigaddset(&mut mask, libc::SIGINT);
            libc::sigaddset(&mut mask, libc::SIGTERM);
            libc::sigaddset(&mut mask, libc::SIGCHLD);
            libc::pthread_sigmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut());
            
            libc::signalfd(-1, &mask, libc::SFD_CLOEXEC)
        };
        
        if sfd < 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyOSError, _>("failed to create signalfd"));
        }

        while self.running.load(Ordering::Relaxed) {
            let mut fds = [libc::pollfd {
                fd: sfd,
                events: libc::POLLIN,
                revents: 0,
            }];
            
            let ret = unsafe { libc::poll(fds.as_mut_ptr(), 1, 1000) }; // 1s timeout
            
            if ret > 0 {
                let mut si = MaybeUninit::<libc::signalfd_siginfo>::uninit();
                let res = unsafe { libc::read(sfd, si.as_mut_ptr() as *mut _, std::mem::size_of::<libc::signalfd_siginfo>()) };
                if res as usize == std::mem::size_of::<libc::signalfd_siginfo>() {
                    let si = unsafe { si.assume_init() };
                    let signo = si.ssi_signo as i32;
                    
                    match signo {
                        libc::SIGINT | libc::SIGTERM => {
                            self.propagate_signal(signo);
                            self.running.store(false, Ordering::Relaxed);
                        }
                        libc::SIGCHLD => {
                            self.reap_children();
                        }
                        _ => {}
                    }
                }
            }
            
            if self.pids.lock().unwrap().is_empty() && !self.running.load(Ordering::Relaxed) {
                break;
            }
        }
        
        unsafe { libc::close(sfd) };
        Ok(())
    }

    #[cfg(not(unix))]
    fn run_impl(&self) -> PyResult<()> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>("Not supported on non-unix"))
    }

    #[cfg(unix)]
    fn propagate_signal(&self, signo: i32) {
        let pids = self.pids.lock().unwrap();
        for &pid in pids.iter() {
            unsafe { libc::kill(pid, signo) };
        }
    }

    #[cfg(unix)]
    fn reap_children(&self) {
        let mut status: i32 = 0;
        loop {
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid > 0 {
                self.remove_child(pid);
            } else {
                break;
            }
        }
    }
}