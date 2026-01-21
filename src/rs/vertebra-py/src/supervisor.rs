use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq, Debug)]
enum SupervisorEvent {
    Signal { signo: i32 },
    Exit { pid: i32, code: i32 },
    Timeout(),
}

#[pyclass]
pub struct NativeSupervisor {
    pids: Arc<Mutex<HashSet<i32>>>,
}

#[pymethods]
impl NativeSupervisor {
    #[new]
    fn new() -> Self {
        Self {
            pids: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn add_child(&self, pid: i32) {
        self.pids.lock().unwrap().insert(pid);
    }

    fn remove_child(&self, pid: i32) {
        self.pids.lock().unwrap().remove(&pid);
    }

    fn propagate_signal(&self, signo: i32) {
        #[cfg(unix)]
        {
            let pids = self.pids.lock().unwrap();
            for &pid in pids.iter() {
                unsafe { libc::kill(pid, signo) };
            }
        }
    }

    fn wait_next_event(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Py<PyAny>> {
        let event = py.detach(|| self.wait_impl(timeout_ms))?;
        let dict = PyDict::new(py);
        match event {
            SupervisorEvent::Signal { signo } => {
                dict.set_item("type", "signal")?;
                dict.set_item("signo", signo)?;
            }
            SupervisorEvent::Exit { pid, code } => {
                dict.set_item("type", "exit")?;
                dict.set_item("pid", pid)?;
                dict.set_item("code", code)?;
            }
            SupervisorEvent::Timeout() => {
                dict.set_item("type", "timeout")?;
            }
        }
        Ok(dict.into())
    }
}

impl NativeSupervisor {
    #[cfg(unix)]
    fn wait_impl(&self, timeout_ms: u64) -> PyResult<SupervisorEvent> {
        use std::mem::MaybeUninit;

        // Ensure signals are blocked (should be done once, but idempotent)
        let mut mask = MaybeUninit::<libc::sigset_t>::uninit();
        let mut old_mask = MaybeUninit::<libc::sigset_t>::uninit();

        let sfd = unsafe {
            libc::sigemptyset(mask.as_mut_ptr());
            let mut mask = mask.assume_init();
            libc::sigaddset(&mut mask, libc::SIGINT);
            libc::sigaddset(&mut mask, libc::SIGTERM);
            libc::sigaddset(&mut mask, libc::SIGCHLD);
            libc::pthread_sigmask(libc::SIG_BLOCK, &mask, old_mask.as_mut_ptr());

            libc::signalfd(-1, &mask, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK)
        };

        if sfd < 0 {
            // Try to restore mask?
            unsafe {
                libc::pthread_sigmask(libc::SIG_SETMASK, old_mask.as_ptr(), std::ptr::null_mut())
            };
            return Err(PyErr::new::<pyo3::exceptions::PyOSError, _>(
                "failed to create signalfd",
            ));
        }

        let mut fds = [libc::pollfd {
            fd: sfd,
            events: libc::POLLIN,
            revents: 0,
        }];

        let ret = unsafe { libc::poll(fds.as_mut_ptr(), 1, timeout_ms as i32) };

        // Read signal if available to clear the buffer
        let mut signal_received = None;
        if ret > 0 {
            let mut si = MaybeUninit::<libc::signalfd_siginfo>::uninit();
            let res = unsafe {
                libc::read(
                    sfd,
                    si.as_mut_ptr() as *mut _,
                    std::mem::size_of::<libc::signalfd_siginfo>(),
                )
            };
            if res as usize == std::mem::size_of::<libc::signalfd_siginfo>() {
                let si = unsafe { si.assume_init() };
                signal_received = Some(si.ssi_signo as i32);
            }
        }

        // Always check for zombie processes (SIGCHLD might have been missed or consumed)
        // This is the critical fix for the race condition.
        let mut status: i32 = 0;
        let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };

        let result = if pid > 0 {
            self.remove_child(pid);
            // Extract exit code
            let code = if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status)
            } else if libc::WIFSIGNALED(status) {
                -libc::WTERMSIG(status)
            } else {
                0
            };
            SupervisorEvent::Exit { pid, code }
        } else if let Some(signo) = signal_received {
            match signo {
                libc::SIGINT | libc::SIGTERM => SupervisorEvent::Signal { signo },
                _ => SupervisorEvent::Timeout(),
            }
        } else {
            SupervisorEvent::Timeout()
        };

        unsafe {
            libc::close(sfd);
            libc::pthread_sigmask(libc::SIG_SETMASK, old_mask.as_ptr(), std::ptr::null_mut());
        }
        Ok(result)
    }

    #[cfg(not(unix))]
    fn wait_impl(&self, _timeout_ms: u64) -> PyResult<SupervisorEvent> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Not supported on non-unix",
        ))
    }
}
