//! The watchdog stays armed through Tokio runtime destruction: a timeout on an
//! async future cannot interrupt synchronous I/O, thread joins, or native GPU work.
use std::{
    io::{self, Write},
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::Duration,
};

pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const OUTPUT_TIMEOUT: Duration = Duration::from_millis(250);

enum Signal {
    Arm(Duration),
    Finished,
}

pub struct ShutdownWatchdog {
    tx: mpsc::Sender<Signal>,
    thread: Option<thread::JoinHandle<()>>,
    armed: bool,
}

impl ShutdownWatchdog {
    pub fn new() -> io::Result<Self> {
        let (tx, rx) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("Shutdown-Watchdog".into())
            .spawn(move || {
                let Ok(Signal::Arm(timeout)) = rx.recv() else {
                    return;
                };
                if matches!(rx.recv_timeout(timeout), Err(RecvTimeoutError::Timeout)) {
                    // Do not let a stalled output sink prevent fail-stop. The
                    // server attempts persistence before waiting on workers.
                    flush_output(Some(
                        "server shutdown deadline exceeded; forcing process exit\n",
                    ));
                    std::process::exit(1);
                }
            })?;
        Ok(Self {
            tx,
            thread: Some(thread),
            armed: false,
        })
    }

    pub fn arm(&mut self, timeout: Duration) {
        if !self.armed {
            self.tx
                .send(Signal::Arm(timeout))
                .expect("shutdown watchdog alive");
            self.armed = true;
        }
    }
}

impl Drop for ShutdownWatchdog {
    fn drop(&mut self) {
        let _ = self.tx.send(Signal::Finished);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Attempt to flush tracing's stdout writer and console stderr before exit.
/// A pipe or terminal can stall too; flushing is bounded and best effort then.
pub fn flush_output(forced_message: Option<&'static str>) -> bool {
    let (tx, rx) = mpsc::channel();
    if thread::Builder::new()
        .name("Shutdown-OutputFlush".into())
        .spawn(move || {
            let stderr = if let Some(message) = forced_message {
                io::stderr().write_all(message.as_bytes())
            } else {
                Ok(())
            };
            let stdout = io::stdout().flush();
            let stderr_flush = io::stderr().flush();
            let _ = tx.send(stderr.is_ok() && stdout.is_ok() && stderr_flush.is_ok());
        })
        .is_err()
    {
        return false;
    }
    rx.recv_timeout(OUTPUT_TIMEOUT).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        process::{Command, Stdio},
        time::Instant,
    };

    #[test]
    fn graceful_runtime_joins_blocking_work_and_flushes() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let mut watchdog = ShutdownWatchdog::new().unwrap();
        watchdog.arm(Duration::from_secs(2));
        let (tx, rx) = mpsc::channel();
        let worker = runtime.spawn_blocking(move || rx.recv().unwrap());
        tx.send(()).unwrap();
        runtime.block_on(worker).unwrap();
        drop(runtime);
        assert!(flush_output(None));
        drop(watchdog);
    }

    #[test]
    fn forced_runtime_drop_exits_after_mock_persistence_and_log_flush() {
        run_forced_child(false);
    }

    #[test]
    fn stalled_output_does_not_prevent_fail_stop() {
        run_forced_child(true);
    }

    fn run_forced_child(stall_output: bool) {
        let started = Instant::now();
        let path = std::env::temp_dir().join(format!(
            "basis-shutdown-{}-{stall_output}.json",
            std::process::id()
        ));
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "shutdown::tests::watchdog_child", "--nocapture"])
            .env("BASIS_SHUTDOWN_TEST_FILE", &path)
            .env(
                "BASIS_SHUTDOWN_STALL_OUTPUT",
                if stall_output { "1" } else { "0" },
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        while child.try_wait().unwrap().is_none() {
            if started.elapsed() >= Duration::from_secs(3) {
                let _ = child.kill();
                let _ = child.wait();
                panic!("shutdown watchdog failed to terminate child within three seconds");
            }
            thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(started.elapsed() < Duration::from_secs(3));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "saved");
        assert!(String::from_utf8_lossy(&output.stdout).contains("critical log flushed"));
        assert!(String::from_utf8_lossy(&output.stderr).contains("forcing process exit"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn watchdog_child() {
        let Some(path) = std::env::var_os("BASIS_SHUTDOWN_TEST_FILE") else {
            return;
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (_release, blocked) = mpsc::channel::<()>();
        runtime.spawn_blocking(move || {
            started_tx.send(()).unwrap();
            let _ = blocked.recv(); // Mock uninterruptible native work.
        });
        started_rx.recv().unwrap();
        let mut watchdog = ShutdownWatchdog::new().unwrap();
        watchdog.arm(Duration::from_millis(100));
        std::fs::write(path, "saved").unwrap();
        println!("critical log flushed");
        assert!(flush_output(None));
        if std::env::var("BASIS_SHUTDOWN_STALL_OUTPUT").as_deref() == Ok("1") {
            let (locked_tx, locked_rx) = mpsc::channel();
            let (_release_output, blocked_output) = mpsc::channel::<()>();
            thread::spawn(move || {
                let _stdout = io::stdout().lock();
                locked_tx.send(()).unwrap();
                let _ = blocked_output.recv();
            });
            locked_rx.recv().unwrap();
            // Keep the release sender alive across runtime destruction.
            drop(runtime);
            drop(_release_output);
            panic!("stalled-output runtime destruction unexpectedly returned");
        }
        drop(runtime); // Must be interrupted by the independent watchdog.
        panic!("blocking runtime destruction unexpectedly returned");
    }
}
