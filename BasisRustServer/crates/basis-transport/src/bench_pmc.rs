//! Linux x86-64 perf groups, used only by the explicitly requested benchmark.
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::FromRawFd;
use std::os::raw::{c_int, c_long, c_ulong};

unsafe extern "C" {
    fn syscall(number: c_long, ...) -> c_long;
    fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
}

pub struct Counters {
    leader: File,
    _member: File,
    fd: c_int,
    previous_times: (u64, u64),
}

pub struct Counts {
    pub branches: u64,
    pub misses: u64,
    pub enabled_ns: u64,
    pub running_ns: u64,
}

impl Counters {
    pub fn open() -> io::Result<Self> {
        fn event(config: u64, group: c_int) -> io::Result<(File, c_int)> {
            // perf_event_attr v7 (128 bytes), hardware event, group read format
            // [nr, time_enabled, time_running, value0, value1]. Count this thread
            // only (pid=0, cpu=-1), excluding kernel and hypervisor execution.
            let mut attr = [0u64; 16];
            attr[0] = 128u64 << 32;
            attr[1] = config;
            attr[4] = 1 | 2 | 8;
            attr[5] = 1 | (1 << 5) | (1 << 6);
            // SAFETY: attr has the advertised size and valid perf_event_attr
            // fields; syscall 298 is perf_event_open on Linux x86-64.
            let fd = unsafe { syscall(298, attr.as_ptr(), 0i32, -1i32, group, 0u64) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: successful perf_event_open returns a new owned fd.
            Ok((unsafe { File::from_raw_fd(fd as c_int) }, fd as c_int))
        }
        let (leader, fd) = event(4, -1)?; // PERF_COUNT_HW_BRANCH_INSTRUCTIONS
        let (member, _) = event(5, fd)?; // PERF_COUNT_HW_BRANCH_MISSES
        Ok(Self {
            leader,
            _member: member,
            fd,
            previous_times: (0, 0),
        })
    }

    fn control(&self, request: c_ulong) -> io::Result<()> {
        // SAFETY: these perf ioctls take an integer PERF_IOC_FLAG_GROUP (1).
        if unsafe { ioctl(self.fd, request, 1u64) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn start(&self) -> io::Result<()> {
        self.control(0x2403)?; // PERF_EVENT_IOC_RESET
        self.control(0x2400) // PERF_EVENT_IOC_ENABLE
    }

    pub fn stop(&mut self) -> io::Result<Counts> {
        self.control(0x2401)?; // PERF_EVENT_IOC_DISABLE
        let mut bytes = [0u8; 40];
        self.leader.read_exact(&mut bytes)?;
        let values: Vec<u64> = bytes
            .chunks_exact(8)
            .map(|chunk| u64::from_ne_bytes(chunk.try_into().unwrap()))
            .collect();
        if values[0] != 2 || values[2] == 0 || values[3] == 0 {
            return Err(io::Error::other("perf group returned missing/zero counts"));
        }
        let enabled_ns = values[1] - self.previous_times.0;
        let running_ns = values[2] - self.previous_times.1;
        self.previous_times = (values[1], values[2]);
        Ok(Counts {
            branches: values[3],
            misses: values[4],
            enabled_ns,
            running_ns,
        })
    }
}
