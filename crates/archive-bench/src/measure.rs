//! Clocks, counters and host facts every record carries.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use hdrhistogram::Histogram;
use serde_json::{json, Value};

/// CPU time of the calling thread, in nanoseconds. Zero where the platform
/// offers no thread clock; [`thread_cpu_available`] says which.
pub fn thread_cpu_ns() -> u64 {
    #[cfg(unix)]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
        if rc == 0 {
            return ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64;
        }
        0
    }
    #[cfg(not(unix))]
    {
        0
    }
}

pub fn thread_cpu_available() -> bool {
    cfg!(unix)
}

/// Process-wide counters sampled before and after a workload.
#[derive(Debug, Clone, Copy, Default)]
pub struct Counters {
    pub user_ns: u64,
    pub sys_ns: u64,
    pub disk_read: Option<u64>,
    pub disk_written: Option<u64>,
    pub max_rss: Option<u64>,
}

impl Counters {
    pub fn delta(&self, earlier: &Counters) -> Counters {
        let sub = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => Some(a.saturating_sub(b)),
            _ => None,
        };
        Counters {
            user_ns: self.user_ns.saturating_sub(earlier.user_ns),
            sys_ns: self.sys_ns.saturating_sub(earlier.sys_ns),
            disk_read: sub(self.disk_read, earlier.disk_read),
            disk_written: sub(self.disk_written, earlier.disk_written),
            max_rss: self.max_rss,
        }
    }

    pub fn json(&self) -> Value {
        json!({
            "user_ms": self.user_ns as f64 / 1e6,
            "sys_ms": self.sys_ns as f64 / 1e6,
            "cpu_ms": (self.user_ns + self.sys_ns) as f64 / 1e6,
            "disk_read_bytes": self.disk_read,
            "disk_written_bytes": self.disk_written,
            "max_rss_bytes": self.max_rss,
        })
    }
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
pub fn counters() -> Counters {
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as i32,
            libc::RUSAGE_INFO_V4,
            &mut info as *mut libc::rusage_info_v4 as *mut libc::rusage_info_t,
        )
    };
    if rc != 0 {
        return Counters::default();
    }
    let mut tb = libc::mach_timebase_info { numer: 0, denom: 0 };
    unsafe { libc::mach_timebase_info(&mut tb) };
    let ns = |t: u64| t * tb.numer as u64 / tb.denom.max(1) as u64;
    Counters {
        user_ns: ns(info.ri_user_time),
        sys_ns: ns(info.ri_system_time),
        disk_read: Some(info.ri_diskio_bytesread),
        disk_written: Some(info.ri_diskio_byteswritten),
        max_rss: Some(info.ri_lifetime_max_phys_footprint),
    }
}

#[cfg(target_os = "linux")]
pub fn counters() -> Counters {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return Counters::default();
    }
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
    let io = std::fs::read_to_string("/proc/self/io").ok();
    let field = |name: &str| -> Option<u64> {
        io.as_ref()?
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.trim_start_matches(':').trim().parse().ok())
    };
    Counters {
        user_ns: tv(usage.ru_utime),
        sys_ns: tv(usage.ru_stime),
        disk_read: field("read_bytes"),
        disk_written: field("write_bytes"),
        max_rss: Some(usage.ru_maxrss as u64 * 1024),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn counters() -> Counters {
    Counters::default()
}

pub fn disk_counters_available() -> bool {
    counters().disk_read.is_some()
}

/// A global allocator that tracks live and peak heap bytes, so a workload's
/// transient memory is the peak it drove above the level it started at.
pub struct PeakAlloc;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static INSTALLED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            INSTALLED.store(1, Ordering::Relaxed);
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let out = System.realloc(ptr, layout, new_size);
        if !out.is_null() {
            if new_size >= layout.size() {
                let now = CURRENT.fetch_add(new_size - layout.size(), Ordering::Relaxed) + new_size
                    - layout.size();
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        out
    }
}

/// Live heap bytes right now, or `None` when the tracking allocator is not
/// the global one of this binary.
pub fn heap_current() -> Option<usize> {
    (INSTALLED.load(Ordering::Relaxed) == 1).then(|| CURRENT.load(Ordering::Relaxed))
}

/// Forget the peak seen so far and start over from the live level.
pub fn heap_reset_peak() {
    PEAK.store(CURRENT.load(Ordering::Relaxed), Ordering::Relaxed);
}

pub fn heap_peak() -> Option<usize> {
    (INSTALLED.load(Ordering::Relaxed) == 1).then(|| PEAK.load(Ordering::Relaxed))
}

/// Latency histogram in nanoseconds, three significant digits, up to an
/// hour.
pub fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 3_600_000_000_000, 3).expect("valid histogram bounds")
}

pub fn histogram_json(h: &Histogram<u64>) -> Value {
    let us = |v: u64| v as f64 / 1000.0;
    json!({
        "count": h.len(),
        "mean_us": h.mean() / 1000.0,
        "p50_us": us(h.value_at_quantile(0.50)),
        "p90_us": us(h.value_at_quantile(0.90)),
        "p95_us": us(h.value_at_quantile(0.95)),
        "p99_us": us(h.value_at_quantile(0.99)),
        "p999_us": us(h.value_at_quantile(0.999)),
        "max_us": us(h.max()),
    })
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

fn os_release() -> Option<String> {
    if cfg!(target_os = "macos") {
        command_output("sw_vers", &["-productVersion"]).map(|v| format!("macOS {v}"))
    } else {
        command_output("uname", &["-sr"])
    }
}

fn cpu_model() -> Option<String> {
    if cfg!(target_os = "macos") {
        return command_output("sysctl", &["-n", "machdep.cpu.brand_string"]);
    }
    let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    info.lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
}

fn memory_bytes() -> Option<u64> {
    if cfg!(target_os = "macos") {
        return command_output("sysctl", &["-n", "hw.memsize"])?
            .parse()
            .ok();
    }
    let info = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kb: u64 = info
        .lines()
        .find(|l| l.starts_with("MemTotal:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some(kb * 1024)
}

/// The filesystem type under `path`, as the platform names it.
#[cfg(target_os = "macos")]
pub fn filesystem_type(path: &Path) -> Option<String> {
    use std::ffi::{CStr, CString};
    let c = CString::new(path.to_string_lossy().as_bytes()).ok()?;
    let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c.as_ptr(), &mut fs) } != 0 {
        return None;
    }
    let name = unsafe { CStr::from_ptr(fs.f_fstypename.as_ptr()) };
    Some(name.to_string_lossy().into_owned())
}

#[cfg(target_os = "linux")]
pub fn filesystem_type(path: &Path) -> Option<String> {
    use std::ffi::CString;
    let c = CString::new(path.to_string_lossy().as_bytes()).ok()?;
    let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c.as_ptr(), &mut fs) } != 0 {
        return None;
    }
    let name = match fs.f_type as u64 {
        0xEF53 => "ext4".to_string(),
        0x5846_5342 => "xfs".to_string(),
        0x9123_683E => "btrfs".to_string(),
        0x0102_1994 => "tmpfs".to_string(),
        0x2FC1_2FC1 => "zfs".to_string(),
        0x6969 => "nfs".to_string(),
        0xF15F => "ecryptfs".to_string(),
        0x6552_4653 => "overlayfs".to_string(),
        other => format!("{other:#x}"),
    };
    Some(name)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn filesystem_type(_path: &Path) -> Option<String> {
    None
}

fn revision() -> Value {
    let root = env!("CARGO_MANIFEST_DIR");
    let git = |args: &[&str]| {
        let out = Command::new("git")
            .args(args)
            .current_dir(root)
            .output()
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    };
    let head = git(&["rev-parse", "HEAD"]);
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"]).map(|s| !s.is_empty());
    json!({ "commit": head, "dirty": dirty })
}

/// Everything about the host and build that a result must carry to be
/// comparable with another.
pub fn environment(paths: &[&Path]) -> Value {
    let filesystems: Vec<Value> = paths
        .iter()
        .map(|p| {
            json!({
                "path": p.display().to_string(),
                "type": filesystem_type(p),
            })
        })
        .collect();
    json!({
        "harness": env!("CARGO_PKG_VERSION"),
        "revision": revision(),
        "recorded_at": chrono::Utc::now().to_rfc3339(),
        "os": std::env::consts::OS,
        "os_release": os_release(),
        "arch": std::env::consts::ARCH,
        "cpu": cpu_model(),
        "memory_bytes": memory_bytes(),
        "zstd": zstd::zstd_safe::version_string(),
        "filesystems": filesystems,
        "counters": {
            "thread_cpu": thread_cpu_available(),
            "disk_io": disk_counters_available(),
            "heap_peak": heap_peak().is_some(),
        },
    })
}
