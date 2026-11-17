//! The `dolos` meter's process series: build info and uptime everywhere and,
//! on Linux, CPU time, memory, file descriptors and threads read from
//! `/proc/self`. Every series is observed when the meter collects.

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

use dolos_core::telemetry::{GIT_SHA, VERSION};
use opentelemetry::{
    metrics::{Meter, ObservableGauge},
    KeyValue,
};
use tracing::warn;

/// The meter of the process series.
pub const METER: &str = "dolos";

/// The handles of the process series, kept as long as the meter provider
/// that collects them.
pub struct ProcessMetrics {
    _build_info: ObservableGauge<u64>,
    _uptime: ObservableGauge<f64>,
    #[cfg(target_os = "linux")]
    _linux: linux::Instruments,
}

impl ProcessMetrics {
    /// Registers the series on `meter`. Uptime counts from this call.
    pub fn register(meter: &Meter) -> Self {
        let build = [
            KeyValue::new(VERSION, env!("CARGO_PKG_VERSION")),
            KeyValue::new(GIT_SHA, crate::banner::git_sha()),
        ];

        let build_info = meter
            .u64_observable_gauge("dolos.build.info")
            .with_description("Always 1, with the version and commit of the running build")
            .with_callback(move |observer| observer.observe(1, &build))
            .build();

        let started = Instant::now();
        let uptime = meter
            .f64_observable_gauge("process.uptime")
            .with_unit("s")
            .with_callback(move |observer| observer.observe(started.elapsed().as_secs_f64(), &[]))
            .build();

        Self {
            _build_info: build_info,
            _uptime: uptime,
            #[cfg(target_os = "linux")]
            _linux: linux::Instruments::register(meter),
        }
    }
}

/// Logs the first failure of one series and none after it.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct WarnOnce {
    series: &'static str,
    warned: AtomicBool,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl WarnOnce {
    const fn new(series: &'static str) -> Self {
        Self {
            series,
            warned: AtomicBool::new(false),
        }
    }

    /// The value, or nothing after logging the error if it is the first.
    fn check<T>(&self, read: std::io::Result<T>) -> Option<T> {
        match read {
            Ok(value) => Some(value),
            Err(err) => {
                if !self.warned.swap(true, Ordering::Relaxed) {
                    warn!(%err, series = self.series, "cannot read a process metric; it is not reported and repeats are not logged");
                }
                None
            }
        }
    }
}

/// The parsers of `/proc/self/stat` and `/proc/self/status`, kept apart from
/// the reads so they are tested on every platform.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod proc {
    use std::io::{Error, ErrorKind, Result};

    /// Clock ticks per second of the CPU times in `/proc/self/stat`: the
    /// kernel's `USER_HZ`, 100 on every architecture Dolos builds for.
    pub const TICKS_PER_SECOND: f64 = 100.0;

    fn invalid(what: &str) -> Error {
        Error::new(ErrorKind::InvalidData, format!("unexpected {what}"))
    }

    /// The user and system CPU time, in clock ticks.
    #[derive(Debug, PartialEq, Eq)]
    pub struct CpuTicks {
        pub user: u64,
        pub system: u64,
    }

    /// Reads `utime` and `stime`, the 14th and 15th fields. The process name,
    /// the 2nd, is in parentheses and may hold spaces and parentheses of its
    /// own, so the fields are counted from the last `)`.
    pub fn parse_stat(text: &str) -> Result<CpuTicks> {
        let close = text.rfind(')').ok_or_else(|| invalid("/proc/self/stat"))?;
        // The state, the 3rd field, comes first after the name.
        let mut fields = text[close + 1..].split_whitespace().skip(11);

        let mut next = || {
            fields
                .next()
                .and_then(|field| field.parse().ok())
                .ok_or_else(|| invalid("/proc/self/stat"))
        };

        Ok(CpuTicks {
            user: next()?,
            system: next()?,
        })
    }

    /// The fields of `/proc/self/status` the series report.
    #[derive(Debug, Default, PartialEq, Eq)]
    pub struct Status {
        pub resident_bytes: u64,
        pub virtual_bytes: u64,
        pub threads: u64,
    }

    /// Reads `VmRSS`, `VmSize` (both in kB) and `Threads`.
    pub fn parse_status(text: &str) -> Result<Status> {
        let (mut resident, mut virt, mut threads) = (None, None, None);

        for line in text.lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let number = || {
                value
                    .trim()
                    .trim_end_matches("kB")
                    .trim_end()
                    .parse::<u64>()
                    .ok()
            };

            match key {
                "VmRSS" => resident = number().map(|kb| kb * 1024),
                "VmSize" => virt = number().map(|kb| kb * 1024),
                "Threads" => threads = number(),
                _ => (),
            }
        }

        Ok(Status {
            resident_bytes: resident.ok_or_else(|| invalid("VmRSS in /proc/self/status"))?,
            virtual_bytes: virt.ok_or_else(|| invalid("VmSize in /proc/self/status"))?,
            threads: threads.ok_or_else(|| invalid("Threads in /proc/self/status"))?,
        })
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use opentelemetry::{
        metrics::{Meter, ObservableCounter, ObservableUpDownCounter},
        KeyValue,
    };

    use super::{proc, WarnOnce};

    fn stat() -> std::io::Result<proc::CpuTicks> {
        proc::parse_stat(&std::fs::read_to_string("/proc/self/stat")?)
    }

    fn status() -> std::io::Result<proc::Status> {
        proc::parse_status(&std::fs::read_to_string("/proc/self/status")?)
    }

    /// The entries of `/proc/self/fd`, which include the descriptor that
    /// lists them.
    fn open_descriptors() -> std::io::Result<u64> {
        let mut count = 0;
        for entry in std::fs::read_dir("/proc/self/fd")? {
            entry?;
            count += 1;
        }
        Ok(count)
    }

    /// The semantic conventions make these series up-down counters, which
    /// the SDK builds only for i64; a read past `i64::MAX` saturates.
    fn signed(value: u64) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }

    /// An up-down counter observing one field of `/proc/self/status`.
    fn status_series(
        meter: &Meter,
        name: &'static str,
        unit: &'static str,
        pick: fn(&proc::Status) -> u64,
    ) -> ObservableUpDownCounter<i64> {
        let warn = WarnOnce::new(name);

        meter
            .i64_observable_up_down_counter(name)
            .with_unit(unit)
            .with_callback(move |observer| {
                if let Some(status) = warn.check(status()) {
                    observer.observe(signed(pick(&status)), &[]);
                }
            })
            .build()
    }

    pub struct Instruments {
        _cpu_time: ObservableCounter<f64>,
        _resident: ObservableUpDownCounter<i64>,
        _virtual: ObservableUpDownCounter<i64>,
        _descriptors: ObservableUpDownCounter<i64>,
        _threads: ObservableUpDownCounter<i64>,
    }

    impl Instruments {
        pub fn register(meter: &Meter) -> Self {
            let warn = WarnOnce::new("process.cpu.time");
            let user = [KeyValue::new("cpu.mode", "user")];
            let system = [KeyValue::new("cpu.mode", "system")];
            let cpu_time = meter
                .f64_observable_counter("process.cpu.time")
                .with_unit("s")
                .with_callback(move |observer| {
                    if let Some(ticks) = warn.check(stat()) {
                        observer.observe(ticks.user as f64 / proc::TICKS_PER_SECOND, &user);
                        observer.observe(ticks.system as f64 / proc::TICKS_PER_SECOND, &system);
                    }
                })
                .build();

            let warn = WarnOnce::new("process.unix.file_descriptor.count");
            let descriptors = meter
                .i64_observable_up_down_counter("process.unix.file_descriptor.count")
                .with_unit("{file_descriptor}")
                .with_callback(move |observer| {
                    if let Some(count) = warn.check(open_descriptors()) {
                        observer.observe(signed(count), &[]);
                    }
                })
                .build();

            Self {
                _cpu_time: cpu_time,
                _resident: status_series(meter, "process.memory.usage", "By", |status| {
                    status.resident_bytes
                }),
                _virtual: status_series(meter, "process.memory.virtual", "By", |status| {
                    status.virtual_bytes
                }),
                _descriptors: descriptors,
                _threads: status_series(meter, "process.thread.count", "{thread}", |status| {
                    status.threads
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, SdkMeterProvider,
    };

    use super::*;

    /// A provider whose meter carries the process series, and its exporter.
    fn setup() -> (SdkMeterProvider, InMemoryMetricExporter, ProcessMetrics) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let metrics = ProcessMetrics::register(&provider.meter(METER));

        (provider, exporter, metrics)
    }

    /// One collection: each series' points, as their attributes and value.
    type Points = Vec<(Vec<(String, String)>, f64)>;

    /// One collection of every series, by name: its unit, points and kind.
    type Series = std::collections::BTreeMap<String, (String, Points, &'static str)>;

    /// One collection of every series.
    fn collect(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Series {
        exporter.reset();
        provider.force_flush().unwrap();

        let mut series = std::collections::BTreeMap::new();
        for resource in exporter.get_finished_metrics().unwrap() {
            for scope in resource.scope_metrics() {
                assert_eq!(scope.scope().name(), METER);

                for metric in scope.metrics() {
                    let mut points: Points = Vec::new();
                    let mut push = |attributes: &mut dyn Iterator<Item = &KeyValue>, value| {
                        let mut attributes: Vec<_> = attributes
                            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                            .collect();
                        attributes.sort();
                        points.push((attributes, value));
                    };

                    let kind = match metric.data() {
                        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                            for point in gauge.data_points() {
                                push(&mut point.attributes(), point.value() as f64);
                            }
                            "gauge"
                        }
                        AggregatedMetrics::F64(MetricData::Gauge(gauge)) => {
                            for point in gauge.data_points() {
                                push(&mut point.attributes(), point.value());
                            }
                            "gauge"
                        }
                        AggregatedMetrics::F64(MetricData::Sum(sum)) if sum.is_monotonic() => {
                            for point in sum.data_points() {
                                push(&mut point.attributes(), point.value());
                            }
                            "counter"
                        }
                        AggregatedMetrics::I64(MetricData::Sum(sum)) if !sum.is_monotonic() => {
                            for point in sum.data_points() {
                                push(&mut point.attributes(), point.value() as f64);
                            }
                            "updowncounter"
                        }
                        _ => panic!("unexpected instrument {}", metric.name()),
                    };

                    points.sort_by(|a, b| a.0.cmp(&b.0));
                    series.insert(
                        metric.name().to_string(),
                        (metric.unit().to_string(), points, kind),
                    );
                }
            }
        }

        series
    }

    /// The value of a series with one point.
    fn value(series: &Series, name: &str) -> f64 {
        let (_, points, _) = series.get(name).unwrap_or_else(|| panic!("{name} missing"));
        assert_eq!(points.len(), 1, "{name}: {points:?}");
        points[0].1
    }

    #[test]
    fn build_info_is_one_with_the_crate_version() {
        let (provider, exporter, _metrics) = setup();

        let series = collect(&provider, &exporter);
        let (_, points, kind) = &series["dolos.build.info"];

        assert_eq!(*kind, "gauge");
        assert_eq!(points.len(), 1);
        let (attributes, value) = &points[0];
        assert_eq!(*value, 1.0);
        assert_eq!(
            attributes,
            &[
                (GIT_SHA.to_string(), crate::banner::git_sha().to_string()),
                (VERSION.to_string(), env!("CARGO_PKG_VERSION").to_string()),
            ]
        );
    }

    #[test]
    fn uptime_is_positive_and_grows() {
        let (provider, exporter, _metrics) = setup();

        let first = collect(&provider, &exporter);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let second = collect(&provider, &exporter);

        assert_eq!(first["process.uptime"].0, "s");
        assert_eq!(first["process.uptime"].2, "gauge");
        assert!(value(&first, "process.uptime") > 0.0);
        assert!(value(&second, "process.uptime") > value(&first, "process.uptime"));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn only_build_info_and_uptime_are_registered_off_linux() {
        let (provider, exporter, _metrics) = setup();

        let names: Vec<String> = collect(&provider, &exporter).into_keys().collect();

        assert_eq!(names, ["dolos.build.info", "process.uptime"]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_series_report_the_process() {
        let (provider, exporter, _metrics) = setup();

        let first = collect(&provider, &exporter);

        // Names, units and kinds as the OpenTelemetry semantic conventions
        // define them.
        for (name, unit, kind) in [
            ("process.cpu.time", "s", "counter"),
            ("process.memory.usage", "By", "updowncounter"),
            ("process.memory.virtual", "By", "updowncounter"),
            (
                "process.unix.file_descriptor.count",
                "{file_descriptor}",
                "updowncounter",
            ),
            ("process.thread.count", "{thread}", "updowncounter"),
        ] {
            assert_eq!(first[name].0, unit, "{name}");
            assert_eq!(first[name].2, kind, "{name}");
        }
        assert!(value(&first, "process.memory.usage") > 0.0);
        assert!(value(&first, "process.memory.virtual") >= value(&first, "process.memory.usage"));
        assert!(value(&first, "process.thread.count") > 0.0);

        // Some CPU time between the collections.
        let mut sum = 0u64;
        let started = Instant::now();
        while started.elapsed() < std::time::Duration::from_millis(50) {
            sum = sum.wrapping_add(std::hint::black_box(sum ^ 0x9e37_79b9));
        }
        std::hint::black_box(sum);

        let second = collect(&provider, &exporter);
        let modes = |series: &Series| {
            series["process.cpu.time"]
                .1
                .iter()
                .map(|(attributes, value)| (attributes[0].1.clone(), *value))
                .collect::<Vec<_>>()
        };
        let (before, after) = (modes(&first), modes(&second));
        assert_eq!(
            before
                .iter()
                .map(|(mode, _)| mode.as_str())
                .collect::<Vec<_>>(),
            ["system", "user"]
        );
        for ((mode, before), (_, after)) in before.iter().zip(&after) {
            assert!(after >= before, "{mode}: {before} then {after}");
        }
    }

    /// Other tests in this binary open and close files at the same time, so
    /// a few rounds are tried before the rise is judged missing.
    #[cfg(target_os = "linux")]
    #[test]
    fn opening_files_raises_the_descriptor_count() {
        let (provider, exporter, _metrics) = setup();
        let dir = tempfile::tempdir().unwrap();

        let rose = (0..5).any(|round| {
            let before = value(
                &collect(&provider, &exporter),
                "process.unix.file_descriptor.count",
            );
            let files: Vec<_> = (0..64)
                .map(|n| std::fs::File::create(dir.path().join(format!("{round}-{n}"))).unwrap())
                .collect();
            let after = value(
                &collect(&provider, &exporter),
                "process.unix.file_descriptor.count",
            );
            drop(files);

            after - before >= 64.0
        });

        assert!(rose);
    }

    /// `/proc/self/stat` of a process named `a) b (c`, with utime 1234 and
    /// stime 567.
    const STAT: &str = "4242 (a) b (c) S 1 4242 4242 0 -1 4194560 1500 0 0 0 1234 567 0 0 20 0 7 0 3600 123456789 2048 18446744073709551615 1 1 0 0 0 0 0 4096 0 0 0 0 17 3 0 0 0 0 0\n";

    #[test]
    fn stat_is_read_after_a_name_with_spaces_and_parentheses() {
        assert_eq!(
            proc::parse_stat(STAT).unwrap(),
            proc::CpuTicks {
                user: 1234,
                system: 567
            }
        );
    }

    #[test]
    fn a_truncated_stat_is_an_error() {
        assert!(proc::parse_stat("4242 (dolos) S 1 2 3").is_err());
        assert!(proc::parse_stat("").is_err());
    }

    const STATUS: &str = "\
Name:\ta) b (c
Umask:\t0022
State:\tS (sleeping)
Tgid:\t4242
VmPeak:\t  210000 kB
VmSize:\t  204800 kB
VmRSS:\t    8192 kB
RssAnon:\t    4096 kB
Threads:\t7
SigQ:\t0/63371
";

    #[test]
    fn status_reads_memory_in_bytes_and_the_threads() {
        assert_eq!(
            proc::parse_status(STATUS).unwrap(),
            proc::Status {
                resident_bytes: 8192 * 1024,
                virtual_bytes: 204800 * 1024,
                threads: 7,
            }
        );
    }

    #[test]
    fn a_status_without_a_field_is_an_error() {
        let without_threads = STATUS.replace("Threads:\t7\n", "");

        assert!(proc::parse_status(&without_threads).is_err());
    }
}
