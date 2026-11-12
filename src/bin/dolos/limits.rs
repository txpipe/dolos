#[cfg(unix)]
pub fn prepare() -> miette::Result<()> {
    unix::prepare()
}

#[cfg(not(unix))]
pub fn prepare() -> miette::Result<()> {
    Ok(())
}

#[cfg(unix)]
mod unix {
    use dolos_fjall::Preset;
    use miette::{miette, Result};
    use nix::sys::resource::{getrlimit, rlim_t, setrlimit, Resource, RLIM_INFINITY};

    /// The highest soft limit that Dolos requests on macOS. macOS
    /// `setrlimit(2)` refuses a soft limit that is more than `OPEN_MAX`.
    #[cfg(target_os = "macos")]
    const CEILING: rlim_t = 10240;

    /// The highest soft limit that Dolos requests on the other Unix systems.
    /// The value is the default `fs.nr_open` of Linux. Some container runtimes
    /// set a hard limit near 2^30. Code that examines each possible descriptor
    /// is then slow. An example is a child process that closes the descriptors
    /// that it inherited.
    #[cfg(not(target_os = "macos"))]
    const CEILING: rlim_t = 1 << 20;

    /// The minimum soft limit. If Dolos cannot set a soft limit of this value
    /// or more, it does not start.
    fn required() -> rlim_t {
        let preset = Preset::Standard;
        let budget = preset
            .state()
            .database
            .max_cached_files
            .saturating_add(preset.archive().database.max_cached_files)
            .saturating_add(2048);
        budget.max(4096) as rlim_t
    }

    /// The soft limit that Dolos tries to set. It is the hard limit, but not
    /// more than `ceiling`.
    fn target(hard: rlim_t, ceiling: rlim_t) -> rlim_t {
        hard.min(ceiling)
    }

    pub(super) fn prepare() -> Result<()> {
        prepare_with(
            required(),
            CEILING,
            || getrlimit(Resource::RLIMIT_NOFILE),
            |soft, hard| setrlimit(Resource::RLIMIT_NOFILE, soft, hard),
        )
    }

    fn guidance(required: rlim_t) -> String {
        format!(
            "To see the limits in a shell, run `ulimit -Sn` and `ulimit -Hn`. If the hard limit \
             is less than {required}, ask your administrator to increase it. Then run \
             `ulimit -Sn {required}` before you start Dolos. For systemd, set \
             `LimitNOFILE={required}` in the service override. Then run \
             `systemctl daemon-reload` and restart the service. For a different supervisor, \
             increase the soft and hard nofile limits that it gives to Dolos to {required} or \
             more. Then restart Dolos."
        )
    }

    fn display(limit: rlim_t) -> String {
        if limit == RLIM_INFINITY {
            "unlimited".to_string()
        } else {
            limit.to_string()
        }
    }

    fn failure(required: rlim_t, inherited: (rlim_t, rlim_t), reason: &str) -> miette::Report {
        miette!(
            help = guidance(required),
            "RLIMIT_NOFILE: inherited soft={} hard={}, required soft >= {required}. {reason}",
            display(inherited.0),
            display(inherited.1),
        )
    }

    fn prepare_with(
        required: rlim_t,
        ceiling: rlim_t,
        mut get: impl FnMut() -> nix::Result<(rlim_t, rlim_t)>,
        mut set: impl FnMut(rlim_t, rlim_t) -> nix::Result<()>,
    ) -> Result<()> {
        let inherited = get().map_err(|error| {
            miette!(
                help = guidance(required),
                "RLIMIT_NOFILE: required soft >= {required}. getrlimit failed: {error}"
            )
        })?;
        let (soft, hard) = inherited;
        let target = target(hard, ceiling).max(required);
        if soft == RLIM_INFINITY || soft >= target {
            return Ok(());
        }
        if hard != RLIM_INFINITY && hard < required {
            return Err(failure(required, inherited, "The hard limit is too low."));
        }

        let expected = match set(target, hard) {
            Ok(()) => target,
            Err(_) if soft >= required => return Ok(()),
            // macOS refuses a soft limit that is more than
            // `kern.maxfilesperproc`. An administrator can set that value to
            // less than `ceiling`, but macOS can still accept the minimum.
            Err(_) if target > required => {
                set(required, hard).map(|()| required).map_err(|error| {
                    failure(required, inherited, &format!("setrlimit failed: {error}"))
                })?
            }
            Err(error) => {
                return Err(failure(
                    required,
                    inherited,
                    &format!("setrlimit failed: {error}"),
                ))
            }
        };
        let (soft, hard) = get().map_err(|error| {
            failure(
                required,
                inherited,
                &format!("getrlimit after setrlimit failed: {error}"),
            )
        })?;
        if (soft != RLIM_INFINITY && soft < expected) || hard != inherited.1 {
            return Err(failure(
                required,
                inherited,
                &format!(
                    "setrlimit did not set the requested limits: effective soft={} hard={}. \
                     The soft limit must be {expected} or more, and the hard limit must not change.",
                    display(soft),
                    display(hard),
                ),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use nix::errno::Errno;
        use std::cell::{Cell, RefCell};

        const CAP: rlim_t = 1 << 20;

        /// A simulated process limit. It refuses a soft limit that is more
        /// than `accepts`, as macOS does for `kern.maxfilesperproc`. It records
        /// each soft limit that `prepare_with` requests.
        struct Fake {
            limit: Cell<(rlim_t, rlim_t)>,
            accepts: rlim_t,
            sets: RefCell<Vec<rlim_t>>,
        }

        impl Fake {
            fn new(soft: rlim_t, hard: rlim_t) -> Self {
                Self::refusing_above(soft, hard, RLIM_INFINITY)
            }

            fn refusing_above(soft: rlim_t, hard: rlim_t, accepts: rlim_t) -> Self {
                Self {
                    limit: Cell::new((soft, hard)),
                    accepts,
                    sets: RefCell::new(Vec::new()),
                }
            }

            fn run(&self, required: rlim_t) -> Result<()> {
                prepare_with(
                    required,
                    CAP,
                    || Ok(self.limit.get()),
                    |soft, hard| {
                        self.sets.borrow_mut().push(soft);
                        if soft > self.accepts {
                            return Err(Errno::EINVAL);
                        }
                        self.limit.set((soft, hard));
                        Ok(())
                    },
                )
            }

            fn sets(&self) -> Vec<rlim_t> {
                self.sets.borrow().clone()
            }
        }

        #[test]
        fn limits_minimum_covers_both_presets_and_reserved_descriptors() {
            let preset = Preset::Standard;
            assert!(required() >= 4096);
            assert!(
                required()
                    >= (preset.state().database.max_cached_files
                        + preset.archive().database.max_cached_files
                        + 2048) as rlim_t
            );
        }

        #[test]
        fn limits_target_is_the_hard_limit_up_to_the_ceiling() {
            assert_eq!(target(8192, CAP), 8192);
            assert_eq!(target(CAP, CAP), CAP);
            assert_eq!(target(CAP * 1024, CAP), CAP);
            assert_eq!(target(RLIM_INFINITY, CAP), CAP);
            const { assert!(CEILING >= 4096) };
        }

        #[test]
        fn limits_raise_only_soft_to_the_target_and_read_back_the_result() {
            for (soft, hard, raised) in [
                (1024, 4096, 4096),
                (1024, 524288, 524288),
                (4096, 524288, 524288),
                (1024, CAP * 1024, CAP),
                (1024, RLIM_INFINITY, CAP),
            ] {
                let fake = Fake::new(soft, hard);
                fake.run(4096).unwrap();
                assert_eq!(fake.limit.get(), (raised, hard), "{soft} {hard}");
                assert_eq!(fake.sets(), [raised], "{soft} {hard}");
            }
        }

        #[test]
        fn limits_leave_a_soft_limit_at_or_above_the_target_alone() {
            for inherited in [
                (4096, 4096),
                (524288, 524288),
                (CAP, RLIM_INFINITY),
                (CAP * 2, RLIM_INFINITY),
                (RLIM_INFINITY, RLIM_INFINITY),
            ] {
                let fake = Fake::new(inherited.0, inherited.1);
                fake.run(4096).unwrap();
                assert_eq!(fake.limit.get(), inherited);
                assert!(fake.sets().is_empty(), "{inherited:?}");
            }
        }

        #[test]
        fn limits_fall_back_to_the_minimum_when_the_target_is_refused() {
            let fake = Fake::refusing_above(1024, RLIM_INFINITY, 8192);
            fake.run(4096).unwrap();
            assert_eq!(fake.limit.get(), (4096, RLIM_INFINITY));
            assert_eq!(fake.sets(), [CAP, 4096]);
        }

        #[test]
        fn limits_keep_a_sufficient_soft_limit_when_the_target_is_refused() {
            let fake = Fake::refusing_above(5000, RLIM_INFINITY, 5000);
            fake.run(4096).unwrap();
            assert_eq!(fake.limit.get(), (5000, RLIM_INFINITY));
            assert_eq!(fake.sets(), [CAP]);
        }

        #[test]
        fn limits_honor_a_minimum_above_the_floor() {
            let fake = Fake::new(4096, 16384);
            fake.run(8192).unwrap();
            assert_eq!(fake.limit.get(), (16384, 16384));

            let fake = Fake::new(4096, 5000);
            let error = fake.run(8192).unwrap_err();
            assert!(error.to_string().contains("required soft >= 8192"));
            assert!(fake.sets().is_empty());
        }

        #[test]
        fn limits_low_hard_limit_refuses_without_mutation() {
            let fake = Fake::new(1024, 2048);
            let error = fake.run(4096).unwrap_err();
            assert!(fake.sets().is_empty());
            assert_eq!(fake.limit.get(), (1024, 2048));
            let message = format!("{error:?}");
            for expected in [
                "inherited soft=1024 hard=2048",
                "required soft >= 4096",
                "hard limit is too low",
                "ulimit -Sn",
                "ulimit -Hn",
                "LimitNOFILE=4096",
                "supervisor",
            ] {
                assert!(message.contains(expected), "{message}");
            }
        }

        #[test]
        fn limits_get_error_names_the_read_and_does_not_set() {
            let error = prepare_with(
                4096,
                CAP,
                || Err(Errno::EIO),
                |_, _| panic!("prepare_with must not set a limit when getrlimit fails"),
            )
            .unwrap_err();
            let message = error.to_string();
            assert!(message.contains("getrlimit failed"), "{message}");
            assert!(message.contains("EIO"), "{message}");
        }

        #[test]
        fn limits_set_error_reports_inherited_limits_and_minimum() {
            for (hard, accepts, sets) in [(8192, 2048, vec![8192, 4096]), (4096, 2048, vec![4096])]
            {
                let fake = Fake::refusing_above(1024, hard, accepts);
                let error = fake.run(4096).unwrap_err();
                assert_eq!(fake.sets(), sets);
                assert_eq!(fake.limit.get(), (1024, hard));
                let message = format!("{error:?}");
                for expected in [
                    "setrlimit failed".to_string(),
                    "EINVAL".to_string(),
                    format!("inherited soft=1024 hard={hard}"),
                    "required soft >= 4096".to_string(),
                    "LimitNOFILE=4096".to_string(),
                ] {
                    assert!(message.contains(&expected), "{message}");
                }
            }
        }

        #[test]
        fn limits_verification_read_error_is_reported() {
            let mut reads = 0;
            let error = prepare_with(
                4096,
                CAP,
                || {
                    reads += 1;
                    if reads == 1 {
                        Ok((1024, 8192))
                    } else {
                        Err(Errno::EIO)
                    }
                },
                |_, _| Ok(()),
            )
            .unwrap_err();
            let message = error.to_string();
            assert!(
                message.contains("getrlimit after setrlimit failed"),
                "{message}"
            );
            assert!(
                message.contains("inherited soft=1024 hard=8192"),
                "{message}"
            );
        }

        #[test]
        fn limits_verification_rejects_ineffective_set_and_changed_hard() {
            for effective in [
                (1024, 8192),
                (4096, 8192),
                (8191, 8192),
                (8192, 4096),
                (8192, 16384),
            ] {
                let mut reads = 0;
                let error = prepare_with(
                    4096,
                    CAP,
                    || {
                        reads += 1;
                        Ok(if reads == 1 { (1024, 8192) } else { effective })
                    },
                    |_, _| Ok(()),
                )
                .unwrap_err();
                let message = error.to_string();
                for expected in [
                    "setrlimit did not set the requested limits",
                    "must be 8192 or more",
                    "inherited soft=1024 hard=8192",
                    "required soft >= 4096",
                ] {
                    assert!(message.contains(expected), "{effective:?}: {message}");
                }
            }
        }

        #[test]
        fn limits_real_soft_limit_in_isolated_child() {
            const CHILD: &str = "DOLOS_LIMITS_TEST_SOFT";
            const HARD: &str = "DOLOS_LIMITS_TEST_HARD";
            if let Some(soft) = std::env::var_os(CHILD) {
                let soft: rlim_t = soft.to_str().unwrap().parse().unwrap();
                let (_, inherited_hard) = getrlimit(Resource::RLIMIT_NOFILE).unwrap();
                let hard = match std::env::var(HARD).unwrap().as_str() {
                    "inherited" => inherited_hard,
                    value => value.parse::<rlim_t>().unwrap(),
                };
                if (inherited_hard != RLIM_INFINITY && inherited_hard < hard)
                    || (hard != RLIM_INFINITY && hard < required().max(soft))
                {
                    eprintln!(
                        "The real limits test did not run. The inherited hard limit is too low \
                         for this case: inherited hard={}, requested hard={}, required soft={}, \
                         soft={soft}",
                        display(inherited_hard),
                        display(hard),
                        required(),
                    );
                    return;
                }
                setrlimit(Resource::RLIMIT_NOFILE, soft, hard).unwrap_or_else(|error| {
                    panic!(
                        "cannot set the start limits of the test, soft={soft} hard={}: {error}",
                        display(hard)
                    )
                });
                assert_eq!(getrlimit(Resource::RLIMIT_NOFILE).unwrap(), (soft, hard));
                crate::limits::prepare().unwrap();
                let (effective, effective_hard) = getrlimit(Resource::RLIMIT_NOFILE).unwrap();
                assert_eq!(effective_hard, hard);
                let raised = soft.max(target(hard, CEILING).max(required()));
                if cfg!(target_os = "macos") {
                    assert!(
                        effective == raised || effective == soft.max(required()),
                        "soft={soft} hard={} effective={effective}",
                        display(hard)
                    );
                } else {
                    assert_eq!(effective, raised, "soft={soft} hard={}", display(hard));
                }
                return;
            }

            for (soft, hard) in [
                (1024, "4096"),
                (4096, "4096"),
                (1024, "8192"),
                (1024, "inherited"),
                (4096, "inherited"),
                (8192, "inherited"),
            ] {
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "limits::unix::tests::limits_real_soft_limit_in_isolated_child",
                        "--nocapture",
                    ])
                    .env(CHILD, soft.to_string())
                    .env(HARD, hard)
                    .output()
                    .unwrap();
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                assert!(output.status.success(), "{stdout}{stderr}");
                assert!(stdout.contains("1 passed"), "{stdout}{stderr}");
                if stderr.contains("The real limits test did not run") {
                    eprint!("{stderr}");
                }
            }
        }
    }
}
