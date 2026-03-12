pub fn print_init_banner() {
    println!(
        r#"
888888ba           dP
88    `8b          88
88     88 .d8888b. 88 .d8888b. .d8888b.
88     88 88'  `88 88 88'  `88 Y8ooooo.
88    .8P 88.  .88 88 88.  .88       88
8888888P  `88888P' dP `88888P' `88888P'"#
    );

    println!("\x1b[90moooooooooooooooooooooooooooooooooooooooo\x1b[0m");

    let git_sha = option_env!("DOLOS_GIT_SHA").unwrap_or("unknown");

    println!(
        "\x1b[1;95mv{} ({})\x1b[0m\n",
        env!("CARGO_PKG_VERSION"),
        git_sha
    );
}
