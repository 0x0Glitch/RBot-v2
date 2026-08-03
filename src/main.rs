//! Binary entry point for read-only bootstrap commands.
#![forbid(unsafe_code)]

use std::process::ExitCode;

use clap::Parser;
use morpho_v2_reallocator::cli::{Cli, Command};

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Status => {
            let build = morpho_v2_reallocator::build_info();
            println!(
                "morpho-v2-reallocator {} ({}) execute=disabled",
                build.version, build.revision
            );
            ExitCode::SUCCESS
        }
    }
}
