//! Embeds an exact clean Git revision when one is available at build time.

use std::{env, path::Path, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=MORPHO_V2_BUILD_REVISION");
    if Path::new(".git").is_dir() {
        println!("cargo:rerun-if-changed=.git/HEAD");
        println!("cargo:rerun-if-changed=.git/index");
        println!("cargo:rerun-if-changed=.git/refs/heads");
    }

    let revision = env::var("MORPHO_V2_BUILD_REVISION")
        .ok()
        .filter(|value| is_git_revision(value))
        .or_else(git_revision)
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=MORPHO_V2_BUILD_REVISION={revision}");
}

fn git_revision() -> Option<String> {
    let status = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()
        .ok()?;
    if !status.status.success() || !status.stdout.is_empty() {
        return None;
    }
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let revision = String::from_utf8(output.stdout).ok()?;
    let revision = revision.trim().to_owned();
    is_git_revision(&revision).then_some(revision)
}

fn is_git_revision(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
