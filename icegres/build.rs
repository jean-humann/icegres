//! Build script: stamp the compiled binary with the git commit it was built
//! from (and a build date), so a deployed `icegres --version` can be traced
//! back to an exact source revision — a basic requirement for operating a GA
//! service. Degrades gracefully: if git or the `.git` dir is unavailable (for
//! example a source tarball or a Docker build with no VCS context), the SHA
//! becomes "unknown" rather than failing the build.

use std::process::Command;

fn main() {
    let sha = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let dirty = match git(&["status", "--porcelain"]) {
        Some(s) if !s.trim().is_empty() => "-dirty",
        _ => "",
    };
    // Prefer the commit date (reproducible: same commit -> same string) over
    // wall-clock, which would make every rebuild differ.
    let date =
        git(&["show", "-s", "--format=%cs", "HEAD"]).unwrap_or_else(|| "unknown".to_string());

    let pkg = env!("CARGO_PKG_VERSION");
    let long = format!("{pkg} ({sha}{dirty} {date})");

    println!("cargo:rustc-env=ICEGRES_GIT_SHA={sha}{dirty}");
    println!("cargo:rustc-env=ICEGRES_BUILD_DATE={date}");
    println!("cargo:rustc-env=ICEGRES_LONG_VERSION={long}");

    // Re-run when HEAD moves so the stamp stays current.
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/index");
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}
