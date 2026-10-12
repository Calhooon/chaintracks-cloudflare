use std::{fs, path::Path, process::Command};

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=build.rs");
    // Refresh dirty state and timestamp on every invocation, including untracked files.
    println!(
        "cargo:rerun-if-changed={}/metadata-refresh",
        std::env::var("OUT_DIR").unwrap_or_default()
    );
    for name in ["HEAD", "index", "packed-refs"] {
        if let Some(path) = git(&["rev-parse", "--git-path", name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(reference) = git(&["symbolic-ref", "-q", "HEAD"]) {
        if let Some(path) = git(&["rev-parse", "--git-path", &reference]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    // An exported source tree must not accidentally claim an enclosing repo's revision.
    let in_git = Path::new(".git").exists();
    let rev = in_git
        .then(|| git(&["rev-parse", "HEAD"]))
        .flatten()
        .filter(|s| s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .unwrap_or_else(|| "unknown".into());
    let dirty = in_git
        .then(|| git(&["status", "--porcelain"]))
        .flatten()
        .is_some_and(|s| !s.is_empty());
    let lock = fs::read_to_string("Cargo.lock").unwrap_or_default();
    let version = lock.split("[[package]]").find_map(|package| {
        let mut name = None;
        let mut version = None;
        for line in package.lines().map(str::trim) {
            if let Some(value) = line.strip_prefix("name = ") {
                name = Some(value.trim_matches('"'));
            }
            if let Some(value) = line.strip_prefix("version = ") {
                version = Some(value.trim_matches('"'));
            }
        }
        (name == Some("bsv-rs")).then_some(version).flatten()
    });
    println!("cargo:rustc-env=CHAINTRACKS_LIB_REV={rev}");
    println!("cargo:rustc-env=CHAINTRACKS_DIRTY={dirty}");
    println!(
        "cargo:rustc-env=CHAINTRACKS_BUILT_AT={}",
        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    );
    // Only a dependency the lock carries is named; this service has none today.
    if let Some(version) = version {
        println!("cargo:rustc-env=CHAINTRACKS_BSV_RS={version}");
    }
}
