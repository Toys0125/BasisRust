use anyhow::{Context, Result};
use clap::Parser;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Check Basis C# source drift against BasisRustServer using git as source of truth"
)]
struct Args {
    /// Git repo URL for the C# source (default: https://github.com/BasisVR/Basis/)
    #[arg(long, default_value = "https://github.com/BasisVR/Basis/")]
    repo: String,

    /// Branch to check (auto-detected from repo if not specified)
    #[arg(long)]
    branch: Option<String>,

    /// Alternative Rust repo URL
    #[arg(long)]
    rust_repo: Option<String>,

    /// Path to local Rust source (default: detected from git)
    #[arg(long)]
    rust_source: Option<PathBuf>,
}

#[derive(Debug)]
struct Finding {
    severity: &'static str,
    message: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Detect branch
    let branch = match args.branch {
        Some(b) => b,
        None => detect_branch(&args.repo)?,
    };

    println!("Source: {} (branch: {})", args.repo, branch);

    // Clone the branch to temp directory
    let temp_dir = clone_branch(&args.repo, &branch)?;

    // Determine Rust source path
    let rust_source = match args.rust_source {
        Some(path) => path,
        None => detect_rust_source()?,
    };

    // Run checks on the cloned branch
    let mut findings = Vec::new();
    check_version(&temp_dir, &rust_source, &mut findings)?;
    check_channels(&temp_dir, &rust_source, &mut findings)?;
    check_permission_nodes(&temp_dir, &rust_source, &mut findings)?;
    check_config_fields(&temp_dir, &rust_source, &mut findings)?;

    if findings.is_empty() {
        println!("PASS: no drift detected for checked constants.");
    } else {
        println!("FAIL: drift detected.");
        for finding in &findings {
            println!("[{}] {}", finding.severity, finding.message);
        }
    }

    Ok(())
}

/// Detect the default branch from the repo by checking for common branch names
fn detect_branch(repo_url: &str) -> Result<String> {
    // Check for common default branch names in order of preference
    let output = std::process::Command::new("git")
        .arg("ls-remote")
        .arg("--heads")
        .arg(repo_url)
        .arg("refs/heads/main")
        .output()
        .context("running git ls-remote")?;

    if !output.stdout.is_empty() {
        return Ok("main".to_string());
    }

    // Check for master branch
    let output = std::process::Command::new("git")
        .arg("ls-remote")
        .arg("--heads")
        .arg(repo_url)
        .arg("refs/heads/master")
        .output()
        .context("running git ls-remote")?;

    if !output.stdout.is_empty() {
        return Ok("master".to_string());
    }

    // Check for developer branch (used by Basis repo)
    let output = std::process::Command::new("git")
        .arg("ls-remote")
        .arg("--heads")
        .arg(repo_url)
        .arg("refs/heads/developer")
        .output()
        .context("running git ls-remote")?;

    if !output.stdout.is_empty() {
        return Ok("developer".to_string());
    }

    // Return first branch found
    let output = std::process::Command::new("git")
        .arg("ls-remote")
        .arg("--heads")
        .arg(repo_url)
        .output()
        .context("running git ls-remote")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if let Some(ref_path) = line.split('\t').next() {
            let ref_path = ref_path.trim();
            if ref_path.contains("refs/heads/") && !ref_path.contains("/") {
                return Ok(ref_path
                    .strip_prefix("refs/heads/")
                    .unwrap_or(ref_path)
                    .to_string());
            }
        }
    }

    Ok("main".to_string())
}

/// Clone a specific branch from the repo to a temp directory
fn clone_branch(repo_url: &str, branch: &str) -> Result<PathBuf> {
    let temp_dir = std::env::temp_dir().join("basis-sync");
    // Remove existing directory if it exists
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)?;
    }
    std::fs::create_dir_all(&temp_dir)?;

    let output = std::process::Command::new("git")
        .arg("clone")
        .arg("--branch")
        .arg(branch)
        .arg("--depth")
        .arg("1")
        .arg("--single-branch")
        .arg(repo_url)
        .arg(&temp_dir)
        .output()
        .context("running git clone")?;

    if !output.status.success() {
        return Err(anyhow::anyhow!("Failed to clone branch: {}", branch));
    }
    Ok(temp_dir)
}

/// Detect Rust source path from current directory by searching for Cargo.toml
fn detect_rust_source() -> Result<PathBuf> {
    let mut current = std::env::current_dir()?;
    loop {
        if current.join("Cargo.toml").exists() {
            return Ok(current);
        }
        if let Some(parent) = current.parent() {
            current = parent.to_path_buf();
        } else {
            break;
        }
    }
    Ok(std::env::current_dir()?)
}

fn check_version(
    temp_dir: &PathBuf,
    rust_source: &PathBuf,
    findings: &mut Vec<Finding>,
) -> Result<()> {
    let csharp = fs::read_to_string(
        temp_dir
            .join("Basis Server")
            .join("BasisNetworkCore")
            .join("BasisNetworkVersion.cs"),
    )
    .context("reading BasisNetworkVersion.cs from cloned branch")?;
    let rust = fs::read_to_string(
        rust_source
            .join("crates")
            .join("basis-protocol")
            .join("src")
            .join("version.rs"),
    )
    .context("reading Rust version.rs")?;

    let csharp_version = extract_after(&csharp, "ServerVersion =")
        .and_then(first_number)
        .unwrap_or_default();
    let rust_version = extract_after(&rust, "SERVER_VERSION: u16 =")
        .and_then(first_number)
        .unwrap_or_default();

    if csharp_version != rust_version {
        findings.push(Finding {
            severity: "ERROR",
            message: format!("protocol version mismatch: C#={csharp_version}, Rust={rust_version}"),
        });
    }
    Ok(())
}

fn check_channels(
    temp_dir: &PathBuf,
    rust_source: &PathBuf,
    findings: &mut Vec<Finding>,
) -> Result<()> {
    let csharp = fs::read_to_string(
        temp_dir
            .join("Basis Server")
            .join("BasisNetworkCore")
            .join("BasisNetworkCommons.cs"),
    )
    .context("reading BasisNetworkCommons.cs from cloned branch")?;
    let rust = fs::read_to_string(
        rust_source
            .join("crates")
            .join("basis-protocol")
            .join("src")
            .join("channels.rs"),
    )
    .context("reading Rust channels.rs")?;

    let csharp_consts = extract_csharp_byte_consts(&csharp);
    for (name, value) in csharp_consts {
        let rust_name = csharp_name_to_rust(&name);
        let needle = format!("pub const {rust_name}: u8 = {value};");
        if !rust.contains(&needle) {
            findings.push(Finding {
                severity: "ERROR",
                message: format!(
                    "missing or changed channel constant: {name}={value} expected `{needle}`"
                ),
            });
        }
    }
    Ok(())
}

fn check_permission_nodes(
    temp_dir: &PathBuf,
    rust_source: &PathBuf,
    findings: &mut Vec<Finding>,
) -> Result<()> {
    let csharp = fs::read_to_string(
        temp_dir
            .join("Basis Server")
            .join("BasisNetworkServer")
            .join("Security")
            .join("PermissionManager.cs"),
    )
    .context("reading PermissionManager.cs from cloned branch")?;
    let rust = fs::read_to_string(
        rust_source
            .join("crates")
            .join("basis-protocol")
            .join("src")
            .join("permissions.rs"),
    )
    .context("reading Rust permissions.rs")?;

    for value in extract_csharp_string_consts(&csharp) {
        if value.starts_with("basis.") && !rust.contains(&value) {
            findings.push(Finding {
                severity: "ERROR",
                message: format!("missing permission node in Rust: {value}"),
            });
        }
    }
    Ok(())
}

fn check_config_fields(
    temp_dir: &PathBuf,
    rust_source: &PathBuf,
    findings: &mut Vec<Finding>,
) -> Result<()> {
    let csharp = fs::read_to_string(
        temp_dir
            .join("Basis Server")
            .join("BasisNetworkCore")
            .join("BasisServerConfiguration.cs"),
    )
    .context("reading BasisServerConfiguration.cs from cloned branch")?;
    let rust = fs::read_to_string(
        rust_source
            .join("crates")
            .join("basis-protocol")
            .join("src")
            .join("config.rs"),
    )
    .context("reading Rust config.rs")?;

    for field in extract_public_config_fields(&csharp) {
        let snake = csharp_config_field_to_rust(&field);
        if !rust.contains(&format!("pub {snake}:")) {
            findings.push(Finding {
                severity: "WARN",
                message: format!("C# config field `{field}` may be missing in Rust as `{snake}`"),
            });
        }
    }
    Ok(())
}

// Helper functions remain the same as the original implementation

fn csharp_config_field_to_rust(field: &str) -> String {
    match field {
        "BSRSMillisecondDefaultInterval" => "bsrsmillisecond_default_interval".to_string(),
        "BSRBaseMultiplier" => "bsrbase_multiplier".to_string(),
        "BSRSIncreaseRate" => "bsrsincrease_rate".to_string(),
        "BSRSlowestSendRate" => "bsrslowest_send_rate".to_string(),
        "EnableBSRProfiling" => "enable_bsrprofiling".to_string(),
        _ => pascal_to_snake(field),
    }
}

fn extract_after<'a>(text: &'a str, needle: &str) -> Option<&'a str> {
    text.split_once(needle).map(|(_, tail)| tail)
}

fn first_number(text: &str) -> Option<u16> {
    let digits: String = text
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

fn extract_csharp_byte_consts(text: &str) -> BTreeMap<String, u8> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("public const byte ") {
            continue;
        }
        if let Some((name, value)) = line["public const byte ".len()..].split_once('=') {
            if let Ok(value) = value.trim().trim_end_matches(';').trim().parse::<u8>() {
                out.insert(name.trim().to_string(), value);
            }
        }
    }
    out
}

fn extract_csharp_string_consts(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("public const string ") {
            continue;
        }
        if let Some((_name, value)) = line["public const string ".len()..].split_once('=') {
            let value = value.trim().trim_end_matches(';').trim().trim_matches('"');
            out.push(value.to_string());
        }
    }
    out
}

fn extract_public_config_fields(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("public ")
            || line.starts_with("public const ")
            || line.contains('(')
            || line.contains(" class ")
        {
            continue;
        }
        let Some(before_equals) = line.split('=').next() else {
            continue;
        };
        let parts: Vec<_> = before_equals.split_whitespace().collect();
        if parts.len() >= 3 {
            out.push(parts[2].trim_end_matches(';').to_string());
        }
    }
    out
}

fn csharp_name_to_rust(name: &str) -> String {
    if let Some(rest) = name.strip_prefix("EventType_") {
        return format!("EVENT_TYPE_{}", pascal_to_snake(rest).to_ascii_uppercase());
    }
    if let Some(rest) = name.strip_prefix("P2PSub_") {
        return format!("P2P_SUB_{}", pascal_to_snake(rest).to_ascii_uppercase());
    }
    let name = name.strip_suffix("Channel").unwrap_or(name);
    match name {
        "metaData" => "META_DATA".to_string(),
        "netIDAssign" => "NET_ID_ASSIGN".to_string(),
        "NetIDAssigns" => "NET_ID_ASSIGNS".to_string(),
        _ => pascal_to_snake(name).to_ascii_uppercase(),
    }
}

fn pascal_to_snake(name: &str) -> String {
    let normalized = name
        .replace("IPv", "Ipv")
        .replace("PIP", "Pip")
        .replace("ID", "Id");
    let mut out = String::new();
    let chars: Vec<_> = normalized.chars().collect();
    for (idx, ch) in chars.iter().enumerate() {
        if ch.is_ascii_uppercase()
            && idx > 0
            && (chars[idx - 1].is_ascii_lowercase()
                || chars
                    .get(idx + 1)
                    .is_some_and(|next| next.is_ascii_lowercase()))
        {
            out.push('_');
        }
        out.push(ch.to_ascii_lowercase());
    }
    out
}
