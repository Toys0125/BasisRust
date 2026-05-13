use anyhow::{Context, Result};
use clap::Parser;
use std::{collections::BTreeMap, fs, path::PathBuf};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Check Basis C# source drift against BasisRustServer"
)]
struct Args {
    #[arg(
        long,
        default_value = "C:/Users/mgsta/Documents/Unity Projects/Basis/Basis Server"
    )]
    csharp_source: PathBuf,
    #[arg(long, default_value = "C:/Users/mgsta/Documents/BasisRustServer")]
    rust_source: PathBuf,
}

#[derive(Debug)]
struct Finding {
    severity: &'static str,
    message: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut findings = Vec::new();
    check_version(&args, &mut findings)?;
    check_channels(&args, &mut findings)?;
    check_permission_nodes(&args, &mut findings)?;
    check_config_fields(&args, &mut findings)?;

    if findings.is_empty() {
        println!("PASS: no drift detected for checked constants.");
    } else {
        println!("FAIL: drift detected.");
        for finding in &findings {
            println!("[{}] {}", finding.severity, finding.message);
        }
        std::process::exit(1);
    }
    Ok(())
}

fn check_version(args: &Args, findings: &mut Vec<Finding>) -> Result<()> {
    let csharp = fs::read_to_string(
        args.csharp_source
            .join("BasisNetworkCore")
            .join("BasisNetworkVersion.cs"),
    )
    .context("reading BasisNetworkVersion.cs")?;
    let rust = fs::read_to_string(
        args.rust_source
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

fn check_channels(args: &Args, findings: &mut Vec<Finding>) -> Result<()> {
    let csharp = fs::read_to_string(
        args.csharp_source
            .join("BasisNetworkCore")
            .join("BasisNetworkCommons.cs"),
    )
    .context("reading BasisNetworkCommons.cs")?;
    let rust = fs::read_to_string(
        args.rust_source
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

fn check_permission_nodes(args: &Args, findings: &mut Vec<Finding>) -> Result<()> {
    let csharp = fs::read_to_string(
        args.csharp_source
            .join("BasisNetworkServer")
            .join("Security")
            .join("PermissionManager.cs"),
    )
    .context("reading PermissionManager.cs")?;
    let rust = fs::read_to_string(
        args.rust_source
            .join("crates")
            .join("basis-protocol")
            .join("src")
            .join("permissions.rs"),
    )
    .context("reading Rust permissions.rs")?;
    for value in extract_csharp_string_consts(&csharp).values() {
        if value.starts_with("basis.") && !rust.contains(value) {
            findings.push(Finding {
                severity: "ERROR",
                message: format!("missing permission node in Rust: {value}"),
            });
        }
    }
    Ok(())
}

fn check_config_fields(args: &Args, findings: &mut Vec<Finding>) -> Result<()> {
    let csharp = fs::read_to_string(
        args.csharp_source
            .join("BasisNetworkCore")
            .join("BasisServerConfiguration.cs"),
    )
    .context("reading BasisServerConfiguration.cs")?;
    let rust = fs::read_to_string(
        args.rust_source
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
            if let Ok(value) = value.trim().trim_end_matches(';').parse::<u8>() {
                out.insert(name.trim().to_string(), value);
            }
        }
    }
    out
}

fn extract_csharp_string_consts(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("public const string ") {
            continue;
        }
        if let Some((name, value)) = line["public const string ".len()..].split_once('=') {
            let value = value.trim().trim_end_matches(';').trim().trim_matches('"');
            out.insert(name.trim().to_string(), value.to_string());
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
