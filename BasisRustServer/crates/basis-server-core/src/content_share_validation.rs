//! Validation of inline BasisVR content-share payloads before relaying them.
use basis_protocol::messages::ContentShareType;
use unicode_general_category::{get_general_category, GeneralCategory};

fn plain_text(value: &str) -> bool {
    !value
        .chars()
        .any(|c| c.is_control() || get_general_category(c) == GeneralCategory::Format)
}

fn valid_http_address(url: &str, strict_link: bool) -> bool {
    if url.is_empty() || url.len() > if strict_link { 4096 } else { 16384 } {
        return false;
    }
    if url.chars().any(|c| {
        c.is_whitespace()
            || c.is_control()
            || c == '\\'
            || c == '"'
            || c == '<'
            || c == '>'
            || get_general_category(c) == GeneralCategory::Format
    }) {
        return false;
    }
    let Some(without_scheme) = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
    else {
        return false;
    };
    let authority = without_scheme.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        return false;
    }
    let (host, port) = if let Some(ipv6) = authority.strip_prefix('[') {
        let Some((host, rest)) = ipv6.split_once(']') else {
            return false;
        };
        if host.contains('%') || host.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        (host, rest.strip_prefix(':').unwrap_or(rest))
    } else {
        let (host, port) = authority.split_once(':').unwrap_or((authority, ""));
        (host, port)
    };
    if !port.is_empty() && (port.parse::<u16>().is_err() || port == "0") {
        return false;
    }
    if host.is_empty() || host.len() > 254 {
        return false;
    }
    if host.parse::<std::net::Ipv4Addr>().is_ok() || host.parse::<std::net::Ipv6Addr>().is_ok() {
        return true;
    }
    if host.starts_with('.')
        || host.contains("..")
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return false;
    }
    let last = host.trim_end_matches('.').rsplit('.').next().unwrap_or("");
    !(last.is_empty()
        || last.bytes().all(|b| b.is_ascii_digit())
        || (last.starts_with("0x") && last[2..].bytes().all(|b| b.is_ascii_hexdigit())))
}

fn unescape_field(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut index = 0;
    let mut out = Vec::with_capacity(bytes.len());
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let decoded = match (bytes[index + 1], bytes[index + 2].to_ascii_uppercase()) {
                (b'2', b'5') => Some(b'%'),
                (b'0', b'9') => Some(b'\t'),
                (b'0', b'A') => Some(b'\n'),
                (b'0', b'D') => Some(b'\r'),
                _ => None,
            };
            if let Some(decoded) = decoded {
                out.push(decoded);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(out).unwrap_or_default()
}

fn valid_folder(payload: &str) -> Option<Vec<ContentShareType>> {
    let mut lines = payload.lines();
    if lines.next()?.trim_end_matches('\r') != "basis-folder:1" {
        return None;
    }
    let folder = lines
        .next()?
        .trim_end_matches('\r')
        .split('\t')
        .collect::<Vec<_>>();
    let id = unescape_field(folder.first()?);
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    let mut items = Vec::new();
    for line in lines {
        if line.trim_end_matches('\r').is_empty() {
            continue;
        }
        if items.len() >= 128 {
            return None;
        }
        let fields = line.trim_end_matches('\r').split('\t').collect::<Vec<_>>();
        if fields.len() < 3 {
            return None;
        }
        let kind = match *fields.first()? {
            "0" => ContentShareType::Avatar,
            "1" => ContentShareType::Prop,
            "2" => ContentShareType::World,
            _ => return None,
        };
        let url = unescape_field(fields[1]);
        let password = unescape_field(fields[2]);
        if password.is_empty() || !valid_http_address(&url, false) {
            return None;
        }
        items.push(kind);
    }
    (!items.is_empty()).then_some(items)
}

fn valid_setting(payload: &str) -> bool {
    let mut lines = payload.lines();
    if lines
        .next()
        .is_none_or(|s| s.trim_end_matches('\r') != "basis-setting:1")
    {
        return false;
    }
    let Some(where_line) = lines.next() else {
        return false;
    };
    let where_fields = where_line
        .trim_end_matches('\r')
        .split('\t')
        .collect::<Vec<_>>();
    if where_fields.len() < 3 {
        return false;
    }
    let menu = unescape_field(where_fields[0]);
    if menu.is_empty()
        || menu.len() > 32
        || !menu.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'_' || b == b'-'
        })
    {
        return false;
    }
    for field in [&where_fields[1], &where_fields[2]] {
        let value = unescape_field(field);
        if value.trim().is_empty() || value.len() > 128 || !plain_text(&value) {
            return false;
        }
    }
    if lines.next().is_none() {
        return false;
    }
    if let Some(line) = lines.next() {
        if !line.is_empty() {
            let fields = line.trim_end_matches('\r').split('\t').collect::<Vec<_>>();
            if fields.len() < 2 {
                return false;
            }
            let kind = unescape_field(fields[0]);
            let value = unescape_field(fields[1]);
            if value.len() > 256 {
                return false;
            }
            match kind.as_str() {
                "bool" => {
                    if value != "true" && value != "false" {
                        return false;
                    }
                }
                "text" => {
                    if !plain_text(&value) {
                        return false;
                    }
                }
                "number" => {
                    if value.is_empty()
                        || value.len() > 64
                        || !value.bytes().all(|b| {
                            b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.' | b'e' | b'E')
                        })
                        || value.parse::<f64>().map_or(true, |v| !v.is_finite())
                    {
                        return false;
                    }
                }
                _ => return false,
            }
        }
    }
    true
}

/// Return the nested share types that must also pass the server's content locks.
pub fn validate(kind: ContentShareType, url: &str) -> Option<Vec<ContentShareType>> {
    use ContentShareType::*;
    match kind {
        Avatar | Prop | World | Server => Some(Vec::new()),
        DollyTrack if url.len() <= 16384 => Some(Vec::new()),
        Link if valid_http_address(url, true) => Some(Vec::new()),
        Text if url.len() <= 4096 && !url.trim().is_empty() => Some(Vec::new()),
        Folder if url.len() <= 16384 => valid_folder(url),
        Setting if url.len() <= 4096 && valid_setting(url) => Some(Vec::new()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn links_reject_spoofing_and_invisible_hosts() {
        assert!(validate(ContentShareType::Link, "https://basisvr.com/").is_some());
        for url in [
            "https://trusted.com@evil.com",
            "https://0x7f.1",
            "https://evil.com\\@trusted.com",
            "https://evil.com\u{200b}x",
            "javascript:alert(1)",
            "https://2130706433",
            "http://abc.123",
        ] {
            assert!(validate(ContentShareType::Link, url).is_none(), "{url}");
        }
    }
    #[test]
    fn folder_nested_types_and_setting_validation() {
        let folder = "basis-folder:1\nfolder_1\tmy folder\n1\thttps://example.com/file\tpass\tprop";
        assert_eq!(
            validate(ContentShareType::Folder, folder),
            Some(vec![ContentShareType::Prop])
        );
        assert!(validate(
            ContentShareType::Folder,
            "basis-folder:1\nx\n5\thttps://x.com\tpass"
        )
        .is_none());
        assert!(validate(
            ContentShareType::Setting,
            "basis-setting:1\nsettings\tvideo\tfps\nFPS\tSettings\nbool\ttrue"
        )
        .is_some());
        assert!(validate(
            ContentShareType::Setting,
            "basis-setting:1\nsettings\tvideo\tfps\nFPS\tSettings\nbool\tyes"
        )
        .is_none());
    }
}
