use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use basis_protocol::NetWriter;
use quick_xml::{events::Event, Reader};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// The fields used by both BasisVR's XML configuration and server library wire format.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename = "BasisDefaultLibraryConfiguration")]
pub struct DefaultLibraryEntry {
    #[serde(rename = "Mode", default)]
    pub mode: u8,
    #[serde(rename = "Url", default)]
    pub url: String,
    #[serde(rename = "Password", default)]
    pub password: String,
}

#[derive(Debug, Clone, Default)]
pub struct DefaultLibrary {
    pub entries: Vec<DefaultLibraryEntry>,
}

impl DefaultLibrary {
    pub fn load_xml_dir(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        recover_removals(path)?;
        let mut entries = Vec::new();
        for file in xml_files(path)? {
            let entry = read_entry(&file)?;
            if !entry.url.is_empty() {
                entries.push(entry);
            }
        }
        Ok(Self { entries })
    }

    /// Persist one item without overwriting existing XML, then update the live library.
    pub fn add_item(
        &mut self,
        path: &Path,
        mode: u8,
        url: &str,
        password: &str,
    ) -> Result<PathBuf> {
        let entry = normalized_entry(mode, url, password)?;
        let xml = quick_xml::se::to_string(&entry)?;
        fs::create_dir_all(path)?;
        recover_removals(path)?;
        let mode_name = ["avatar", "world", "prop"][mode as usize];
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let mut counter = 0u64;
        let (file_path, mut file) = loop {
            let suffix = if counter == 0 {
                String::new()
            } else {
                format!("_{counter}")
            };
            let file_path = path.join(format!("{mode_name}_{stamp}{suffix}.xml"));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file_path)
            {
                Ok(file) => break (file_path, file),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => counter += 1,
                Err(error) => return Err(error.into()),
            }
        };
        if let Err(error) = file
            .write_all(format!("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n{xml}\n").as_bytes())
        {
            drop(file);
            let _ = fs::remove_file(&file_path);
            return Err(error.into());
        }
        self.entries.push(entry);
        Ok(file_path)
    }

    pub fn add_item_in_memory(&mut self, mode: u8, url: &str, password: &str) -> Result<()> {
        self.entries.push(normalized_entry(mode, url, password)?);
        Ok(())
    }

    /// Remove every XML with a matching URL, comparing case-insensitively like BasisVR.
    /// Unreadable files are retained, as they cannot safely be matched to the request.
    pub fn remove_item(&mut self, path: &Path, url: &str) -> Result<usize> {
        self.remove_item_with(path, url, |from, to| fs::rename(from, to))
    }

    fn remove_item_with(
        &mut self,
        path: &Path,
        url: &str,
        mut rename: impl FnMut(&Path, &Path) -> std::io::Result<()>,
    ) -> Result<usize> {
        ensure!(!url.trim().is_empty(), "URL was empty.");
        let files = if path.exists() {
            recover_removals(path)?;
            xml_files(path)?
                .into_iter()
                .filter(|file| read_entry(file).is_ok_and(|entry| urls_match(&entry.url, url)))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if files.is_empty() {
            self.remove_item_in_memory(url)?;
            return Ok(0);
        }
        // Stage all matches on the same filesystem before committing either view.
        // A later move failure restores earlier files instead of leaving a partial deletion.
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut counter = 0u64;
        let staging = loop {
            let dir = path.join(format!(".basis-remove-{stamp}-{counter}"));
            match fs::create_dir(&dir) {
                Ok(()) => break dir,
                Err(error) if error.kind() == ErrorKind::AlreadyExists => counter += 1,
                Err(error) => return Err(error.into()),
            }
        };
        let mut staged: Vec<(PathBuf, PathBuf)> = Vec::new();
        let result = (|| -> Result<()> {
            for file in &files {
                let target = staging.join(file.file_name().unwrap());
                rename(file, &target)
                    .with_context(|| format!("staging default library {}", file.display()))?;
                staged.push((file.clone(), target));
            }
            // The marker distinguishes committed cleanup remnants from interrupted transactions.
            fs::write(staging.join("committed"), b"")?;
            Ok(())
        })();
        if let Err(error) = result {
            let mut rollback_error = None;
            for (original, moved) in staged.iter().rev() {
                if let Err(error) = rename(moved, original) {
                    rollback_error = Some(error);
                }
            }
            if let Some(rollback_error) = rollback_error {
                // Retain any unrecovered originals for load-time recovery, never delete them.
                return Err(error).context(format!(
                    "rollback incomplete: {rollback_error}; recovery retained at {}",
                    staging.display()
                ));
            }
            let _ = fs::remove_dir(&staging);
            return Err(error);
        }
        self.remove_item_in_memory(url)?;
        // Removal has committed. Cleanup failure leaves only inactive files in a hidden directory;
        // it must not report failure after the disk and live views have both changed.
        let _ = fs::remove_dir_all(&staging);
        Ok(staged.len())
    }

    pub fn remove_item_in_memory(&mut self, url: &str) -> Result<usize> {
        ensure!(!url.trim().is_empty(), "URL was empty.");
        let previous = self.entries.len();
        self.entries.retain(|entry| !urls_match(&entry.url, url));
        Ok(previous - self.entries.len())
    }

    /// [u16 raw length][u16 compressed length][raw or LZ4 block payload].
    /// A zero compressed length means the payload is uncompressed.
    pub fn encode_library(&self) -> Result<Vec<u8>> {
        ensure!(
            self.entries.len() <= u16::MAX as usize,
            "too many default library entries"
        );
        let mut raw = NetWriter::new();
        raw.put_u16(self.entries.len() as u16);
        for entry in &self.entries {
            // NetWriter's string length includes an extra one for nonempty strings.
            ensure!(
                entry.url.len() < u16::MAX as usize,
                "default library URL is too long"
            );
            ensure!(
                entry.password.len() < u16::MAX as usize,
                "default library password is too long"
            );
            raw.put_u8(entry.mode);
            raw.put_string(&entry.url)?;
            raw.put_string(&entry.password)?;
        }
        let raw = raw.into_vec();
        ensure!(
            raw.len() <= u16::MAX as usize,
            "default library exceeds wire size limit"
        );
        let compressed = lz4_flex::block::compress(&raw);
        let use_compressed = !compressed.is_empty() && compressed.len() < raw.len();
        let mut wire = NetWriter::new();
        wire.put_u16(raw.len() as u16);
        wire.put_u16(if use_compressed {
            compressed.len() as u16
        } else {
            0
        });
        wire.put_bytes(if use_compressed { &compressed } else { &raw });
        Ok(wire.into_vec())
    }
}

fn recover_removals(path: &Path) -> Result<()> {
    for item in fs::read_dir(path)? {
        let item = item?;
        if !item.file_type()?.is_dir()
            || !item
                .file_name()
                .to_string_lossy()
                .starts_with(".basis-remove-")
        {
            continue;
        }
        let staging = item.path();
        if staging.join("committed").exists() {
            let _ = fs::remove_dir_all(&staging);
            continue;
        }
        for moved in xml_files(&staging)? {
            let original = path.join(moved.file_name().unwrap());
            // Hard links refuse to overwrite an operator file and leave the backup until recovery succeeds.
            match fs::hard_link(&moved, &original) {
                Ok(()) => {}
                Err(error)
                    if error.kind() == ErrorKind::AlreadyExists
                        && fs::read(&moved)? == fs::read(&original)? => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "recovering interrupted library removal of {}",
                            original.display()
                        )
                    });
                }
            }
            fs::remove_file(&moved)?;
        }
        fs::remove_dir(&staging)?;
    }
    Ok(())
}

fn normalized_entry(mode: u8, url: &str, password: &str) -> Result<DefaultLibraryEntry> {
    ensure!(!url.trim().is_empty(), "URL was empty.");
    ensure!(
        mode <= 2,
        "Unknown library mode {mode} (expected 0=Avatar, 1=World, 2=Prop)."
    );
    let mut password = password.to_owned();
    let url = if let Some((url, fragment)) = url.split_once('#') {
        if password.is_empty() && !fragment.is_empty() {
            // Convert.FromBase64String permits whitespace; UTF8.GetString replaces invalid bytes.
            let fragment: String = fragment.chars().filter(|c| !c.is_whitespace()).collect();
            if let Ok(decoded) = STANDARD.decode(fragment) {
                password = String::from_utf8_lossy(&decoded).into_owned();
            }
        }
        url
    } else {
        url
    };
    ensure!(!url.trim().is_empty(), "URL was empty.");
    Ok(DefaultLibraryEntry {
        mode,
        url: url.to_owned(),
        password,
    })
}

fn xml_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(|value| value.to_str()) == Some("xml")
        {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

fn read_entry(path: &Path) -> Result<DefaultLibraryEntry> {
    let xml = fs::read_to_string(path)
        .with_context(|| format!("reading default library {}", path.display()))?;
    // Serde XML checks fields but does not verify the root element's name itself.
    let mut reader = Reader::from_str(&xml);
    loop {
        match reader.read_event()? {
            Event::Start(element) | Event::Empty(element) => {
                ensure!(
                    element.name().as_ref() == b"BasisDefaultLibraryConfiguration",
                    "invalid default library XML root in {}",
                    path.display()
                );
                break;
            }
            Event::Eof => bail!("empty default library XML in {}", path.display()),
            _ => {}
        }
    }
    quick_xml::de::from_str(&xml)
        .with_context(|| format!("parsing default library {}", path.display()))
}

fn urls_match(left: &str, right: &str) -> bool {
    basis_protocol::permissions::ordinal_ignore_case_equal(left, right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_protocol::{
        messages::{BasisDeserialize, ServerLibraryMessage},
        NetReader,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "basis-library-{}-{stamp}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn csharp_xml_load_and_roundtrip_preserve_fields() {
        let dir = TestDir::new();
        fs::write(dir.0.join("existing.xml"), r#"<?xml version="1.0" encoding="utf-8"?>
<BasisDefaultLibraryConfiguration xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <!-- world -->
  <Mode>1</Mode><Url>https://host/world?x=1&amp;y=2</Url><Password>p&lt;&amp;&gt;</Password>
</BasisDefaultLibraryConfiguration>"#).unwrap();
        fs::write(dir.0.join("disabled.xml[remove]"), "ignored").unwrap();
        fs::write(dir.0.join("empty.xml"), "<BasisDefaultLibraryConfiguration><Mode>0</Mode><Url/><Password/></BasisDefaultLibraryConfiguration>").unwrap();
        let mut library = DefaultLibrary::load_xml_dir(&dir.0).unwrap();
        assert_eq!(
            library.entries,
            vec![DefaultLibraryEntry {
                mode: 1,
                url: "https://host/world?x=1&y=2".into(),
                password: "p<&>".into()
            }]
        );
        let first = library
            .add_item(&dir.0, 2, "https://host/prop#c2VjcmV0", "")
            .unwrap();
        let second = library
            .add_item(&dir.0, 2, "https://host/prop", "override")
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(read_entry(&first).unwrap(), library.entries[1]);
        assert_eq!(library.entries[1].password, "secret");
        let loaded = DefaultLibrary::load_xml_dir(&dir.0).unwrap();
        assert_eq!(loaded.entries.len(), 3);
        assert!(loaded.entries.contains(&library.entries[1]));
    }

    #[test]
    fn removal_deletes_all_case_insensitive_matches_and_skips_unreadable_xml() {
        let dir = TestDir::new();
        let mut library = DefaultLibrary::default();
        let first = library
            .add_item(&dir.0, 0, "https://host/item", "a")
            .unwrap();
        let second = library
            .add_item(&dir.0, 2, "HTTPS://HOST/ITEM", "b")
            .unwrap();
        let kept = library
            .add_item(&dir.0, 1, "https://host/other", "")
            .unwrap();
        fs::write(dir.0.join("broken.xml"), "<broken>").unwrap();
        assert_eq!(library.remove_item(&dir.0, "https://host/ITEM").unwrap(), 2);
        assert!(!first.exists() && !second.exists());
        assert!(kept.exists() && dir.0.join("broken.xml").exists());
        assert_eq!(library.entries.len(), 1);
        assert_eq!(library.remove_item(&dir.0, "absent").unwrap(), 0);
    }

    #[test]
    fn later_removal_failure_rolls_back_files_and_live_entries() {
        let dir = TestDir::new();
        let mut library = DefaultLibrary::default();
        let first = library
            .add_item(&dir.0, 0, "https://host/item", "a")
            .unwrap();
        let second = library
            .add_item(&dir.0, 1, "HTTPS://HOST/ITEM", "b")
            .unwrap();
        let before = library.entries.clone();
        let first_xml = fs::read(&first).unwrap();
        let second_xml = fs::read(&second).unwrap();
        let mut calls = 0;
        let result = library.remove_item_with(&dir.0, "https://host/item", |from, to| {
            calls += 1;
            if calls == 2 {
                Err(std::io::Error::new(
                    ErrorKind::PermissionDenied,
                    "injected later failure",
                ))
            } else {
                fs::rename(from, to)
            }
        });
        assert!(result.is_err());
        assert_eq!(library.entries, before);
        assert_eq!(fs::read(first).unwrap(), first_xml);
        assert_eq!(fs::read(second).unwrap(), second_xml);
        assert_eq!(
            DefaultLibrary::load_xml_dir(&dir.0).unwrap().entries.len(),
            before.len()
        );
        assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 2);
    }

    #[test]
    fn incomplete_rollback_recovers_original_files_when_reloaded() {
        let dir = TestDir::new();
        let mut library = DefaultLibrary::default();
        library.add_item(&dir.0, 0, "url", "a").unwrap();
        library.add_item(&dir.0, 1, "url", "b").unwrap();
        let before = library.entries.clone();
        let mut calls = 0;
        assert!(library
            .remove_item_with(&dir.0, "url", |from, to| {
                calls += 1;
                if calls == 2 || calls == 3 {
                    Err(std::io::Error::new(
                        ErrorKind::PermissionDenied,
                        "injected move/rollback failure",
                    ))
                } else {
                    fs::rename(from, to)
                }
            })
            .is_err());
        assert_eq!(library.entries, before);
        let reloaded = DefaultLibrary::load_xml_dir(&dir.0).unwrap();
        assert_eq!(reloaded.entries.len(), 2);
        assert!(before.iter().all(|entry| reloaded.entries.contains(entry)));
        assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 2);
    }

    fn decode_wire(library: &DefaultLibrary) -> (u16, ServerLibraryMessage) {
        let wire = library.encode_library().unwrap();
        let mut reader = NetReader::new(&wire);
        let raw_len = reader.get_u16().unwrap() as usize;
        let compressed_len = reader.get_u16().unwrap();
        let payload = reader
            .get_bytes(if compressed_len == 0 {
                raw_len
            } else {
                compressed_len as usize
            })
            .unwrap();
        assert_eq!(wire.len(), payload.len() + 4);
        let raw = if compressed_len == 0 {
            payload.to_vec()
        } else {
            lz4_flex::block::decompress(payload, raw_len).unwrap()
        };
        assert_eq!(raw.len(), raw_len);
        let message = ServerLibraryMessage::deserialize(&mut NetReader::new(&raw)).unwrap();
        (compressed_len, message)
    }

    #[test]
    fn library_wire_matches_protocol_for_empty_and_compressed_payloads() {
        let empty = DefaultLibrary::default();
        assert_eq!(empty.encode_library().unwrap(), [2, 0, 0, 0, 0, 0]);
        assert!(decode_wire(&empty).1.items.is_empty());
        let mut library = DefaultLibrary::default();
        library.add_item_in_memory(2, "u", "p").unwrap();
        assert_eq!(
            library.encode_library().unwrap(),
            [9, 0, 0, 0, 1, 0, 2, 2, 0, b'u', 2, 0, b'p']
        );
        library.entries.clear();
        for _ in 0..32 {
            library
                .add_item_in_memory(2, "https://host/repeated-prop", "secret")
                .unwrap();
        }
        let (compressed_len, message) = decode_wire(&library);
        assert!(compressed_len > 0);
        assert_eq!(message.items.len(), 32);
        assert_eq!(message.items[0].mode, 2);
        assert_eq!(message.items[0].url, "https://host/repeated-prop");
        assert_eq!(message.items[0].password, "secret");
    }

    #[test]
    fn memory_mutations_normalize_fragments_and_reject_invalid_input() {
        let mut library = DefaultLibrary::default();
        assert!(library.add_item_in_memory(3, "url", "").is_err());
        assert!(library.add_item_in_memory(0, " ", "").is_err());
        assert!(library.add_item_in_memory(0, "#c2VjcmV0", "").is_err());
        library.add_item_in_memory(0, "url#invalid!", "").unwrap();
        assert_eq!(library.entries[0].url, "url");
        assert!(library.entries[0].password.is_empty());
        library
            .add_item_in_memory(1, "url#c2VjcmV0", "override")
            .unwrap();
        assert_eq!(library.entries[1].password, "override");
        assert_eq!(library.remove_item_in_memory("URL").unwrap(), 2);
        assert!(library.remove_item_in_memory(" ").is_err());
    }

    #[test]
    fn malformed_xml_and_oversized_wire_report_errors() {
        let dir = TestDir::new();
        fs::write(dir.0.join("wrong.xml"), "<Wrong><Url>url</Url></Wrong>").unwrap();
        assert!(DefaultLibrary::load_xml_dir(&dir.0).is_err());
        fs::write(
            dir.0.join("wrong.xml"),
            "<BasisDefaultLibraryConfiguration><Url>url</Password>",
        )
        .unwrap();
        assert!(DefaultLibrary::load_xml_dir(&dir.0).is_err());
        let mut library = DefaultLibrary::default();
        library
            .add_item_in_memory(0, &"x".repeat(u16::MAX as usize), "")
            .unwrap();
        assert!(library.encode_library().is_err());
    }
}
