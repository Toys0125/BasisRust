//! Opt-in validation; the legacy loader and flat PascalCase schema remain unchanged.
use crate::config::RawConfig;
use anyhow::anyhow;
use anyhow::Result;
use quick_xml::{events::Event, Reader};
use std::{path::Path, str::FromStr};

fn xml_character(c: char) -> bool {
    matches!(c, '\u{9}' | '\u{a}' | '\u{d}' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}')
}

pub(super) fn validate_xml(text: &str, path: &Path) -> Result<()> {
    if text.chars().any(|c| !xml_character(c)) {
        return Err(anyhow!("config {}: invalid XML character", path.display()));
    }
    let mut reader = Reader::from_str(text);
    let mut depth = 0usize;
    let mut roots = 0usize;
    loop {
        let event = reader
            .read_event()
            .map_err(|error| anyhow!("config {}: malformed XML: {error}", path.display()))?;
        match event {
            Event::Start(ref element) | Event::Empty(ref element) => {
                for attribute in element.attributes() {
                    let attribute = attribute.map_err(|error| {
                        anyhow!(
                            "config {}: malformed XML attribute: {error}",
                            path.display()
                        )
                    })?;
                    let value = attribute
                        .decode_and_unescape_value(reader.decoder())
                        .map_err(|error| {
                            anyhow!(
                                "config {}: malformed XML attribute value: {error}",
                                path.display()
                            )
                        })?;
                    if value.chars().any(|c| !xml_character(c)) {
                        return Err(anyhow!(
                            "config {}: invalid XML attribute character",
                            path.display()
                        ));
                    }
                }
                if depth == 0 {
                    roots += 1;
                    if roots != 1 || element.name().as_ref() != b"Configuration" {
                        return Err(anyhow!(
                            "config {}: expected one Configuration root",
                            path.display()
                        ));
                    }
                }
                if matches!(event, Event::Start(_)) {
                    depth += 1;
                }
            }
            Event::End(_) => depth = depth.saturating_sub(1),
            Event::Text(text)
                if depth == 0 && text.as_ref().iter().any(|b| !b.is_ascii_whitespace()) =>
            {
                return Err(anyhow!(
                    "config {}: text outside Configuration root",
                    path.display()
                ));
            }
            Event::CData(_) | Event::GeneralRef(_) if depth == 0 => {
                return Err(anyhow!(
                    "config {}: data outside Configuration root",
                    path.display()
                ));
            }
            Event::GeneralRef(reference) => validate_reference(&reference, path)?,
            Event::Eof => break,
            _ => {}
        }
    }
    if roots != 1 || depth != 0 {
        return Err(anyhow!(
            "config {}: missing or unclosed Configuration root",
            path.display()
        ));
    }
    Ok(())
}

fn validate_reference(reference: &quick_xml::events::BytesRef<'_>, path: &Path) -> Result<()> {
    match reference.resolve_char_ref().map_err(|error| {
        anyhow!(
            "config {}: invalid XML character reference: {error}",
            path.display()
        )
    })? {
        Some(c) if !xml_character(c) => Err(anyhow!(
            "config {}: invalid XML character reference",
            path.display()
        )),
        None => {
            let name = reference.decode().map_err(|error| {
                anyhow!("config {}: invalid XML reference: {error}", path.display())
            })?;
            if quick_xml::escape::resolve_predefined_entity(&name).is_none() {
                return Err(anyhow!("config {}: undefined XML entity", path.display()));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn scalar<T: FromStr>(
    value: &Option<String>,
    field: &str,
    path: &Path,
    valid: impl FnOnce(&T) -> bool,
    expected: &str,
) -> Result<()> {
    if let Some(text) = value {
        // Do not echo supplied values: config files can contain credentials.
        let parsed = text.parse::<T>().map_err(|_| {
            anyhow!(
                "config {}: invalid {field}; expected {expected}",
                path.display()
            )
        })?;
        if !valid(&parsed) {
            return Err(anyhow!(
                "config {}: invalid {field}; expected {expected}",
                path.display()
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_scalars(raw: &RawConfig, path: &Path) -> Result<()> {
    scalar::<u16>(
        &raw.port,
        "Port",
        path,
        |_| true,
        "an integer from 0 to 65535",
    )?;
    scalar::<usize>(
        &raw.client_count,
        "ClientCount",
        path,
        |_| true,
        "a nonnegative client count",
    )?;
    scalar::<u8>(
        &raw.avatar_load_mode,
        "AvatarLoadMode",
        path,
        |_| true,
        "an integer from 0 to 255",
    )?;
    scalar::<bool>(
        &raw.voice_enabled,
        "VoiceEnabled",
        path,
        |_| true,
        "true or false",
    )?;
    scalar::<u8>(
        &raw.voice_speaker_percent,
        "VoiceSpeakerPercent",
        path,
        |v| *v <= 100,
        "an integer from 0 to 100",
    )?;
    scalar::<f32>(
        &raw.voice_hearing_distance,
        "VoiceHearingDistance",
        path,
        |v| v.is_finite() && *v >= 0.0,
        "a finite nonnegative distance",
    )?;
    scalar::<u64>(
        &raw.voice_frame_duration_ms,
        "VoiceFrameDurationMs",
        path,
        |v| *v > 0,
        "a positive integer duration in milliseconds",
    )?;
    Ok(())
}
