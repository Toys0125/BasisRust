# BasisRustClient

A headless Rust client for the Basis VR platform that implements the Basis protocol using `basis-protocol` and `basis-transport`.

## Overview

This is a headless client implementation that mimics the behavior of the Basis VR client. It connects to the Basis server and can simulate multiple clients with configurable behavior including movement, chat, and resource loading.

## Features

- UDP-based client using the same protocol as the Basis server
- Configurable via XML config file
- Multiple client simulation support
- Movement and camera simulation
- Chat support
- Resource loading simulation
- Opt-in voice simulation from Ogg Opus files
- Automatic reconnection

## Running

```powershell
cargo run --release
```

Configuration is read from `Config.xml` by default. You can specify a different config and server address via command-line arguments:

```powershell
cargo run --release -- --help
```

Voice simulation is disabled by default. By default, it requires `ffmpeg` on
`PATH` to re-encode input files into Unity-compatible voice packets. To enable
it, place `.opus` or `.ogg` Ogg Opus files in the configured audio folder, then run:

```powershell
cargo run --release -- --voice --voice-audio-folder audio --voice-speaker-percent 10
```

The client re-encodes each input file through `ffmpeg` into 48 kHz mono Opus with
20 ms frames, then sends those packets over the Basis voice channel. Speaking
clients send to peers within the configured hearing distance, which defaults to
25 meters.

To bypass the ffmpeg re-encode/cache path and send packetized input Opus directly:

```powershell
cargo run --release -- --voice --no-voice-reencode
```

## Config

The client reads a `Config.xml` file for configuration including avatar data, server settings, and behavior parameters.
Voice settings include `VoiceEnabled`, `VoiceAudioFolder`, `VoiceSpeakerPercent`,
`VoiceHearingDistance`, and `VoiceFrameDurationMs`.

## Building

```powershell
cargo build --release
```

## Dependencies

This crate depends on:
- `basis-protocol` — Protocol definitions and message types (shared with the server)
- `basis-transport` — UDP transport layer (shared with the server)
