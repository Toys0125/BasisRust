//! Headless Basis client connection, simulation, and population runtime.
//! Wire definitions and codecs are shared with the server's protocol and transport crates.

mod avatar;
mod client;
mod config;
mod diagnostics;
mod identity;
mod net;
mod observer;
mod observer_sequence;
mod observer_session;
mod packet_diagnostics;
mod population;
mod receiver;
mod runtime;
mod simulation;
mod strict_config;
mod transport;
mod voice;
mod voice_diagnostics;
mod wire;

#[cfg(test)]
mod tests;

pub use config::{ClientOptions, Config};
pub use runtime::{run, ConsoleCommand};
