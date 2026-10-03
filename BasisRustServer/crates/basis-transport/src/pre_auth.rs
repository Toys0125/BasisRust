//! Ingress budgets apply before parsing/copying requests or generating replies.
use std::{
    collections::HashMap,
    net::IpAddr,
    time::{Duration, Instant},
};

pub(crate) const MAX_PENDING_REQUESTS: usize = 4096;
pub(crate) const REQUEST_TTL: Duration = Duration::from_secs(10);
const MAX_IPS: usize = 4096;
const IP_TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Connection,
    Info,
    Nat,
}

impl Kind {
    fn index(self) -> usize {
        self as usize
    }
    fn budget(self) -> (f64, f64) {
        match self {
            // Permit large legitimate batches behind a NAT, then bound retries.
            Self::Connection => (4096.0, 128.0),
            Self::Info => (1.0, 2.0),
            Self::Nat => (32.0, 8.0),
        }
    }
}

struct Entry {
    tokens: [f64; 3],
    updated: [Instant; 3],
    last_allowed: Instant,
}

pub(crate) struct Limiter {
    entries: HashMap<IpAddr, Entry>,
    last_cleanup: Instant,
    global_tokens: [f64; 3],
    global_updated: [Instant; 3],
}

impl Default for Limiter {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            entries: HashMap::new(),
            last_cleanup: now,
            global_tokens: [4096.0, 200.0, 256.0],
            global_updated: [now; 3],
        }
    }
}

impl Limiter {
    pub(crate) fn cleanup(&mut self, now: Instant) {
        if now.duration_since(self.last_cleanup) >= Duration::from_secs(1) {
            self.entries
                .retain(|_, entry| now.duration_since(entry.last_allowed) < IP_TTL);
            self.last_cleanup = now;
        }
    }

    pub(crate) fn allow(&mut self, ip: IpAddr, kind: Kind, now: Instant) -> bool {
        self.cleanup(now);
        let index = kind.index();
        let (global_burst, global_rate) = match kind {
            Kind::Connection => (4096.0, 512.0),
            Kind::Info => (200.0, 100.0),
            Kind::Nat => (256.0, 128.0),
        };
        self.global_tokens[index] = (self.global_tokens[index]
            + now
                .saturating_duration_since(self.global_updated[index])
                .as_secs_f64()
                * global_rate)
            .min(global_burst);
        self.global_updated[index] = now;
        if self.global_tokens[index] < 1.0 {
            return false;
        }
        let ip = match ip {
            IpAddr::V6(ip) => ip
                .to_ipv4_mapped()
                .map(IpAddr::V4)
                .unwrap_or(IpAddr::V6(ip)),
            ip => ip,
        };
        if !self.entries.contains_key(&ip) && self.entries.len() >= MAX_IPS {
            return false;
        }
        let entry = self.entries.entry(ip).or_insert_with(|| Entry {
            tokens: [4096.0, 1.0, 32.0],
            updated: [now; 3],
            last_allowed: now,
        });
        let (burst, rate) = kind.budget();
        entry.tokens[index] = (entry.tokens[index]
            + now
                .saturating_duration_since(entry.updated[index])
                .as_secs_f64()
                * rate)
            .min(burst);
        entry.updated[index] = now;
        if entry.tokens[index] < 1.0 {
            return false;
        }
        entry.tokens[index] -= 1.0;
        self.global_tokens[index] -= 1.0;
        entry.last_allowed = now;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn budgets_are_per_ip_and_kind_and_refill_without_port_bypass() {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let mut limiter = Limiter::default();
        let now = Instant::now();
        assert!(limiter.allow(ip, Kind::Info, now));
        assert!(!limiter.allow(ip, Kind::Info, now));
        assert!(!limiter.allow(
            IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
            Kind::Info,
            now
        ));
        assert!(limiter.allow(ip, Kind::Nat, now));
        assert!(limiter.allow("127.0.0.2".parse().unwrap(), Kind::Info, now));
        assert!(limiter.allow(ip, Kind::Info, now + Duration::from_millis(500)));
        assert!(!limiter.allow(ip, Kind::Info, now + Duration::from_millis(500)));
        for _ in 0..2000 {
            assert!(limiter.allow(ip, Kind::Connection, now + Duration::from_millis(500)));
        }
    }

    #[test]
    fn table_is_bounded_and_idle_entries_expire() {
        let mut limiter = Limiter::default();
        let now = Instant::now();
        for n in 0..MAX_IPS as u32 {
            assert!(limiter.allow(Ipv4Addr::from(n).into(), Kind::Connection, now));
        }
        let new_ip = Ipv4Addr::from(MAX_IPS as u32).into();
        assert!(!limiter.allow(new_ip, Kind::Nat, now));
        assert_eq!(limiter.entries.len(), MAX_IPS);
        limiter.cleanup(now + IP_TTL);
        assert!(limiter.entries.is_empty());
        assert!(limiter.allow(new_ip, Kind::Info, now + IP_TTL));
    }
    #[test]
    fn global_reply_budget_bounds_spoofed_sources() {
        let mut limiter = Limiter::default();
        let now = Instant::now();
        for n in 0..200u32 {
            assert!(limiter.allow(Ipv4Addr::from(n).into(), Kind::Info, now));
        }
        assert!(!limiter.allow(Ipv4Addr::from(200).into(), Kind::Info, now));
        assert!(limiter.allow(
            Ipv4Addr::from(200).into(),
            Kind::Info,
            now + Duration::from_millis(10)
        ));
        assert_eq!(limiter.entries.len(), 201);
    }
}
