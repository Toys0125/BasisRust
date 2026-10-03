//! Ingress budgets apply before parsing/copying requests or generating replies.
use std::{
    collections::HashMap,
    hash::{BuildHasher, RandomState},
    net::IpAddr,
    time::{Duration, Instant},
};

pub(crate) const MAX_PENDING_REQUESTS: usize = 4096;
pub(crate) const REQUEST_TTL: Duration = Duration::from_secs(10);
const MAX_IPS: usize = 4096;
// Untracked addresses share bounded token buckets once the exact-IP table fills.
// Sharing can only make a bucket stricter; it cannot grant fresh per-IP budgets.
const OVERFLOW_BUCKETS: usize = 256;
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

#[derive(Clone)]
struct Entry {
    tokens: [f64; 3],
    updated: [Instant; 3],
    last_allowed: Instant,
}

pub(crate) struct Limiter {
    entries: HashMap<IpAddr, Entry>,
    overflow: Vec<Option<Entry>>,
    overflow_hash: RandomState,
    last_cleanup: Instant,
    global_tokens: [f64; 3],
    global_updated: [Instant; 3],
}

impl Default for Limiter {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            entries: HashMap::new(),
            overflow: (0..OVERFLOW_BUCKETS).map(|_| None).collect(),
            overflow_hash: RandomState::new(),
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
            for entry in &mut self.overflow {
                if entry
                    .as_ref()
                    .is_some_and(|entry| now.duration_since(entry.last_allowed) >= IP_TTL)
                {
                    *entry = None;
                }
            }
            self.last_cleanup = now;
        }
    }

    fn overflow_bucket(&self, ip: IpAddr) -> usize {
        self.overflow_hash.hash_one(ip) as usize % OVERFLOW_BUCKETS
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
        let bucket = self.overflow_bucket(ip);
        let entry = if self.entries.contains_key(&ip) {
            self.entries.get_mut(&ip).expect("entry checked above")
        } else if self.entries.len() < MAX_IPS {
            let seeded = self.overflow[bucket].clone().unwrap_or(Entry {
                tokens: [4096.0, 1.0, 32.0],
                updated: [now; 3],
                last_allowed: now,
            });
            self.entries.entry(ip).or_insert(seeded)
        } else {
            self.overflow[bucket].get_or_insert(Entry {
                tokens: [4096.0, 1.0, 32.0],
                updated: [now; 3],
                last_allowed: now,
            })
        };
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
    fn table_is_bounded_and_overflow_bucket_expires() {
        let mut limiter = Limiter::default();
        let now = Instant::now();
        for n in 0..MAX_IPS as u32 {
            assert!(limiter.allow(Ipv4Addr::from(n).into(), Kind::Connection, now));
        }
        let new_ip = Ipv4Addr::from(MAX_IPS as u32).into();
        assert!(limiter.allow(new_ip, Kind::Nat, now));
        assert_eq!(limiter.entries.len(), MAX_IPS);
        assert_eq!(limiter.overflow.iter().flatten().count(), 1);
        limiter.cleanup(now + IP_TTL);
        assert!(limiter.entries.is_empty());
        assert!(limiter.overflow.iter().all(Option::is_none));
        assert!(limiter.allow(new_ip, Kind::Info, now + IP_TTL));
    }

    #[test]
    fn overflow_addresses_share_a_budget_instead_of_resetting_it() {
        let mut limiter = Limiter::default();
        let now = Instant::now();
        for n in 0..MAX_IPS as u32 {
            assert!(limiter.allow(Ipv4Addr::from(n).into(), Kind::Connection, now));
        }

        // Pigeonhole bound guarantees a collision regardless of the random seed.
        let mut seen = HashMap::new();
        let mut pair = None;
        for n in MAX_IPS as u32..(MAX_IPS + OVERFLOW_BUCKETS + 1) as u32 {
            let candidate = Ipv4Addr::from(n);
            let bucket = limiter.overflow_bucket(candidate.into());
            if let Some(first) = seen.insert(bucket, candidate) {
                pair = Some((first, candidate));
                break;
            }
        }
        let (first, colliding) = pair.expect("more sources than overflow buckets");

        assert!(limiter.allow(first.into(), Kind::Info, now));
        assert!(!limiter.allow(first.into(), Kind::Info, now));
        assert!(!limiter.allow(colliding.into(), Kind::Info, now));
        assert_eq!(limiter.entries.len(), MAX_IPS);
        assert_eq!(limiter.overflow.iter().flatten().count(), 1);
    }

    #[test]
    fn promoting_overflow_ip_keeps_its_live_budget() {
        let mut limiter = Limiter::default();
        let start = Instant::now();
        for n in 0..MAX_IPS as u32 {
            assert!(limiter.allow(Ipv4Addr::from(n).into(), Kind::Connection, start));
        }

        let overflow_ip = IpAddr::V4(Ipv4Addr::from(MAX_IPS as u32));
        limiter.cleanup(start + Duration::from_secs(59));
        let first = start + Duration::from_millis(59_800);
        assert!(limiter.allow(overflow_ip, Kind::Info, first));

        // The exact entries expire at 60 seconds, while the overflow bucket
        // was refreshed 300 ms ago and must seed the newly available exact slot.
        let promoted = start + Duration::from_millis(60_100);
        assert!(!limiter.allow(overflow_ip, Kind::Info, promoted));
        assert!(limiter.entries.contains_key(&overflow_ip));
    }

    #[test]
    fn new_connection_ip_progresses_after_global_refill_at_capacity() {
        let mut limiter = Limiter::default();
        let now = Instant::now();
        for n in 0..MAX_IPS as u32 {
            let ip = Ipv4Addr::from(n);
            assert!(limiter.allow(ip.into(), Kind::Connection, now));
        }
        // The initial batch spends the whole global burst, but each existing
        // IP still has per-IP tokens and can remain active while the table is full.
        assert!(!limiter.allow(Ipv4Addr::from(MAX_IPS as u32).into(), Kind::Connection, now));
        assert!(limiter.allow(
            Ipv4Addr::from(MAX_IPS as u32).into(),
            Kind::Connection,
            now + Duration::from_millis(10)
        ));
        assert_eq!(limiter.entries.len(), MAX_IPS);
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
