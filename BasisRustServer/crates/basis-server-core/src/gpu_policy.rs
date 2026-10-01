//! Policy captured with each GPU bucket. Packed decisions contain no live state.
use basis_protocol::channels;

pub(crate) const DECISION_VALUE_MASK: u16 = 0x03ff;
pub(crate) const DECISION_CORRECTION_FLAG: u16 = 0x0400;
pub(crate) const DECISION_INVALID_FLAG: u16 = 0x0800;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReductionPolicy {
    pub base_interval_ms: i32,
    pub base_multiplier: f32,
    pub increase_rate: f32,
    pub high_distance_sq: f32,
    pub medium_distance_sq: f32,
    pub low_distance_sq: f32,
}

impl PartialEq for ReductionPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.base_interval_ms == other.base_interval_ms
            && [
                self.base_multiplier,
                self.increase_rate,
                self.high_distance_sq,
                self.medium_distance_sq,
                self.low_distance_sq,
            ]
            .map(f32::to_bits)
                == [
                    other.base_multiplier,
                    other.increase_rate,
                    other.high_distance_sq,
                    other.medium_distance_sq,
                    other.low_distance_sq,
                ]
                .map(f32::to_bits)
    }
}
impl Eq for ReductionPolicy {}

#[cfg(test)]
impl Default for ReductionPolicy {
    fn default() -> Self {
        Self {
            base_interval_ms: 20,
            base_multiplier: 1.0,
            increase_rate: 0.005,
            high_distance_sq: 100.0,
            medium_distance_sq: 400.0,
            low_distance_sq: 1600.0,
        }
    }
}

impl ReductionPolicy {
    pub fn validate(&self) -> bool {
        (1..=i32::MAX - 2048).contains(&self.base_interval_ms)
            && [
                self.base_multiplier,
                self.increase_rate,
                self.high_distance_sq,
                self.medium_distance_sq,
                self.low_distance_sq,
            ]
            .iter()
            .all(|v| v.is_finite())
            && self.base_multiplier >= 0.0
            && self.increase_rate >= 0.0
            && (self.increase_rate == 0.0 || self.increase_rate >= f32::MIN_POSITIVE)
    }

    pub fn cpu_decision(&self, distance_sq: f32) -> u16 {
        let quality = if distance_sq <= self.high_distance_sq {
            3
        } else if distance_sq <= self.medium_distance_sq {
            2
        } else if distance_sq <= self.low_distance_sq {
            1
        } else {
            0
        };
        let raw = (self.base_interval_ms as f32
            * (self.base_multiplier + distance_sq * self.increase_rate)) as i32;
        let byte = channels::encode_avatar_interval_byte(raw, self.base_interval_ms);
        ((quality as u16) << 8) | byte as u16
    }

    pub fn interval_table(&self) -> [u64; 256] {
        std::array::from_fn(|byte| {
            channels::decode_avatar_interval_ms(byte as u8, self.base_interval_ms).max(1) as u64
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReductionDecision {
    pub quality: u8,
    pub interval_byte: u8,
    pub interval_ms: u64,
}
