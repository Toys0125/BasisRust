//! Headless GPU distance and reduction-policy computation. Only the offload worker may call this
//! module: GPU submission, fencing, and readback never run on the server tick.

use std::{borrow::Cow, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, ensure, Context, Result};
use parking_lot::Mutex;

use crate::gpu_policy::{ReductionPolicy, DECISION_INVALID_FLAG};

const WORKGROUP_SIZE: u32 = 64;
const GPU_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

const SHADER: &str = r#"
struct Parameters {
    count: u32, groups_x: u32, base_interval: i32, padding0: u32,
    multiplier: f32, rate: f32, high: f32, medium: f32,
    low: f32, padding1: u32, padding2: u32, padding3: u32,
}
@group(0) @binding(0) var<storage, read> positions: array<vec4<f32>>;
@group(0) @binding(1) var<storage, read_write> decisions: array<u32>;
@group(0) @binding(2) var<uniform> parameters: Parameters;

fn finite(value: f32) -> bool {
    return (bitcast<u32>(value) & 0x7fffffffu) < 0x7f800000u;
}

fn decision(index: u32) -> u32 {
    let receiver = index / parameters.count;
    let sender = index % parameters.count;
    let difference = positions[receiver].xyz - positions[sender].xyz;
    let distance = difference.x * difference.x
        + difference.y * difference.y + difference.z * difference.z;
    if !finite(distance) || distance < 0.0 { return 0x0800u; }

    var quality = 0u;
    if distance <= parameters.high { quality = 3u; }
    else if distance <= parameters.medium { quality = 2u; }
    else if distance <= parameters.low { quality = 1u; }

    let epsilon = 1.1920928955078125e-7;
    let tolerance = 8.0 * epsilon * max(distance, 1.0);
    var correction = abs(distance - parameters.high) <= tolerance
        || abs(distance - parameters.medium) <= tolerance
        || abs(distance - parameters.low) <= tolerance;
    let base = f32(parameters.base_interval);
    // Once the encoded byte saturates, larger arithmetic has no observable
    // effect. Bound the intermediate operations to avoid shader overflow.
    let contribution_limit = (base + 854.0) / base + 1.0;
    var contribution = 0.0;
    if parameters.rate > 0.0 {
        if distance > contribution_limit / parameters.rate {
            contribution = contribution_limit;
        } else {
            contribution = distance * parameters.rate;
        }
    }
    let raw = base * (min(parameters.multiplier, contribution_limit) + contribution);
    // Nonnegative validated policy can overflow to +infinity: Rust's float to
    // integer cast saturates, and the byte is then unambiguously 255. Reject
    // NaN rather than allowing an undefined shader integer conversion.
    if (bitcast<u32>(raw) & 0x7fffffffu) > 0x7f800000u { return 0x0800u; }
    var interval = 2147483647i;
    if raw < 2147483648.0 {
        interval = i32(clamp(raw, 0.0, 2147483520.0));
    }
    let relative = max(interval - parameters.base_interval, 0i);
    var encoded = 255u;
    if relative < 200i { encoded = u32(relative); }
    else if relative < 854i { encoded = 200u + u32((relative - 200i + 6i) / 12i); }

    // Integer boundaries above the start of final-byte saturation cannot
    // change an encoded interval. Keep its lower boundary guarded as well.
    let interval_tolerance = abs(base) * abs(parameters.rate) * tolerance
        + 4.0 * epsilon * abs(raw);
    if finite(raw) && raw + interval_tolerance >= base + 1.0
        && raw - interval_tolerance <= base + 854.0 {
        correction = correction || abs(raw - round(raw)) <= interval_tolerance;
    }
    return (quality << 8u) | encoded | select(0u, 0x0400u, correction);
}

@compute @workgroup_size(64)
fn main(@builtin(workgroup_id) group: vec3<u32>,
        @builtin(local_invocation_id) local: vec3<u32>) {
    let word = (group.y * parameters.groups_x + group.x) * 64u + local.x;
    let pair_count = parameters.count * parameters.count;
    if word >= (pair_count + 1u) / 2u { return; }
    let first = word * 2u;
    var packed = decision(first);
    if first + 1u < pair_count { packed |= decision(first + 1u) << 16u; }
    decisions[word] = packed;
}
"#;

#[derive(Debug)]
struct BufferBucket {
    capacity: usize,
    positions: wgpu::Buffer,
    decisions: wgpu::Buffer,
    staging: wgpu::Buffer,
    parameters: wgpu::Buffer,
    bindings: wgpu::BindGroup,
}

/// Two retained buffer sets match the controller's alternating CPU snapshots.
/// A failed submission is returned to the controller for its CPU fallback.
#[derive(Debug)]
pub(super) struct GpuDistanceBackend {
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    adapter_name: String,
    buckets: [Option<BufferBucket>; 2],
    device_error: Arc<Mutex<Option<String>>>,
}

impl GpuDistanceBackend {
    pub(super) fn new(device_name: &str) -> Result<Self> {
        std::panic::catch_unwind(AssertUnwindSafe(|| Self::new_inner(device_name)))
            .map_err(|_| anyhow!("GPU backend initialization panicked"))?
    }

    fn new_inner(device_name: &str) -> Result<Self> {
        let backends = wgpu::Backends::VULKAN | wgpu::Backends::DX12 | wgpu::Backends::METAL;
        let mut descriptor = wgpu::InstanceDescriptor::new_without_display_handle();
        descriptor.backends = backends;
        let instance = wgpu::Instance::new(descriptor);
        let mut adapters: Vec<_> = pollster::block_on(instance.enumerate_adapters(backends))
            .into_iter()
            .filter(|adapter| {
                let info = adapter.get_info();
                let name = info.name.to_ascii_lowercase();
                info.device_type != wgpu::DeviceType::Cpu
                    && !["lavapipe", "llvmpipe", "swiftshader", "software"]
                        .iter()
                        .any(|software| name.contains(software))
            })
            .collect();
        // Stable, useful default: dedicated GPUs first, then integrated GPUs.
        adapters.sort_by_key(|adapter| {
            let info = adapter.get_info();
            let priority = match info.device_type {
                wgpu::DeviceType::DiscreteGpu => 0,
                wgpu::DeviceType::IntegratedGpu => 1,
                _ => 2,
            };
            (priority, info.name, format!("{:?}", info.backend))
        });
        let selector = device_name.trim();
        let selected = if selector.is_empty() {
            0
        } else if let Ok(index) = selector.parse::<usize>() {
            index
        } else {
            let selector = selector.to_ascii_lowercase();
            adapters
                .iter()
                .position(|adapter| {
                    adapter
                        .get_info()
                        .name
                        .to_ascii_lowercase()
                        .contains(&selector)
                })
                .ok_or_else(|| anyhow!("no hardware GPU matches ComputeDevice {device_name:?}"))?
        };
        ensure!(
            selected < adapters.len(),
            "hardware GPU index {selected} is unavailable"
        );
        let adapter = adapters.swap_remove(selected);
        let info = adapter.get_info();
        let adapter_name = format!("{} ({:?})", info.name, info.backend);
        let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
            label: Some("Basis distance offload"),
            required_limits: wgpu::Limits::default(),
            ..Default::default()
        }))
        .context("requesting hardware GPU device")?;
        let device_error = Arc::new(Mutex::new(None));
        let error_target = Arc::clone(&device_error);
        device.on_uncaptured_error(Arc::new(move |error: wgpu::Error| {
            *error_target.lock() = Some(error.to_string());
        }));
        let lost_target = Arc::clone(&device_error);
        device.set_device_lost_callback(move |reason, message| {
            *lost_target.lock() = Some(format!("GPU device lost ({reason:?}): {message}"));
        });
        let scopes = error_scopes(&device);
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Basis distance and reduction policy"),
            source: wgpu::ShaderSource::Wgsl(Cow::Borrowed(SHADER)),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("Basis distance and reduction policy"),
            layout: None,
            module: &shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        check_scopes(scopes)?;
        Ok(Self {
            device,
            queue,
            pipeline,
            adapter_name,
            buckets: [None, None],
            device_error,
        })
    }

    pub(super) fn adapter_name(&self) -> &str {
        &self.adapter_name
    }

    /// Return receiver-major packed decisions for the exact input snapshot.
    /// Boundary flags are repaired by the worker before publishing a bucket.
    /// Device errors and bounded worker waits become errors, never tick waits.
    pub(super) fn compute(
        &mut self,
        bucket: usize,
        positions: &[[f32; 4]],
        policy: &ReductionPolicy,
    ) -> Result<Vec<u16>> {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.compute_inner(bucket, positions, policy)
        }))
        .map_err(|_| anyhow!("GPU distance computation panicked"))?
    }

    fn compute_inner(
        &mut self,
        bucket: usize,
        positions: &[[f32; 4]],
        policy: &ReductionPolicy,
    ) -> Result<Vec<u16>> {
        ensure!(
            policy.validate(),
            "GPU reduction policy is unsupported or non-finite"
        );
        ensure!(
            bucket < self.buckets.len(),
            "GPU bucket index must be 0 or 1"
        );
        if let Some(error) = self.device_error.lock().as_ref() {
            bail!("GPU device error: {error}");
        }
        ensure!(
            positions
                .iter()
                .all(|p| p[..3].iter().all(|v| v.is_finite())),
            "GPU positions must have finite coordinates"
        );
        let count = positions.len();
        if count == 0 {
            return Ok(Vec::new());
        }
        // WGSL arithmetic overflow is not a portable finite/NaN test. Bound
        // all pair differences from the snapshot's AABB before dispatch.
        let mut minimum = [f64::INFINITY; 3];
        let mut maximum = [f64::NEG_INFINITY; 3];
        for position in positions {
            for axis in 0..3 {
                minimum[axis] = minimum[axis].min(position[axis] as f64);
                maximum[axis] = maximum[axis].max(position[axis] as f64);
            }
        }
        let maximum_distance: f64 = (0..3)
            .map(|axis| (maximum[axis] - minimum[axis]).powi(2))
            .sum();
        ensure!(
            maximum_distance <= f32::MAX as f64 * 0.5,
            "GPU position range can overflow squared-distance arithmetic"
        );
        let pair_count = count
            .checked_mul(count)
            .context("GPU distance count overflow")?;
        ensure!(
            pair_count <= u32::MAX as usize,
            "GPU distance matrix exceeds shader indexing"
        );
        let word_count = pair_count.div_ceil(2);
        let decision_bytes = (word_count as u64)
            .checked_mul(4)
            .context("GPU distance size overflow")?;
        let position_bytes = (count as u64)
            .checked_mul(16)
            .context("GPU position size overflow")?;
        let limits = self.device.limits();
        let maximum_bytes = limits
            .max_buffer_size
            .min(limits.max_storage_buffer_binding_size);
        ensure!(decision_bytes <= maximum_bytes && position_bytes <= maximum_bytes,
            "GPU distance matrix ({decision_bytes} bytes) exceeds device limit ({maximum_bytes} bytes)");
        let (groups_x, groups_y) = dispatch_dimensions(
            word_count as u32,
            limits.max_compute_workgroups_per_dimension,
        )?;
        let scopes = error_scopes(&self.device);
        let result = (|| {
            let needs_resize = self.buckets[bucket]
                .as_ref()
                .is_none_or(|b| b.capacity < count);
            if needs_resize {
                // Modest growth avoids allocation every time a peer connects.
                // At the device limit retain an exact-size allocation instead.
                let rounded = count.max(32).checked_next_power_of_two().unwrap_or(count);
                let capacity = if (rounded as u64)
                    .saturating_mul(rounded as u64)
                    .div_ceil(2)
                    .saturating_mul(4)
                    <= maximum_bytes
                {
                    rounded
                } else {
                    count
                };
                self.buckets[bucket] = Some(self.create_bucket(capacity));
            }
            let buffers = self.buckets[bucket].as_ref().expect("allocated GPU bucket");
            self.queue
                .write_buffer(&buffers.positions, 0, bytemuck::cast_slice(positions));
            self.queue.write_buffer(
                &buffers.parameters,
                0,
                bytemuck::cast_slice(&[
                    count as u32,
                    groups_x,
                    policy.base_interval_ms as u32,
                    0,
                    policy.base_multiplier.to_bits(),
                    policy.increase_rate.to_bits(),
                    policy.high_distance_sq.to_bits(),
                    policy.medium_distance_sq.to_bits(),
                    policy.low_distance_sq.to_bits(),
                    0,
                    0,
                    0,
                ]),
            );
            let mut encoder = self
                .device
                .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                    label: Some("Basis distance submission"),
                });
            {
                let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                    label: Some("Basis distance pass"),
                    timestamp_writes: None,
                });
                pass.set_pipeline(&self.pipeline);
                pass.set_bind_group(0, &buffers.bindings, &[]);
                pass.dispatch_workgroups(groups_x, groups_y, 1);
            }
            encoder.copy_buffer_to_buffer(
                &buffers.decisions,
                0,
                &buffers.staging,
                0,
                decision_bytes,
            );
            let submission = self.queue.submit([encoder.finish()]);
            let slice = buffers.staging.slice(..decision_bytes);
            let (map_sender, map_receiver) = std::sync::mpsc::sync_channel(1);
            slice.map_async(wgpu::MapMode::Read, move |result| {
                let _ = map_sender.send(result);
            });
            let mapping = self
                .device
                .poll(wgpu::PollType::Wait {
                    submission_index: Some(submission),
                    timeout: Some(GPU_WAIT_TIMEOUT),
                })
                .context("waiting for GPU distance submission")
                .and_then(|_| {
                    map_receiver
                        .recv_timeout(GPU_WAIT_TIMEOUT)
                        .context("waiting for GPU readback callback")
                })
                .and_then(|mapped| mapped.context("mapping GPU distance readback"));
            if let Err(error) = mapping {
                buffers.staging.unmap();
                return Err(error);
            }
            let decisions = slice
                .get_mapped_range()
                .context("accessing GPU decision readback")
                .and_then(|mapped| {
                    let words = bytemuck::try_cast_slice::<u8, u32>(&mapped)
                        .map_err(|error| anyhow!("invalid GPU decision readback: {error}"))?;
                    // Decode directly from the mapped words into the retained
                    // halfword representation, with no intermediate matrix.
                    let mut values = Vec::with_capacity(pair_count);
                    let mut invalid = false;
                    for &word in words {
                        let first = word as u16;
                        invalid |= first & DECISION_INVALID_FLAG != 0;
                        values.push(first);
                        if values.len() < pair_count {
                            let second = (word >> 16) as u16;
                            invalid |= second & DECISION_INVALID_FLAG != 0;
                            values.push(second);
                        }
                    }
                    ensure!(
                        !invalid,
                        "GPU decision matrix contains invalid distances or intervals"
                    );
                    Ok(values)
                });
            buffers.staging.unmap();
            decisions
        })();
        let scope_result = check_scopes(scopes);
        scope_result?;
        if let Some(error) = self.device_error.lock().as_ref() {
            bail!("GPU device error: {error}");
        }
        result
    }

    fn create_bucket(&self, capacity: usize) -> BufferBucket {
        let make_buffer = |label, size, usage| {
            self.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some(label),
                size,
                usage,
                mapped_at_creation: false,
            })
        };
        let positions = make_buffer(
            "Basis distance positions",
            capacity as u64 * 16,
            wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
        );
        let decision_bytes = (capacity as u64 * capacity as u64).div_ceil(2) * 4;
        let decisions = make_buffer(
            "Basis distance results",
            decision_bytes,
            wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        );
        let staging = make_buffer(
            "Basis distance readback",
            decision_bytes,
            wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        );
        let parameters = make_buffer(
            "Basis distance parameters",
            48,
            wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        );
        let bindings = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Basis distance bindings"),
            layout: &self.pipeline.get_bind_group_layout(0),
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: positions.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: decisions.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: parameters.as_entire_binding(),
                },
            ],
        });
        BufferBucket {
            capacity,
            positions,
            decisions,
            staging,
            parameters,
            bindings,
        }
    }
}

fn error_scopes(device: &wgpu::Device) -> [wgpu::ErrorScopeGuard; 3] {
    [
        device.push_error_scope(wgpu::ErrorFilter::OutOfMemory),
        device.push_error_scope(wgpu::ErrorFilter::Internal),
        device.push_error_scope(wgpu::ErrorFilter::Validation),
    ]
}

fn check_scopes(scopes: [wgpu::ErrorScopeGuard; 3]) -> Result<()> {
    let mut first_error = None;
    for scope in scopes.into_iter().rev() {
        if let Some(error) = pollster::block_on(scope.pop()) {
            first_error.get_or_insert(error);
        }
    }
    if let Some(error) = first_error {
        bail!("GPU operation failed: {error}");
    }
    Ok(())
}

fn dispatch_dimensions(word_count: u32, maximum_dimension: u32) -> Result<(u32, u32)> {
    ensure!(
        maximum_dimension > 0,
        "GPU does not support compute dispatch"
    );
    let groups = word_count.div_ceil(WORKGROUP_SIZE);
    let x = groups.min(maximum_dimension);
    let y = groups.div_ceil(x);
    ensure!(
        y <= maximum_dimension,
        "GPU distance dispatch exceeds device dimensions"
    );
    Ok((x, y))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gpu_policy::{DECISION_CORRECTION_FLAG, DECISION_VALUE_MASK};

    fn default_policy() -> ReductionPolicy {
        ReductionPolicy {
            base_interval_ms: 20,
            base_multiplier: 1.0,
            increase_rate: 0.005,
            high_distance_sq: 100.0,
            medium_distance_sq: 400.0,
            low_distance_sq: 1_600.0,
        }
    }

    #[test]
    fn dispatch_spans_two_dimensions_after_device_limit() {
        assert_eq!(dispatch_dimensions(2_000_000, 65_535).unwrap(), (31_250, 1));
        assert_eq!(dispatch_dimensions(9_000_000, 65_535).unwrap(), (65_535, 3));
        assert!(dispatch_dimensions(1_000_000, 2).is_err());
    }

    fn assert_parity(
        gpu: &mut GpuDistanceBackend,
        bucket: usize,
        positions: &[[f32; 4]],
        policy: &ReductionPolicy,
    ) -> Vec<u16> {
        let decisions = gpu.compute(bucket, positions, policy).unwrap();
        let count = positions.len();
        assert_eq!(decisions.len(), count * count);
        let mut corrections = 0;
        for (index, &decision) in decisions.iter().enumerate() {
            let a = positions[index / count];
            let b = positions[index % count];
            let dx = a[0] - b[0];
            let dy = a[1] - b[1];
            let dz = a[2] - b[2];
            let expected = policy.cpu_decision(dx * dx + dy * dy + dz * dz);
            let corrected = if decision & DECISION_CORRECTION_FLAG != 0 {
                corrections += 1;
                expected
            } else {
                decision & DECISION_VALUE_MASK
            };
            assert_eq!(corrected, expected, "pair {index}, policy {policy:?}");
            assert_eq!(
                decision & !(DECISION_VALUE_MASK | DECISION_CORRECTION_FLAG),
                0
            );
        }
        eprintln!("Policy parity: {count} peers, {corrections} boundary corrections");
        decisions
    }

    /// Run explicitly on hardware: cargo test -p basis-server-core
    /// hardware_gpu_distance_parity -- --ignored --nocapture
    #[test]
    #[ignore = "requires a hardware Vulkan, DX12, or Metal GPU"]
    fn hardware_gpu_distance_parity() {
        let mut gpu = GpuDistanceBackend::new("").expect("hardware GPU required");
        eprintln!("Hardware reduction test: {}", gpu.adapter_name());
        let policy = default_policy();
        // Check all directed pairs, including odd rows/halfwords, retained
        // bucket reuse, growth, and shrinking after the 2,000-peer allocation.
        for (bucket, count) in [(0, 4), (1, 2_000), (0, 65), (1, 3), (0, 1)] {
            let positions: Vec<_> = (0..count)
                .map(|index| {
                    [
                        index as f32 * 0.37,
                        (index % 7) as f32 * 1.13,
                        (index % 11) as f32 * -0.91,
                        0.0,
                    ]
                })
                .collect();
            assert_parity(&mut gpu, bucket, &positions, &policy);
        }
        let mut positions = vec![[0.0; 4]];
        for threshold in [10.0_f32, 20.0, 40.0] {
            for bits in [
                threshold.to_bits() - 1,
                threshold.to_bits(),
                threshold.to_bits() + 1,
            ] {
                positions.push([f32::from_bits(bits), 0.0, 0.0, 0.0]);
            }
            positions.push([
                threshold / 3.0,
                threshold * 2.0 / 3.0,
                threshold * 2.0 / 3.0,
                0.0,
            ]);
        }
        let decisions = assert_parity(&mut gpu, 0, &positions, &policy);
        for decision in decisions.iter().take(positions.len()).skip(1) {
            assert_ne!(
                decision & DECISION_CORRECTION_FLAG,
                0,
                "quality threshold must request CPU snapshot correction"
            );
        }
        // Direct and extended encoding edges, rounded 12 ms steps, and the
        // 854 ms start of saturation. Include adjacent representable positions
        // so f32 multiply/add contractions must either agree or request repair.
        let interval_policy = ReductionPolicy {
            increase_rate: 0.05,
            ..policy
        };
        let mut interval_positions = vec![[0.0; 4]];
        for relative in [
            1.0_f32, 199.0, 200.0, 205.0, 206.0, 211.0, 212.0, 217.0, 218.0, 853.0, 854.0, 855.0,
        ] {
            let coordinate = relative.sqrt();
            for bits in [
                coordinate.to_bits() - 1,
                coordinate.to_bits(),
                coordinate.to_bits() + 1,
            ] {
                interval_positions.push([f32::from_bits(bits), 0.0, 0.0, 0.0]);
            }
        }
        assert_parity(&mut gpu, 1, &interval_positions, &interval_policy);
        for varied in [
            ReductionPolicy {
                base_interval_ms: 47,
                base_multiplier: 0.25,
                increase_rate: 0.017,
                high_distance_sq: 22.3,
                medium_distance_sq: 95.1,
                low_distance_sq: 333.7,
            },
            ReductionPolicy {
                base_multiplier: 0.5,
                increase_rate: 0.0,
                ..policy
            },
            ReductionPolicy {
                base_interval_ms: i32::MAX - 2_048,
                base_multiplier: 1.0,
                increase_rate: 0.0000001,
                ..policy
            },
            ReductionPolicy {
                base_multiplier: f32::MAX,
                increase_rate: f32::MAX,
                ..policy
            },
            ReductionPolicy {
                high_distance_sq: 1600.0,
                medium_distance_sq: 100.0,
                low_distance_sq: 400.0,
                ..policy
            },
        ] {
            assert_parity(&mut gpu, 0, &interval_positions, &varied);
        }
        // Large finite distance and interval overflow saturate the byte, while
        // an overflowing squared distance rejects the bucket for CPU fallback.
        assert_parity(&mut gpu, 1, &[[0.0; 4], [1.0e15, 0.0, 0.0, 0.0]], &policy);
        assert_parity(
            &mut gpu,
            0,
            &[
                [0.0; 4],
                [1.0e-20, 0.0, 0.0, 0.0],
                [1.0e-19, 1.0e-19, 0.0, 0.0],
            ],
            &ReductionPolicy {
                high_distance_sq: 0.0,
                medium_distance_sq: 1.0e-39,
                low_distance_sq: 1.0e-37,
                increase_rate: 1.0e36,
                ..policy
            },
        );
        assert!(gpu
            .compute(0, &[[0.0; 4], [f32::MAX, 0.0, 0.0, 0.0]], &policy)
            .is_err());
        assert!(gpu.compute(2, &[], &policy).is_err());
        assert!(gpu
            .compute(
                0,
                &[],
                &ReductionPolicy {
                    increase_rate: f32::from_bits(1),
                    ..policy
                }
            )
            .is_err());
        assert!(gpu
            .compute(0, &[[f32::NAN, 0.0, 0.0, 0.0]], &policy)
            .is_err());
        assert!(gpu
            .compute(
                0,
                &[],
                &ReductionPolicy {
                    increase_rate: -1.0,
                    ..policy
                }
            )
            .is_err());
        assert!(gpu
            .compute(
                0,
                &[],
                &ReductionPolicy {
                    high_distance_sq: f32::INFINITY,
                    ..policy
                }
            )
            .is_err());
        assert!(gpu
            .compute(
                0,
                &[],
                &ReductionPolicy {
                    base_interval_ms: 0,
                    ..policy
                }
            )
            .is_err());
        assert_eq!(gpu.compute(0, &[], &policy).unwrap(), Vec::<u16>::new());
    }
}
