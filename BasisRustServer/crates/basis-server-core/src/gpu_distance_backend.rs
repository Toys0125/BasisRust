//! Headless GPU distance computation. Only the offload worker may call this
//! module: GPU submission, fencing, and readback never run on the server tick.

use std::{borrow::Cow, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, ensure, Context, Result};
use parking_lot::Mutex;

const WORKGROUP_SIZE: u32 = 64;
const GPU_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

const SHADER: &str = r#"
struct Parameters { count: u32, groups_x: u32, padding0: u32, padding1: u32 }
@group(0) @binding(0) var<storage, read> positions: array<vec4<f32>>;
@group(0) @binding(1) var<storage, read_write> distances: array<f32>;
@group(0) @binding(2) var<uniform> parameters: Parameters;

@compute @workgroup_size(64)
fn main(@builtin(workgroup_id) group: vec3<u32>,
        @builtin(local_invocation_id) local: vec3<u32>) {
    let index = (group.y * parameters.groups_x + group.x) * 64u + local.x;
    if index >= parameters.count * parameters.count { return; }
    let receiver = index / parameters.count;
    let sender = index % parameters.count;
    let difference = positions[receiver].xyz - positions[sender].xyz;
    distances[index] = difference.x * difference.x
        + difference.y * difference.y + difference.z * difference.z;
}
"#;

#[derive(Debug)]
struct BufferBucket {
    capacity: usize,
    positions: wgpu::Buffer,
    distances: wgpu::Buffer,
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
            label: Some("Basis squared distances"),
            source: wgpu::ShaderSource::Wgsl(Cow::Borrowed(SHADER)),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("Basis squared distances"),
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

    /// Return receiver-major squared distances for the exact input snapshot.
    /// Device errors and bounded worker waits become errors, never tick waits.
    pub(super) fn compute(&mut self, bucket: usize, positions: &[[f32; 4]]) -> Result<Vec<f32>> {
        std::panic::catch_unwind(AssertUnwindSafe(|| self.compute_inner(bucket, positions)))
            .map_err(|_| anyhow!("GPU distance computation panicked"))?
    }

    fn compute_inner(&mut self, bucket: usize, positions: &[[f32; 4]]) -> Result<Vec<f32>> {
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
        if count <= 1 {
            return Ok(vec![0.0; count]);
        }
        let pair_count = count
            .checked_mul(count)
            .context("GPU distance count overflow")?;
        ensure!(
            pair_count <= u32::MAX as usize,
            "GPU distance matrix exceeds shader indexing"
        );
        let distance_bytes = (pair_count as u64)
            .checked_mul(4)
            .context("GPU distance size overflow")?;
        let position_bytes = (count as u64)
            .checked_mul(16)
            .context("GPU position size overflow")?;
        let limits = self.device.limits();
        let maximum_bytes = limits
            .max_buffer_size
            .min(limits.max_storage_buffer_binding_size);
        ensure!(distance_bytes <= maximum_bytes && position_bytes <= maximum_bytes,
            "GPU distance matrix ({distance_bytes} bytes) exceeds device limit ({maximum_bytes} bytes)");
        let (groups_x, groups_y) = dispatch_dimensions(
            pair_count as u32,
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
                bytemuck::cast_slice(&[count as u32, groups_x, 0, 0]),
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
                &buffers.distances,
                0,
                &buffers.staging,
                0,
                distance_bytes,
            );
            let submission = self.queue.submit([encoder.finish()]);
            let slice = buffers.staging.slice(..distance_bytes);
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
            let distances = slice
                .get_mapped_range()
                .context("accessing GPU distance readback")
                .and_then(|mapped| {
                    bytemuck::try_cast_slice::<u8, f32>(&mapped)
                        .map(|values| values.to_vec())
                        .map_err(|error| anyhow!("invalid GPU distance readback: {error}"))
                });
            buffers.staging.unmap();
            let distances = distances?;
            ensure!(
                distances
                    .iter()
                    .all(|distance| distance.is_finite() && *distance >= 0.0),
                "GPU distance matrix contains non-finite or negative distances"
            );
            Ok(distances)
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
        let distance_bytes = capacity as u64 * capacity as u64 * 4;
        let distances = make_buffer(
            "Basis distance results",
            distance_bytes,
            wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        );
        let staging = make_buffer(
            "Basis distance readback",
            distance_bytes,
            wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        );
        let parameters = make_buffer(
            "Basis distance parameters",
            16,
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
                    resource: distances.as_entire_binding(),
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
            distances,
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

fn dispatch_dimensions(pair_count: u32, maximum_dimension: u32) -> Result<(u32, u32)> {
    ensure!(
        maximum_dimension > 0,
        "GPU does not support compute dispatch"
    );
    let groups = pair_count.div_ceil(WORKGROUP_SIZE);
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

    #[test]
    fn dispatch_spans_two_dimensions_after_device_limit() {
        assert_eq!(dispatch_dimensions(4_000_000, 65_535).unwrap(), (62_500, 1));
        assert_eq!(dispatch_dimensions(9_000_000, 65_535).unwrap(), (65_535, 3));
        assert!(dispatch_dimensions(1_000_000, 2).is_err());
    }

    /// Run explicitly on hardware: cargo test -p basis-server-core
    /// hardware_gpu_distance_parity -- --ignored --nocapture
    #[test]
    #[ignore = "requires a hardware Vulkan, DX12, or Metal GPU"]
    fn hardware_gpu_distance_parity() {
        let mut gpu = GpuDistanceBackend::new("").expect("hardware GPU required");
        eprintln!("Hardware distance test: {}", gpu.adapter_name());
        for (bucket, count) in [(0, 4), (1, 2_000), (0, 65), (1, 3)] {
            let positions: Vec<_> = (0..count)
                .map(|index| [index as f32, (index % 7) as f32, 2.0, 0.0])
                .collect();
            let distances = gpu.compute(bucket, &positions).unwrap();
            assert_eq!(distances.len(), count * count);
            for (receiver, sender) in [(0, 0), (0, count - 1), (count - 1, 0), (1, count / 2)] {
                let a = positions[receiver];
                let b = positions[sender];
                let dx = a[0] - b[0];
                let dy = a[1] - b[1];
                let dz = a[2] - b[2];
                assert_eq!(
                    distances[receiver * count + sender],
                    dx * dx + dy * dy + dz * dz
                );
            }
        }
        // Check adjacent representable positions at the default quality
        // boundaries, plus three-axis arithmetic that can differ by a few ULPs
        // between GPU drivers and the CPU. The controller guards such boundary
        // ambiguity before selecting quality/interval bytes.
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
        let distances = gpu.compute(0, &positions).unwrap();
        for (sender, position) in positions.iter().enumerate() {
            let expected =
                position[0] * position[0] + position[1] * position[1] + position[2] * position[2];
            let actual = distances[sender];
            assert!((actual - expected).abs() <= expected.max(1.0) * f32::EPSILON * 8.0);
            if position[1] == 0.0 && position[2] == 0.0 {
                for threshold in [10.0_f32, 20.0, 40.0] {
                    assert_eq!(actual.sqrt() <= threshold, expected.sqrt() <= threshold);
                }
            }
        }
        assert!(gpu.compute(2, &[]).is_err());
        assert!(gpu.compute(0, &[[f32::NAN, 0.0, 0.0, 0.0]]).is_err());
        assert_eq!(gpu.compute(0, &[]).unwrap(), Vec::<f32>::new());
        assert_eq!(gpu.compute(1, &[[1.0, 2.0, 3.0, 0.0]]).unwrap(), [0.0]);
    }
}
