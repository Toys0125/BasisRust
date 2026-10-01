# Performance artifact availability

The September 30, 2026 reports publish measurements, workload descriptions,
revision references, and limitations. Their `captures/` paths identify ignored
local experiment archives; those directories are not included in this repository
and no public download is currently hosted.

To obtain the original materials, [open an artifact request](https://github.com/Toys0125/BasisRust/issues/new?title=Performance%20artifact%20request)
for benchmark author Marcus. Identify the report, revision, and archive directory
below. Access requires the author to provide a copy; there is no automatic
download. The retained materials include runners and their helper/fixture
dependencies, modified load-client source patches, server variant patches,
configuration and binary/source hashes, manifests, compact result JSON, observer
and health samples, and native profiles. Request these together when attempting
the original experiment, rather than running an isolated script.

| Report | Local archive directory under `captures/` |
| --- | --- |
| [Receiver refresh](receiver-distance-refresh-fix.md) | `receiver-distance-refresh-20260930/` |
| [Receiver parallelism](receiver-build-parallelism-2000.md) | `perf-receiver-parallelism-20260930/` |
| [GPU distance buckets](gpu-distance-buckets-2000.md) | `gpu-two-buckets-20260930/` |
| [GPU gap investigation](gpu-update-gap-investigation.md) | `gpu-distance-investigation-20260930/` |
| [GPU tier and interval decisions](gpu-reduction-decisions-2000.md) | `gpu-reduction-decisions-20260930/` |
| [CPU versus GPU](cpu-vs-gpu-decisions-2000.md) | `cpu-vs-gpu-decisions-2000-20260930/` |
| [Bundle-cache fingerprint](bundle-cache-fingerprint-2000.md) | `bundle-cache-fingerprint-2000-20260930/` |

Commands invoking `captures/` scripts are records of the author's local runs,
not instructions that work in a fresh checkout. The historical 2,000-client
experiments require the retained archive and the specified revisions. Building
the current client and server does not recreate the modified historical load
harness or its inputs. Without an archive, readers can inspect the published
report tables and run repository tests, but cannot independently inspect the
raw evidence or reproduce those measurements from this checkout alone.

Current GPU correctness tests are available from the repository without the
archive. They require a hardware Vulkan, DX12, or Metal GPU:

```sh
cargo test --manifest-path BasisRustServer/Cargo.toml \
  -p basis-server-core --features gpu hardware_gpu_ -- --ignored --nocapture
```

These tests check computation parity; they do not rerun the performance studies.
