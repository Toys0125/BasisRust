# PR #38 player-storage quota accounting

Three paired local runs compare reviewed head `4e1dcb3` with this change. Both binaries use the same benchmark source, Cargo.lock dependencies, debug profile, four Tokio runtime threads, and host. The baseline was built from an archived checkout of `4e1dcb3`; binary SHA-256 hashes are recorded below and with every local sample. No task builds or tests ran during measurement.

Each run starts with a fresh persistent store containing 1,000 or 4,000 populated accounts. Eight concurrent writers perform 512 overwrites of distinct accounts, each with a 1 KiB value. Global quota enforcement stays enabled at 1 GiB and per-player quota at 16 KiB. All 512 values are read back after timing; all runs completed with zero errors. Startup reconciliation and fixture creation are outside the write-latency window.

Values below are medians of the three runs. CPU time is process user + system time and includes setup, startup reconciliation, readback, and teardown; it is not limited to the timed write phase.

| Backend | Accounts | p50 ms before / after | p95 ms before / after | Writes/s before / after | CPU seconds before / after |
|---|---:|---:|---:|---:|---:|
| file | 1,000 | 24.12 / 24.07 | 25.69 / 24.84 | 327.0 / 330.2 | 1.497 / 0.907 |
| file | 4,000 | 65.76 / 24.68 | 68.77 / 25.60 | 121.1 / 322.5 | 4.629 / 1.856 |
| sqlite | 1,000 | 22.14 / 8.22 | 24.11 / 8.78 | 357.9 / 955.5 | 1.681 / 0.907 |
| sqlite | 4,000 | 64.21 / 8.30 | 66.69 / 8.73 | 124.2 / 955.2 | 4.879 / 1.860 |

At 4,000 accounts, the observed p95 reduction is about 2.7× for files and 7.6× for SQLite. File latency at 1,000 accounts is essentially unchanged, while CPU use falls. These are small-sample debug-profile observations, not production capacity estimates. One file/1,000 baseline run had a 79.99 ms p95 outlier; the raw samples retain it.

The implementation removes per-write account-wide scans. File storage reads a constant-size usage ledger under an OS directory lock and uses a durable recovery marker around account and ledger updates. SQL row triggers update an aggregate counter in the same transaction as each data change. Full reconciliation and legacy empty-account cleanup happen at startup, or after an interrupted file update.

Correctness checks separately cover two independent file/SQLite instances competing at a quota boundary, insert/update/delete accounting, removal of empty records, legacy cleanup, and file-ledger crash recovery. The PostgreSQL integration test passed locally against PostgreSQL 16.15 and runs against a PostgreSQL 17 service in the new SQL CI job.

## Reproduce a workload

```sh
cargo run --locked --manifest-path BasisRustServer/Cargo.toml \
  -p basis-server-storage --features sql-storage \
  --example player-storage-quota-benchmark -- file 4000
# Replace file with sqlite; use 1000 or 4000 accounts.
```

For a baseline, archive `4e1dcb3`, copy the same example and its `[[example]]` manifest declaration into that source tree, and build it with the same toolchain/profile. Copy both binaries to separate paths before running tests or rebuilding: Cargo may rebuild examples during `cargo test`. Alternate baseline/candidate runs three times per workload.

[Benchmark source](../../BasisRustServer/crates/basis-server-storage/examples/player-storage-quota-benchmark.rs). The machine export remains local at the ignored path `docs/performance/results/pr38-player-storage-quota-comparison.json`, following the results directory policy.

## Binary provenance and individual runs

- before: `a7ce270bf71125c62be553f3cf35a1ba5d5e37afa760d4ca185d65a4dc3b938c`
- after: `d6ee2a22fbf321f245aff674fbf620fd0473fc7cfd6b482ff4ce1970ab5195a9`

| Backend | Accounts | Repeat | Revision | p50 ms | p95 ms | Writes/s | Process CPU seconds | Errors |
|---|---:|---:|---|---:|---:|---:|---:|---:|
| file | 1000 | 1 | before | 24.192 | 25.690 | 327.0 | 1.500 | 0 |
| file | 1000 | 1 | after | 24.177 | 24.866 | 330.2 | 0.908 | 0 |
| file | 1000 | 2 | before | 24.005 | 79.991 | 252.2 | 1.487 | 0 |
| file | 1000 | 2 | after | 24.075 | 24.842 | 330.2 | 0.907 | 0 |
| file | 1000 | 3 | before | 24.120 | 25.033 | 328.0 | 1.497 | 0 |
| file | 1000 | 3 | after | 24.051 | 24.636 | 332.0 | 0.898 | 0 |
| file | 4000 | 1 | before | 65.764 | 68.034 | 121.1 | 4.616 | 0 |
| file | 4000 | 1 | after | 24.679 | 25.862 | 322.5 | 1.843 | 0 |
| file | 4000 | 2 | before | 66.081 | 69.423 | 120.0 | 4.660 | 0 |
| file | 4000 | 2 | after | 24.933 | 25.597 | 320.4 | 1.864 | 0 |
| file | 4000 | 3 | before | 64.908 | 68.773 | 122.0 | 4.629 | 0 |
| file | 4000 | 3 | after | 24.649 | 25.417 | 323.7 | 1.856 | 0 |
| sqlite | 1000 | 1 | before | 22.533 | 24.250 | 351.6 | 1.681 | 0 |
| sqlite | 1000 | 1 | after | 8.354 | 8.776 | 950.3 | 0.919 | 0 |
| sqlite | 1000 | 2 | before | 22.141 | 23.954 | 358.1 | 1.686 | 0 |
| sqlite | 1000 | 2 | after | 8.159 | 8.680 | 972.3 | 0.896 | 0 |
| sqlite | 1000 | 3 | before | 21.950 | 24.107 | 357.9 | 1.681 | 0 |
| sqlite | 1000 | 3 | after | 8.223 | 9.001 | 955.5 | 0.907 | 0 |
| sqlite | 4000 | 1 | before | 62.940 | 66.692 | 125.5 | 4.837 | 0 |
| sqlite | 4000 | 1 | after | 8.191 | 8.732 | 965.8 | 1.860 | 0 |
| sqlite | 4000 | 2 | before | 64.396 | 67.509 | 123.6 | 4.884 | 0 |
| sqlite | 4000 | 2 | after | 8.305 | 8.720 | 955.2 | 1.838 | 0 |
| sqlite | 4000 | 3 | before | 64.208 | 65.684 | 124.2 | 4.879 | 0 |
| sqlite | 4000 | 3 | after | 8.671 | 9.044 | 931.9 | 1.893 | 0 |
