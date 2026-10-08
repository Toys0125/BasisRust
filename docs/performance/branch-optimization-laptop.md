# Laptop branch-optimization review — 2026-10-07

Lenovo Legion 7 16ACHg6, AMD Ryzen 9 5900HX (8 cores/16 threads), native Ubuntu 24.04.5 LTS,
Linux 7.0.0-31-generic, Rust 1.99.0 GNU/LLVM 23.1.1, Python 3.12.3, GCC 13.3.0.
AC power was online before/after the series. Platform profile, CPU governor and energy
preference were already `performance`; they were not changed. CPU 2 was available and
pinned for every process (SMT sibling CPU 3). Initial load average was low; other user
processes were not stopped. Clock frequency and temperature were not continuously logged.
Runs began on October 8 UTC (October 7 local time).

## Original baseline versus table/cold-extraction candidate

Baseline `1e45b3b` versus candidate `3606a9b`: four sequential ABBA/BAAB blocks, eight
independent processes per variant, one warmup plus nine measured passes per phase.
All 864 grouped branch/branch-miss samples had 100% scheduling coverage; all printed
workload outcomes matched. User-mode counters exclude kernel/hypervisor execution.

| Phase | Branches/op baseline → candidate | Misses/op baseline → candidate | Miss rate baseline → candidate | Median ns/op baseline → candidate | Timing change |
|---|---:|---:|---:|---:|---:|
| `from_byte` | 2.990 → 2.010 | 0.010506 → 0.010406 | 0.35138% → 0.51771% | 1.245 → 1.260 | +1.20% |
| `parse_message_packet` | 2.000 → 2.000 | 0.010721 → 0.010489 | 0.53602% → 0.52443% | 3.055 → 2.930 | -4.09% |
| `build_outbound_packet` | 84.251 → 84.251 | 0.000467 → 0.000403 | 0.00055% → 0.00048% | 38.080 → 37.880 | -0.53% |
| `build_merged_datagrams` | 5165.996 → 5165.996 | 15.325278 → 15.312062 | 0.29666% → 0.29640% | 1452.950 → 1441.740 | -0.77% |
| `process_ack` | 2449.085 → 2448.095 | 65.983803 → 65.999167 | 2.69422% → 2.69594% | 22698.690 → 22489.290 | -0.92% |
| `process_ack_noop` | 604.591 → 604.591 | 1.000905 → 1.000870 | 0.16555% → 0.16555% | 262.905 → 258.825 | -1.55% |

The decoder table was slower in every balanced block (+1.20%, +1.20%, +1.61%, +0.80%);
its aggregate misses/op fell about 0.95%. The desktop had shown a much larger timing
penalty and more misses/op, so that miss behavior did not replicate on this CPU.
Parse timing improved in all four blocks; other timing and counter changes were mixed.
No significance test or general application speedup is established. The unchanged merge
function’s timing does not establish an instruction-cache mechanism. ACK/refill latency
was roughly 22.5–22.7 microseconds here, substantially above the desktop observation;
the reason for this cross-host difference was not isolated.

The Rust 1.99.0 optimized decoder probes again show a compare/conditional move for the
original match and a table load for the candidate, with no baseline indirect jump.
The probes do not represent every production call site.

Full run timings, per-pass counters, workload totals, hashes, environment and assembly:
[original laptop evidence](results/branch-review-laptop-20261007-summary.json).

```sh
RUSTUP_TOOLCHAIN=1.99.0 python3 scripts/perf/review-branches.py \
  --output captures/laptop-branches --baseline 1e45b3b --candidate 3606a9b \
  --blocks 4 --cpu 2 --pmc
```

## Decoder-only alternative

Table candidate `3606a9b` versus match-restoration candidate `83a96e9`: two sequential
ABBA/BAAB blocks, four independent processes per variant. The only executable source
change restores `PacketProperty::from_byte`; the fresh-ID comment is also corrected.
The same frozen benchmark files were used. The table binary exactly matches its hash
from the original series. All 432 samples had 100% coverage and matched outcomes.

| Phase | Branches/op table → match | Misses/op table → match | Miss rate table → match | Median ns/op table → match | Timing change |
|---|---:|---:|---:|---:|---:|
| `from_byte` | 2.010 → 2.990 | 0.010409 → 0.010903 | 0.51785% → 0.36462% | 1.255 → 1.260 | +0.40% |
| `parse_message_packet` | 2.000 → 2.000 | 0.010482 → 0.010650 | 0.52410% → 0.53247% | 2.930 → 3.065 | +4.61% |
| `build_outbound_packet` | 84.251 → 84.251 | 0.000974 → 0.000477 | 0.00116% → 0.00057% | 38.150 → 39.135 | +2.58% |
| `build_merged_datagrams` | 5165.996 → 5165.996 | 15.736028 → 15.266347 | 0.30461% → 0.29552% | 1511.165 → 1507.325 | -0.25% |
| `process_ack` | 2448.095 → 2448.095 | 66.355812 → 66.079611 | 2.71051% → 2.69923% | 22373.350 → 22466.575 | +0.42% |
| `process_ack_noop` | 604.591 → 604.591 | 1.000863 → 1.000906 | 0.16554% → 0.16555% | 259.180 → 261.595 | +0.93% |

Restoring the match gave no local decoder timing improvement: median timing rose 0.40%
and misses/op rose about 4.75%. Parse timing rose 4.61%, with +3.34% and +5.17% changes
in the two balanced blocks. This smaller sample does not establish significance or an
application impact. Its timings should not be pooled with the earlier four-block study.

The match-restoration candidate is rejected. The original table and cold extractions are
retained; their general benefit remains unproven. The rejected revision stays in Git
history so its evidence remains reproducible. Final changes correct the table rationale,
fresh-ID frequency, benchmark comment, report claims and runner failure diagnostic.

[Decoder-alternative evidence](results/branch-review-laptop-decoder-20261007-summary.json)
contains every run, per-pass sample, total, hash, outcome and probe.

```sh
RUSTUP_TOOLCHAIN=1.99.0 python3 scripts/perf/review-branches.py \
  --output captures/laptop-decoder-revert --baseline 3606a9b --candidate 83a96e9 \
  --blocks 2 --cpu 2 --pmc
```

Both studies used the frozen harness from `6b8e769`; the final harness edit corrects only
the outbound coverage comment. Its executable benchmark logic is unchanged.

## Scope and verification

These are synthetic fixed-seed inputs, not production traffic. Outbound omits
ReliableUnordered. Merge uses 32-packet batches; unlike phase rates must not be combined.
Warmup and fixture construction are excluded; allocation/refill and small clock/control
overhead inside phases are included. Workload totals/checksums are not hardware counters.
Socket backpressure, event enqueue, receive dispatch, shedding, ID allocation, application
latency/throughput and individual cold-helper attribution were not measured. No desktop
benchmarks were resumed and no Windows result is inferred from the laptop.

The original, match-restoration and final retained revisions each passed 54 ordinary
transport tests (one benchmark ignored) and workspace formatting with Rust 1.99.0. The runner’s missing
and successful executable-lookup cases were checked, and both complete series exercised
its build/parse/coverage validation. CodeRabbit's initial two minor issues (report claims
and missing-executable diagnostic) were corrected with no findings on that follow-up.
The complete retained-diff review found one additional minor wording issue about when
fresh IDs are used; the report now explicitly describes reuse before fresh allocation.
