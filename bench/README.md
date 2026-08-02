# LSM benchmark harness

`lsmbench` measures the real MiniQuorum LSM through its injected filesystem
boundary. It has three strict subcommands:

```
go run ./cmd/lsmbench collect -profile smoke-v1|report-v1 -out <raw.json>
go run ./cmd/lsmbench validate -in <raw.json>
go run ./cmd/lsmbench graph -in <raw.json> -out-dir <directory>
```

`smoke-v1` is the one-trial local plumbing check. `report-v1` is the frozen
five-trial measurement matrix used by the later reporting packet. Neither
profile scales work for host speed. Collection records every individual trial;
timing varies by host and those raw individual trials are authoritative.

Frozen `report-v1`: trials/seeds `1..5`; 16-byte keys and 128-byte values;
write thresholds 65536/262144/1048576/4194304 with 100000 PUTs each;
point-read table counts 1/4/16/64, Bloom on/off, hit/miss, 4096 entries/table,
and 25000 lookups/cell; Bloom n=1000/10000/100000 with 100000 probes; 250000
amplification operations over 25000 keys (90% PUT/10% DELETE), threshold
65536, then exactly 50000 reads split 50/50 hit/miss; 100000 compaction-pause
PUTs over 100000 keys at threshold 65536; and 1000000 skip-list samples.

Frozen `smoke-v1`: trial/seed 1; write thresholds 65536/4194304 with 2000
PUTs; point-read table counts 1/4, Bloom modes `[true,false]`, lookup kinds
hit/miss, 256 entries/table and 1000 lookups; Bloom n=1000 with 5000 probes;
5000 amplification operations over 500 keys (90% PUT/10% DELETE) at
threshold 16384 then exactly 1000 50/50 reads; 5000 compaction-pause PUTs over
5000 keys at threshold 16384; and 10000 skip-list samples. Both use 16-byte
keys and 128-byte values.

The versioned raw JSON is validated strictly against the selected frozen
matrix. Its SVGs are deterministic transformations of valid raw input and are:

- `write-throughput.svg`
- `point-read-latency.svg`
- `bloom-false-positive.svg`
- `amplification.svg`
- `compaction-pause.svg`
- `skiplist-height.svg`

Metrics use these exact numerators and denominators:

- Write throughput is operations divided by elapsed seconds, including
  synchronous puts, automatic flush/compaction, and the final `ForceFlush`.
- Point-read latency records per-lookup elapsed time and nearest-rank
  p50/p95/p99/max. Its read accounting records actual SSTable read calls and
  bytes returned by the filesystem.
- Bloom false-positive rate is actual Bloom `MayContain` decisions for absent
  probes divided by probe keys. The graph's theory is
  `(1 - exp(-k*n/m))^k` using each row's actual `m_bits`, `inserted_keys`, and
  `k`.
- Write amplification is `(sstable_write_bytes + manifest_write_bytes) /
  logical_user_write_bytes`, where PUT bytes are key plus value and DELETE
  bytes are key only.
- Read amplification is both `sstable_read_bytes / read_operations` and
  `sstable_read_calls / read_operations`.
- Space amplification is `referenced_sstable_bytes / logical_live_bytes`.
  Logical live bytes count final live key/value pairs only; referenced bytes
  include only manifest-referenced `.sst` files.
- Compaction pause reports nearest-rank latency quantiles separately for
  operations that synchronously advanced the real completed-compaction counter
  and those that did not.
- Skip-list height rows record exact counts for heights 1 through 16 from the
  production tower sampler. Measured exact-height probability is
  `height_count / samples`; theory is `(3/4)*(1/4)^(h-1)` for heights 1..15
  and `(1/4)^15` for the truncated height 16.

Make targets:

```
make bench-smoke
make bench-collect BENCH_RAW=bench/results/report-v1.json
make bench-validate BENCH_RAW=bench/results/report-v1.json
make bench-graphs BENCH_RAW=bench/results/report-v1.json BENCH_GRAPH_DIR=docs/benchmarks
make bench-report BENCH_RAW=bench/results/report-v1.json BENCH_GRAPH_DIR=docs/benchmarks
```

`bench-smoke` collects, validates, graphs, checks the six output files, and
prints SHA-256 values. `bench-report` exists for the later report workflow;
this harness does not supply analytical conclusions. Analytical conclusions
belong to Packet 5F2.
