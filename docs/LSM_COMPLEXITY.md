# MiniQuorum LSM complexity notes

This document separates asymptotic behavior from the host-specific measurements
in [`LSM_BENCHMARKS.md`](LSM_BENCHMARKS.md). Big-O statements describe the
implemented data structures under their stated assumptions; they are not
predictions of the measured nanosecond values.

## Skip list memtable

Keys are byte-ordered in a probabilistic skip list with maximum tower height
16. A node promotes to the next level with probability `p=1/4`. Search and
insert take expected `O(log n)` time under the usual random-height model. An
ordered iteration visits every stored node and therefore takes `O(n)` time;
the iterator includes tombstones as stored records.

For heights 1 through 15, the exact-height distribution is

`P(H=h) = (3/4) * (1/4)^(h-1)`.

The height-16 bucket is the truncation tail,
`P(H=16) = (1/4)^15`, rather than an unbounded geometric bucket. The expected
tower height is the truncated geometric sum

`E[H] = sum(i=0..15) (1/4)^i = (1-(1/4)^16)/(1-1/4)`,

which is approximately 1.333333333. The report measured a median expected
height of 1.332604 across trials, with trial range 1.331605–1.333274. This
agreement is a distribution check, not a latency guarantee. Expected search
and insert are `O(log n)`; worst-case search and insert can be `O(n)` for an
unfavorable tower realization. Ordered iteration is `O(n)` in both the normal
and worst case.

The engine protects the memtable and SSTable view with one engine-level
`sync.RWMutex`: Apply is the writer and Read/Hash/iteration are readers. That
concurrency mechanism is separate from the skip list's asymptotic height
model.

## Bloom filter

For `n` inserted keys, `m` filter bits, and `k` probes per lookup, building the
filter is `O(k n)` time and storage is `m` bits. A lookup performs `O(k)` hash
probes. The false-positive probability follows from the probability that a
bit remains zero after `kn` placements:

`P(FP) = (1 - exp(-k n / m))^k`.

The probe count minimizing this approximation is

`k_opt = (m/n) ln 2`.

At 10 bits/key, `m/n=10`, so `k_opt = 10 ln 2 ≈ 6.93`, which is implemented
as `k=7`. The report uses each row's actual `m_bits`, `inserted_keys`, and
`k`; the theoretical rate is 0.008193722 for the three configured sizes.
Measured rates were near that value, with a maximum measured/theoretical ratio
of 1.107 across all 15 rows and zero false negatives in every row.

The no-false-negative property depends on correct construction and lookup:
every stored key must set the same probes that lookup checks. A Bloom negative
can reject a key; a Bloom positive only permits the SSTable lookup to
continue. The filter does not by itself prove that a key exists.

## SSTables

SSTable construction receives entries in sorted key order and writes sorted
data blocks, followed by an index block, Bloom block, and footer. A sequential
traversal is `O(N)` for `N` records, with one data block decoded at a time by
the iterator. The index stores each block's first key and byte range; a point
lookup binary-searches the index and then scans one selected data block. With
`B` data blocks, index selection is `O(log B)` plus the selected block scan;
the Bloom check adds `O(k)` probes before that path.

The data portion stores records and per-block CRCs. The index is `O(B)` in
block count, and the Bloom block uses `m` bits plus its serialized metadata.
The footer stores offsets, lengths, count, key bounds, CRC, and the format
magic. A record larger than the 4 KiB data-block target gets an oversized
block. Four KiB is a target, not a maximum. The KV value-size limit remains
1 MiB as specified by the service, so an oversized record is still bounded by
that service rule.

Version resolution is by highest Raft sequence number among candidate records,
not by file recency. A highest-sequence tombstone means not found. This is why
the SSTable read path must consider the relevant candidate files even when a
newly created compacted file may contain an older logical value.

## Size-tiered compaction

Files are assigned to size classes growing by a factor of four. A class is
eligible when it has at least four files; one compaction selects four inputs
from the lowest eligible tier and emits one file in the next tier. The merge
uses a binary min-heap over `k` input iterators, so merging `N` input records
costs `O(N log k)` time and `O(k)` heap space, apart from records and block
buffers. The implementation streams each input one data block at a time and
keeps only the current heap items plus the current block for each iterator.

For each key, the merge emits the highest-sequence record, including a
tombstone. Tombstones are retained in v1. They are not dropped merely because
some tier was compacted: an uncovered older table could otherwise resurrect a
deleted key. Safe tombstone garbage collection would require proving that all
tables that could contain an older version are covered; that is outside this
v1 report.

The current SSTable writer is not fully streaming: it retains the output
entries and an assembled output `bytes.Buffer` until `Finish`, so a compaction
has `O(output)` additional writer buffering, in addition to the `O(k)` merge
heap and per-input block buffers. The merge itself does not materialize every
input table. This memory behavior is an implementation detail that matters for
large outputs and is distinct from the `O(N log k)` merge bound.

The report's write amplification is
`(sstable_write_bytes + manifest_write_bytes) /
logical_user_write_bytes`; its median was 3.972453 with a trial range of
3.970877–3.973391. Read amplification was reported both as SSTable bytes per
read (median 9,187.463) and SSTable calls per read (median 2.290080). Space
amplification, referenced SSTable bytes divided by logical live bytes, had
median 2.546049. These are measured sustained-workload ratios, not the
`O(N log k)` complexity result and not universal constants.

Compaction is executed synchronously in the apply path for this benchmark
profile. The pause report classifies an operation after its PUT by whether the
completed-compaction counter advanced; it does not imply an asynchronous
background execution model. The measured compaction-causing p99 median was
157,897,000 ns, while ordinary p99 median was 1,000 ns, with the spreads shown
in the benchmark report.

Leveled compaction is a comparison point only: it normally trades different
overlap and rewrite behavior for more organized levels. It is not implemented
or measured by this packet.

## Boundaries and non-claims

The analysis covers the implemented skip list, Bloom filter, SSTables, and
size-tiered compaction. It does not claim safe tombstone garbage collection,
a block cache, compression, leveled compaction, a separate WAL, or Phase 6
snapshot behavior. The Raft log remains the only WAL in this phase, and
snapshot creation/restoration remains a Phase 6 concern. All measurements in
the companion report are host-specific observations of the frozen matrix.
