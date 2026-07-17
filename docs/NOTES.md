# MiniQuorum notes

Phase 0 establishes the deterministic Raft boundary and its enforcement test.

## Unbounded client deduplication (Phase 2)

The map state machine retains one cached result and highest sequence number per
client ID indefinitely. Session expiry is deliberately out of scope, so this
deduplication table grows without bound; it is the accepted Phase 2 trade-off
that preserves safe retry semantics across leader changes.

## Disruptive rejoin without PreVote (Phase 1)

Seed 505 elects a leader, then isolates a follower as a one-node minority. The
isolated follower cannot win an election, but its election timeouts advance its
term. On healing, its higher-term RequestVote forces the majority leader to
step down; the cluster then elects one leader at the new highest term. (An
already-elected isolated leader does not time out without CheckQuorum, so it
may retain its role instead.) This is the expected liveness disruption from the
intentionally absent PreVote and CheckQuorum mechanisms (Implementation Guide
decision #13), not a safety violation: `SingleLeaderPerTerm` remains checked
after every simulator event.

## AppendEntries rejection backoff (Phase 2A)

Leader retry uses the paper's linear fallback: each rejected AppendEntries
decrements that follower's `nextIndex` by one and retries the suffix. The
conflict-index optimization was intentionally skipped because rejections carry
no conflict term/index hint; `matchIndex` identifies only the range covered by
a successful request. The implementation keeps one logical request in flight
per follower and deterministically resends that same request on a heartbeat
until a response arrives. This keeps the Phase 2 implementation simple, at the
cost of O(log length) retries for a severely divergent follower.

## Leadership-lost is wire-compatible with NotLeader (Phase 2C)

`ExecuteResponse` (frozen) has no separate field for "your proposal was lost
to a leadership change, retry" versus "I was never the leader". The server
does not need one: when a different entry commits at a waiter's index (see
`internal/server/waiter.go`), it fails that waiter the same way it answers a
plain non-leader request — `NotLeader` with whatever leader hint is currently
known (often the very hint learned from the AppendEntries that caused the
stale commit, so it is frequently accurate rather than empty). `mqctl`
already retries every `NotLeader` response identically (follow the hint, else
round-robin, same client_id/seq), so the two server-side causes collapse into
one client-side retry path with no protocol change.

## Leader hints live on the host, not in `internal/raft` (Phase 2C correction)

An early Phase 2C draft added a `Node.LeaderHint()` accessor plus `leaderHint`
state to the raft core to serve `NotLeader` hints. That broke the frozen
Phase 0 API contract (`internal/raft` exposes exactly the synchronous shape in
`phases/phase-0-scaffold.md` §4) for a value the core has no intrinsic need
to track. The fix moved the hint entirely into `internal/server.Host`: it
records its own configured ID (`Host.SelfID`) as the hint after a successful
leader `Propose`, and learns a hint by inspecting inbound `AppendEntries`
messages in `Host.Step` before handing them to `Node.Step`. Both sources are
best-effort — an unset (`0`) `LeaderId` never overwrites an existing hint,
and staleness around elections is accepted, matching the original design's
guarantees without adding any state to the consensus core.

## Production election jitter must be crypto-seeded (Phase 2C correction)

`cmd/miniquorumd` originally injected a `Rand` that always returned `0`. Three
independently started real processes with identical, unjittered election
timeouts can keep re-triggering elections in lockstep and never converge on a
stable leader — a liveness bug that sim can't catch (sim's seeded PRNG is
intentionally deterministic per the Implementation Guide's `Rand` decision,
and each simulated node already draws distinct values from it). The daemon
now seeds a `math/rand/v2` PCG source from `crypto/rand` once at startup
(`newProdRand` in `cmd/miniquorumd/main.go`) and fails startup explicitly if
the crypto seed read fails, rather than silently falling back to a fixed or
weak seed.

## Fail-stop must drain every outstanding waiter, not just the failing one (Phase 2C correction)

A fail-stopping proposal used to clean up only its own waiter: on a
multi-node leader, an earlier proposal still awaiting quorum stayed
registered in `KVApplier`'s maps forever once a later Ready's `Storage.Save`
or `Apply` failed — the host never processes another Ready, so nothing could
ever fulfill it, and its `Execute` stayed blocked for as long as its context
lived (forever, with `context.Background`). The fix is a single server-side
notification contract: `Host` invokes `FailStopNotifier.FailStop(err)` on its
`Applier` exactly once, under `Host.mu`, at the stopped transition — the one
point both `Save` and `Apply` errors funnel through — and
`KVApplier.FailStop` resolves every registered waiter and empties both maps.
The drain is exhaustive and final because waiters register only inside
`Host.Propose`'s `onProposed` callback under `Host.mu`, and a stopped host
short-circuits before `onProposed` can ever run again.

Outcome choice (two wire-compatible options existed): drained waiters
surface `codes.Unavailable "raft host stopped"` — the same shape the
concurrently-failing `Execute` already returns — rather than a `NotLeader`
response. A dead node is not "not the leader", and its hint may still name
itself; an RPC error makes `mqctl` round-robin to another peer immediately,
reusing the same `client_id`/`seq`, which dedup makes safe.

## Disklog suffix truncation is logical, via TruncateRecord (Phase 3A)

Of the two truncation options the phase spec authorizes,
`internal/storage/disklog` implements the primary one: truncation as a log
record, not a file rewrite. When `Save` receives entries whose first index
is ≤ `LastIndex()`, it appends a `TruncateRecord{from_index}` frame ahead of
the `EntriesRecord` in the same synced batch; recovery replays records in
order and drops the mirrored suffix at `from_index` before appending the
replacements. (Replay also treats an `EntriesRecord` overlapping existing
indexes as an implicit truncate at its first index — the same rule
`MemStorage.Save` applies — so both encodings of an overwrite agree.)

This is the smaller option under segmented files: overwritten suffixes can
span segment boundaries, so a physical truncate would need file deletions
plus a tail rewrite plus directory syncs, while the logical record keeps
segments strictly append-only and keeps one `Save` = one write + one fsync
on one file. The single physical rewrite in the package is recovery's
torn-tail handling, which truncates the final segment at the first bad tail
record so discarded bytes cannot resurface as mid-log corruption after
appends resume.

## Disklog recovery must re-establish durability, not assume it (Phase 3A)

What the paper doesn't tell you: bytes that read back after a restart are
not evidence they are on stable media. A process that wrote a batch and then
failed its fsync fail-stops — but its in-memory poison dies with it, and the
complete frames it wrote may sit in the page cache, readable by the next
process yet gone after a power cut. The same holds for a segment name whose
creation-time directory fsync failed. Meanwhile recovered state drives
responses without any further `Save`: a follower re-answers a repeated
`RequestVote` from its recovered `VotedFor` with no new `HardState`, and
`Save(nil, nil)` persists nothing — so nothing downstream will sync on the
recovered state's behalf. `Open` therefore syncs every segment file and then
the directory before it returns, and fails if it cannot; normal Saves keep
one Ready = one Save = one batch = one sync.

Related recovery subtlety: a record's own length header cannot be trusted to
decide torn-tail versus mid-log corruption. An upward-corrupted middle
length claims the rest of the file and masquerades as a torn tail (silently
discarding intact synced records after it); a downward-corrupted final
length leaves trailing payload bytes that look like data "after" a bad
record. Recovery classifies by evidence instead: a failed record in the
final segment is a torn tail only if no complete, CRC-valid record starts at
any byte offset after its header — otherwise the damage is mid-log and
startup refuses with `ErrCorrupt`.
