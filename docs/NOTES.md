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
torn-tail handling, which truncates the final segment at the start of an
unterminated final frame so discarded bytes cannot resurface after appends
resume. A frame whose terminator was observed is never rewritten away.

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

The physical framing makes that tail rule decidable without believing the
record's length. A frame is `0xFA || E(B) || 0xFB`; `E` represents every body
byte as two bytes from the otherwise-unused `0x40..0x4F` alphabet. At a
record boundary only `0xFA` is legal, and before `0xFB` only alphabet bytes
are legal. Once `0xFB` is observed, parity, decoded length, CRC32C, protobuf,
and record-kind failures are corruption even in the final frame. Only EOF
before `0xFB` in the final record of the final segment is discarded; zero
padding is not a special case.

One information-theoretic limit remains and is accepted by guide decision
#16: a missing final `0xFB`, or a final `0xFB` changed to an alphabet byte,
is indistinguishable from an interrupted append that retained that same
prefix. Recovery discards either shape as an incomplete final frame. The
integrity guarantee is deliberately stated for every single-byte change in
a complete non-final frame, where a following start marker exposes a changed
terminator.


## Section 5.4.2 negative control: the caught schedule (Phase 4C)

The pinned negative control is `(seed 7, corpus/pinned/buggy-5.4.2.json)`.
Under `-tags buggy` it makes **Porcupine itself** report
`linearizable=false`; on the ordinary build the same pair is a green,
non-vacuous, fully checked run.

    # ordinary build: the pin must pass
    go test ./internal/simharness -run '^TestPinnedNegativeControlSchedulePassesOrdinaryBuild$' -count=1
    go run ./cmd/simreplay -seed 7 -schedule corpus/pinned/buggy-5.4.2.json

    # buggy build: the pin must fail the checker
    make negative-control
    go run -tags buggy ./cmd/simreplay -seed 7 -schedule corpus/pinned/buggy-5.4.2.json

### What the schedule does

A recorded sweep of roughly 2,000 generated and high-churn seeds found no
violation; on one high-churn scripted schedule 27 of 200 seeds did reach a
premature prior-term commit, but none of those commits was ever overwritten.
That is finite measured evidence, not a proof that random seeds cannot find it.
There is, though, a structural reason to expect them to miss the *overwrite*:
generated Save-ordinal crashes pair with `RestartAfterCrash: true` and recover
at the same virtual time, so no node can hold a competing log down long enough
to win a later election. The pinned schedule is therefore hand-shaped, stage by
stage, from Raft paper Figure 8, using only serializable fault primitives. With
five nodes and node 4 leading:

1. `t=3000` symmetric split `{4,1} | {2,3,5}`. Node 4 keeps leading a
   minority, so the client entries it accepts in term 1 replicate to node 1
   but can never commit.
2. The majority side elects node 2, which dies on the very Save that persists
   its term-start NOOP (`CrashDirective{Node: 2, Save: 441, Point:
   after-sync-before-send}`, `RestartAfterCrash` left false). Node 2 alone
   holds a *different* entry at index 118 with term 2, and never sent it.
   Nodes 3 and 5 are two of five and cannot elect, so their logs stay put —
   this is the paper's server 5.
3. `t=4320` partial heal that isolates node 5. Node 4 regains leadership in
   term 4 with `lastIndex = 129` still carrying term-1 entries.
4. Node 4 probes at `(129, 1)`. Node 1 matches and acks `matchIndex = 129`;
   node 3 rejects and backfills to 130. Node 1 is then crashed on the exact
   Save that would append node 4's term-4 NOOP (`CrashDirective{Node: 1,
   Save: 497, Point: before-sync, RetainUnsynced: 0}`), which pins
   `matchIndex[1] = 129` and leaves node 1's durable last log term at 1.
5. `t=4860` the buggy rule commits index 129 — a **prior-term** index — on
   replica count alone (index 130 has only two replicas, so the correct rule
   commits nothing here) and applies indices 118..129.
6. `t=5010` the ghost appliers (nodes 4 and 3) are crashed and restarted, and
   nodes 1 and 2 come back. Node 2's competing log wins with nodes 1 and 5,
   overwriting index 118 onward. `t=7510` heals everything.

### The violation

At `t=4860` the premature commit acknowledges two client operations. One of
them is a write:

    client=1 seq=24 PUT k0 = "v1-o"   invoke=2968  return=4860  OK

Node 2's higher-term log then overwrites that log position. A later,
*non-overlapping* read of the same key does not see the acknowledged write, with
no intervening PUT or DELETE:

    client=4 seq=25 GET k0          invoke=5998  return=6069  found=false

This read is invoked at `t=5998`, well after the PUT returned at `t=4860`, so no
linearization may place it before the write. (Client 3 also reads `k0`, at
`invoke=3025` — but that GET *overlaps* the still-open PUT, so a checker may
legally order it before the write; it is not what forces the violation. The
later non-overlapping client-4 read is.) An acknowledged write followed by a
strictly-later read of the same key that does not see it, with nothing in
between that could have removed it, has no valid linearization — Porcupine
returns `Illegal`.

On the ordinary build the same operations resolve after the cluster really
recovers, and the history is linearizable: the `client=1 seq=24` PUT returns at
`5934`, and the paired later read — `client=4 seq=25 GET k0`, `invoke=5993`,
`return=6047` — finds it, `found=true` with value `v1-o`. (The `5943` return
belongs to client 3's *overlapping* `seq=24` GET of `k0`, a different operation
from the paired client-4 read.)

The `-tags buggy` run reaches the checker on a full workload: 200 logical
operations, 200 completed, 5 concurrent clients, 13 faults fired during
in-flight operations. The failure is the checker's verdict, not a setup
error, an invariant abort, or a weak-workload rejection —
`TestPinnedNegativeControlScheduleFailsPorcupineUnderBuggyTag` asserts each of
those separately.

### Replay identity

Two independent `-tags buggy` replays of the pin produce byte-identical
`schedule.json`, `history.json`, `summary.json`, `trace.txt`, `violation.txt`,
and `porcupine.html` (73,077 bytes, renders). The buggy test performs both
replays and compares every artifact; it also asserts the replayed
`schedule.json` is byte-identical to the committed pinned file, so the
artifact and the pin cannot drift.

    sha256(history.json)   962465d0636d2bfebbc9bdbaf8a452011aca5c9a10e3efde77a8c206e04dfe42
    sha256(porcupine.html) e08b9efc20cee3ffc4512c1cfd56ac1d2e0567e7aef535879a9fa4839de68b84
    sha256(schedule.json)  7ae83dac6d1e802a4c12549fa670922d643cab340d10a37c8512b692f4ec5a11
    sha256(trace.txt)      d97b05971f33c92ba8d0d3f0da28a0dff050c129f4f876bdf749e7b02c9b4c34

### What this does and does not show

It shows that this harness, on this workload, catches the section 5.4.2
commit-by-count bug through the linearizability checker, and that the catch is
a committed, deterministically replayable artifact rather than a hope that
random seeds rediscover it. It does **not** show that the harness catches
every violation of the rule, nor that random seeding would have found this
one: the recorded generated/high-churn sweep did not, which is exactly why the
phase file asks for the failing schedule file to be pinned.

### Why the pinned schedule is version 0

`LoadCorpus` previously required the pinned schedule's version to equal the
manifest generator version. The catch needed a hand-authored scripted
schedule, which by the `FaultSchedule` contract carries version 0, so that
check now admits version 0 **or** the current generator version, and still
rejects every stale generator version — the property the check exists to
protect. The pinned entry's seed is also required to be non-zero, since a
schedule file alone does not identify a replay.

### Five nodes, not three

`internal/simharness` pins the harness cluster at five nodes. The overwrite
half of Figure 8 is unobservable at three: any entry on a majority (2 of 3) is
immortal there, because the single node without it can never win an election
against two log-superior peers. Deployment stays at three nodes (guide
decision #11); only the simulator harness runs five.
