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
