# MiniQuorum notes

Phase 0 establishes the deterministic Raft boundary and its enforcement test.

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
conflict-index optimization was intentionally skipped because the frozen
`AppendEntriesResp` contract carries only `term` and `success`, not a conflict
term/index hint. The implementation keeps one logical request in flight per
follower and deterministically resends that same request on a heartbeat until a
response arrives. This keeps the Phase 2 implementation simple, at the cost of
O(log length) retries for a severely divergent follower.
