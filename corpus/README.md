# Phase 4 regression corpus

This directory is the committed, versioned schedule corpus required by the
Phase 4 exit gate: schedule **files** tagged with the generator version, never
bare seeds. Phase 5 and later must run these files unchanged.

A schedule file alone does not identify a replay — the seed also drives
message delays, election jitter, and workload draws. Every corpus entry is
therefore a `(seed, schedule)` pair:

- `generated/seed-<N>.json` — schedules produced by
  `sim.GenerateFaultSchedule` for seed `N` over the pinned harness cluster
  (five nodes, 1..5 — five because the section 5.4.2 negative control needs
  the Figure 8 shape, which cannot occur with three servers) and fault
  horizon (6000 virtual ms). The filename carries the seed; the file's
  `Version` field carries the generator version, which must match
  `manifest.json`. As of generator v5 a generated *before-sync* storage crash
  varies its retained unsynced prefix across none (`RetainUnsynced` 0), a
  positive partial/torn prefix, and all (-1), so the campaign combines Phase 3's
  before-sync loss and torn-write survivor states with the network/pause/skew
  faults; post-sync crashes keep -1, where the drained buffer makes retention a
  no-op. Regenerate only on a deliberate generator bump:

      go run ./cmd/schedgen -out corpus -start 1 -count 100

- `scripted/*.json` — hand-written scenario schedules (`Version` 0), listed
  with their seeds in `manifest.json`. These are the reusable shapes later
  phases replay against new engines: `crash-at-point-x` (one storage crash at
  each schedulable crash point: 1 = before-sync with a torn/empty retained
  prefix, 2 = after-sync-before-send, 3 = after-send), `partition-during-write`
  (an asymmetric edge partition and a symmetric group split while client
  writes are in flight), and `pause-leader` (each of the five nodes paused in
  turn for longer than a heartbeat interval so the current leader is paused
  at some point regardless of which node won the election).

- `pinned/buggy-5.4.2.json` — the negative-control schedule for the section
  5.4.2 commit-by-count bug, replayed with seed 7 (`manifest.json`'s `pinned`
  entry). The ordinary build must pass it; `-tags buggy` must make **Porcupine
  itself** report a violation on it. It is a hand-shaped scripted schedule
  (`Version` 0) because the recorded generated/high-churn sweeps did not reach
  the Figure 8 overwrite — see `docs/NOTES.md` for the construction, the
  violating operations, and the replay hashes. `LoadCorpus` therefore accepts a
  pinned schedule at version 0 or the current generator version, and never a
  stale one.

`FaultEvent.Kind` integers: 1 drop-rate, 2 duplicate-rate, 3 partition,
4 heal, 5 partition-groups, 6 heal-groups, 7 pause, 8 resume, 9 clock-skew,
10 crash, 11 restart. `CrashDirective.Point`: 1 before-sync,
2 after-sync-before-send, 3 after-send; `RetainUnsynced` -1 keeps every
unsynced byte, 0 loses the batch, other values keep that byte prefix.

Gates over this corpus:

    make corpus            # ordinary build replays every entry green
    make negative-control  # -tags buggy fails Porcupine on the pinned entry

Replay any entry directly:

    go run ./cmd/simreplay -seed <seed> -schedule corpus/<file>
