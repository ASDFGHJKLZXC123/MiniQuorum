# MiniQuorum contributor guide

- Go module: `miniquorum`. Phase 0’s complete build contract is
  [`phases/phase-0-scaffold.md`](phases/phase-0-scaffold.md).
- `internal/raft` is a synchronous, deterministic event-driven state machine.
  It never reads wall time or performs I/O, networking, randomness directly,
  goroutines, locks, or gRPC work. The host supplies ticks, messages, and a
  `Rand`; simulation is strictly single-threaded, with no free-running
  goroutines.
- Process each `Ready` exactly in this order: `Storage.Save`, send messages,
  apply committed entries in order, then `Node.Advance`. A `Storage.Save` error
  is fail-stop: do not send, apply, or Advance that batch. A state-machine
  `Apply` error stops the node and prevents `Advance`.
- Main commands: `make proto`, `make test`, `make sim SEED=<seed>`, and
  `make corpus`.

## Worker ground rules

1. The entire spec is `phases/phase-N-*.md` plus this file; missing information
   is an escalation, not permission to assume.
2. Interfaces, protos, and locked decisions are frozen. Escalate contract
   changes; only explicitly authorized additive phase extensions are allowed.
3. Never weaken, delete, skip tests, or mark phase checklist boxes.
4. Finish each packet with named local gates, `go test ./... -race`, the boundary
   gate, and a short report with anything that smells.
5. After two failed attempts at one gate, stop and escalate with evidence.
