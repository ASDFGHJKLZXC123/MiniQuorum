.PHONY: proto proto-check test boundary sim sim-500 sim-crash-500 corpus lint real-smoke real-crash sim-1k sim-10k negative-control bench-smoke bench-collect bench-validate bench-graphs bench-report

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/raft.proto proto/kv.proto proto/lsm.proto

proto-check:
	@tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	cp proto/*.pb.go "$$tmp"/; \
	$(MAKE) proto; \
	for generated in proto/*.pb.go; do \
		cmp -s "$$generated" "$$tmp/$$(basename "$$generated")" || { echo "generated protobuf drift: $$generated"; exit 1; }; \
	done

# internal/simharness replays all 104 committed 12,000-virtual-ms,
# 200-operation fault runs in-process. With race-mode replay admission bounded
# at the measured four-worker throughput optimum, the focused corpus took
# 77m11s and the complete repository race gate took 1h56m23s on the Packet 4C
# correction host. A later uncached repeat under heavy host load exceeded
# three hours without a race or correctness failure, so five hours preserves
# two hours of stressed-run headroom while remaining inside the hosted-CI
# job's six-hour limit; no corpus work is reduced.
test:
	go test ./... -race -timeout 5h

boundary:
	go test ./internal/boundary -count=1

SEED ?= 1

sim:
	go test ./sim -run '^TestSim$$' -seed=$(SEED) -count=1

# sim-500 is the Phase 2 local/CI corpus: 500 independently seeded runs with
# replication, leader isolation/failover, healing, and all Phase 1+2 safety
# invariants checked after every virtual event.
sim-500:
	go test ./sim -run '^TestPhase2FiveHundredSeedsAllInvariants$$' -count=1

# sim-crash-500 is the Phase 3 seeded crash corpus: 500 seeds with storage
# crash faults mixed in (all three schedulable crash points and torn unsynced
# prefixes), Phase 1+2 safety invariants after every event, and post-crash
# recovery plus commit liveness on every seed.
sim-crash-500:
	go test ./sim -run '^TestPhase3FiveHundredSeedsWithCrashFaults$$' -count=1

# corpus replays every committed schedule file — 100 generated, 3 scripted, and
# the pinned negative control — as full 12,000-virtual-ms runs. The replays are
# independent, so the test runs them in parallel, but the whole gate still needs
# well over `go test`'s default 10m package timeout on a smaller machine.
corpus:
	go test ./checker
	go test ./internal/simharness -run '^TestCommittedGeneratedCorpusMatchesGeneratorOutput$$' -count=1
	go test ./internal/simharness -run '^TestCommittedCorpusSchedulesReplayCleanly$$' -count=1 -timeout 40m

# sim-1k / sim-10k are the Phase 4 full-fault Porcupine gates: independent
# seeds, complete 4A fault model, K=5 clients x 40 ops over 8 keys, bounded
# parallel workers, deterministic seed-ordered aggregation.
sim-1k:
	go run ./cmd/simrun -start 1 -count 1000

sim-10k:
	go run ./cmd/simrun -start 1 -count 10000

# negative-control replays the pinned corpus schedule with the section 5.4.2
# commit-by-count bug compiled in and asserts Porcupine itself reports the
# violation. Only this focused test may run under -tags buggy; the rest of
# the suite intentionally fails with the bug present.
negative-control:
	go test -tags buggy ./internal/simharness -run '^TestPinnedNegativeControlScheduleFailsPorcupineUnderBuggyTag$$' -count=1

lint:
	golangci-lint run

real-smoke:
	go test -race -tags=integration ./cmd/miniquorumd -run '^TestRealThreeProcessMQCTLRoundTripAndLeaderFailover$$' -count=1 -v

# real-crash is the Phase 3 real-process spot check: SIGKILL the leader in
# the middle of an mqctl put loop, restart it from the same --data-dir, and
# verify every acknowledged write survives.
real-crash:
	go test -race -tags=integration ./cmd/miniquorumd -run '^TestRealLeaderSIGKILLDuringMQCTLLoopLosesNoAckedWrite$$' -count=1 -v

BENCH_RAW ?= bench/results/report-v1.json
BENCH_GRAPH_DIR ?= docs/benchmarks

bench-smoke:
	@tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	go run ./cmd/lsmbench collect -profile smoke-v1 -out "$$tmp/raw.json"; \
	go run ./cmd/lsmbench validate -in "$$tmp/raw.json"; \
	go run ./cmd/lsmbench graph -in "$$tmp/raw.json" -out-dir "$$tmp/graphs"; \
	for artifact in "$$tmp/raw.json" "$$tmp/graphs/write-throughput.svg" "$$tmp/graphs/point-read-latency.svg" "$$tmp/graphs/bloom-false-positive.svg" "$$tmp/graphs/amplification.svg" "$$tmp/graphs/compaction-pause.svg" "$$tmp/graphs/skiplist-height.svg"; do \
		if [ ! -s "$$artifact" ]; then \
			echo "bench-smoke: missing or empty $$artifact"; \
			exit 1; \
		fi; \
	done; \
	echo "bench-smoke raw: $$tmp/raw.json"; \
	echo "raw.json: $$(sha256sum "$$tmp/raw.json" | awk '{print $$1}')"; \
	for svg in write-throughput.svg point-read-latency.svg bloom-false-positive.svg amplification.svg compaction-pause.svg skiplist-height.svg; do \
		echo "$${svg}: $$(sha256sum "$$tmp/graphs/$$svg" | awk '{print $$1}')"; \
	done

bench-collect:
	go run ./cmd/lsmbench collect -profile report-v1 -out "$(BENCH_RAW)"

bench-validate:
	go run ./cmd/lsmbench validate -in "$(BENCH_RAW)"

bench-graphs:
	go run ./cmd/lsmbench graph -in "$(BENCH_RAW)" -out-dir "$(BENCH_GRAPH_DIR)"

bench-report:
	$(MAKE) bench-collect
	$(MAKE) bench-validate
	$(MAKE) bench-graphs
