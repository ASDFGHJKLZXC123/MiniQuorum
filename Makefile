.PHONY: proto proto-check test boundary sim sim-500 sim-crash-500 corpus lint real-smoke real-crash

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/raft.proto proto/kv.proto

proto-check:
	@tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	cp proto/*.pb.go "$$tmp"/; \
	$(MAKE) proto; \
	for generated in proto/*.pb.go; do \
		cmp -s "$$generated" "$$tmp/$$(basename "$$generated")" || { echo "generated protobuf drift: $$generated"; exit 1; }; \
	done

test:
	go test ./... -race

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

corpus:
	go test ./checker

lint:
	golangci-lint run

real-smoke:
	go test -race -tags=integration ./cmd/miniquorumd -run '^TestRealThreeProcessMQCTLRoundTripAndLeaderFailover$$' -count=1 -v

# real-crash is the Phase 3 real-process spot check: SIGKILL the leader in
# the middle of an mqctl put loop, restart it from the same --data-dir, and
# verify every acknowledged write survives.
real-crash:
	go test -race -tags=integration ./cmd/miniquorumd -run '^TestRealLeaderSIGKILLDuringMQCTLLoopLosesNoAckedWrite$$' -count=1 -v
