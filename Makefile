.PHONY: proto test boundary sim sim-500 corpus lint

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/raft.proto proto/kv.proto

test:
	go test ./... -race

boundary:
	go test ./internal/boundary -run '^TestRaftBoundary$$' -count=1

SEED ?= 1

sim:
	go test ./sim -run '^TestSim$$' -seed=$(SEED) -count=1

# sim-500 is the Phase 1 local/CI corpus: 500 independently seeded cold
# starts, each with SingleLeaderPerTerm checked after every virtual event.
sim-500:
	go test ./sim -run '^TestPhase1FiveHundredSeeds$$' -count=1

corpus:
	go test ./checker

lint:
	golangci-lint run
