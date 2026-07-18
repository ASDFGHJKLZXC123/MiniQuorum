// Package workload implements deterministic, scheduler-driven MiniQuorum
// clients. It contains no goroutines or clocks; its Host supplies virtual time,
// event scheduling, and synchronous Raft submission.
package workload

import "fmt"

const (
	// DefaultClientCount is the Phase 4 logical-client count.
	DefaultClientCount = 5
	// DefaultKeyCount is the deliberately small contended keyspace.
	DefaultKeyCount = 8
	// DefaultOperationsPerClient yields 200 logical operations per default run.
	DefaultOperationsPerClient = 40
	// DefaultOperationTimeout is measured in virtual milliseconds.
	DefaultOperationTimeout int64 = 3000
	// DefaultRetryDelay is the virtual delay after a retryable rejection.
	DefaultRetryDelay int64 = 10
	// DefaultThinkTime separates one client's completed operation from its next
	// invocation, preserving exact real-time order at millisecond resolution.
	DefaultThinkTime int64 = 1
	// DefaultClientIDBase makes the five default stable sessions 1 through 5.
	DefaultClientIDBase uint64 = 1
)

// Config controls one deterministic workload. Zero values select defaults,
// except Seed (where zero is a valid seed) and StartTime (where zero means the
// beginning of the simulation).
type Config struct {
	Seed                int64
	ClientCount         int
	ClientIDBase        uint64
	KeyCount            int
	OperationsPerClient int
	OperationTimeout    int64
	RetryDelay          int64
	ThinkTime           int64
	StartTime           int64
	Targets             []uint64
}

func normalizeConfig(config Config) (Config, error) {
	if config.ClientCount == 0 {
		config.ClientCount = DefaultClientCount
	}
	if config.ClientIDBase == 0 {
		config.ClientIDBase = DefaultClientIDBase
	}
	if config.KeyCount == 0 {
		config.KeyCount = DefaultKeyCount
	}
	if config.OperationsPerClient == 0 {
		config.OperationsPerClient = DefaultOperationsPerClient
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = DefaultOperationTimeout
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = DefaultRetryDelay
	}
	if config.ThinkTime == 0 {
		config.ThinkTime = DefaultThinkTime
	}

	switch {
	case config.ClientCount < 0:
		return Config{}, fmt.Errorf("workload: ClientCount %d must be positive", config.ClientCount)
	case config.KeyCount < 0:
		return Config{}, fmt.Errorf("workload: KeyCount %d must be positive", config.KeyCount)
	case config.OperationsPerClient < 0:
		return Config{}, fmt.Errorf("workload: OperationsPerClient %d must be positive", config.OperationsPerClient)
	case config.OperationTimeout < 0:
		return Config{}, fmt.Errorf("workload: OperationTimeout %d must be positive", config.OperationTimeout)
	case config.RetryDelay < 0:
		return Config{}, fmt.Errorf("workload: RetryDelay %d must be positive", config.RetryDelay)
	case config.ThinkTime < 0:
		return Config{}, fmt.Errorf("workload: ThinkTime %d must be positive", config.ThinkTime)
	case config.StartTime < 0:
		return Config{}, fmt.Errorf("workload: StartTime %d must not be negative", config.StartTime)
	case len(config.Targets) == 0:
		return Config{}, fmt.Errorf("workload: Targets must not be empty")
	}
	if uint64(config.ClientCount-1) > ^uint64(0)-config.ClientIDBase {
		return Config{}, fmt.Errorf("workload: client ID range overflows uint64")
	}
	config.Targets = append([]uint64(nil), config.Targets...)
	return config, nil
}
