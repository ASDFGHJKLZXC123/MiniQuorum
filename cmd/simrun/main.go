// Command simrun executes a bounded parallel batch of independent Phase 4
// seeds. Every individual simulator remains single-threaded.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"miniquorum/internal/simharness"
)

func main() {
	start := flag.Int64("start", 1, "first seed (inclusive)")
	count := flag.Int("count", 1000, "number of consecutive seeds")
	workers := flag.Int("workers", 0, "bounded parallel workers (default: GOMAXPROCS)")
	engine := flag.String("engine", "map", "state-machine engine: map or lsm")
	lsmFlushThreshold := flag.Int64("lsm-flush-threshold", 0, "LSM flush threshold (0 uses the default; lsm engine only)")
	failureOut := flag.String("failure-out", "sim-artifacts/batch-failure", "artifact directory for the first failing seed")
	flag.Parse()
	if *count <= 0 {
		fmt.Fprintln(os.Stderr, "simrun: -count must be positive")
		os.Exit(2)
	}
	if *engine != "map" && *engine != "lsm" {
		fmt.Fprintf(os.Stderr, "simrun: invalid -engine %q (want map or lsm)\n", *engine)
		os.Exit(2)
	}
	if *lsmFlushThreshold < 0 {
		fmt.Fprintln(os.Stderr, "simrun: -lsm-flush-threshold must not be negative")
		os.Exit(2)
	}
	if *engine == "map" && *lsmFlushThreshold != 0 {
		fmt.Fprintln(os.Stderr, "simrun: -lsm-flush-threshold requires -engine lsm")
		os.Exit(2)
	}

	started := time.Now()
	aggregate := simharness.RunBatch(simharness.BatchConfig{
		StartSeed: *start,
		Count:     *count,
		Workers:   *workers,
		Run:       simharness.RunConfig{Engine: *engine, LSMFlushThreshold: *lsmFlushThreshold},
	})
	elapsed := time.Since(started)
	data, err := json.MarshalIndent(aggregate, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "simrun: encode aggregate: %v\n", err)
		os.Exit(2)
	}
	fmt.Println(string(data))
	fmt.Printf("elapsed=%s seeds_per_second=%.2f\n", elapsed.Round(time.Millisecond), float64(*count)/elapsed.Seconds())
	if aggregate.Failed == 0 {
		return
	}
	artifactSeed := aggregate.FirstFailureSeed
	if aggregate.FirstViolationSeed != nil {
		artifactSeed = aggregate.FirstViolationSeed
	}
	if artifactSeed != nil {
		result, _ := simharness.RunSeed(simharness.RunConfig{Seed: *artifactSeed, Engine: *engine, LSMFlushThreshold: *lsmFlushThreshold})
		directory := filepath.Join(*failureOut, fmt.Sprintf("seed-%d", *artifactSeed))
		if err := simharness.WriteArtifacts(directory, result); err != nil {
			fmt.Fprintf(os.Stderr, "simrun: write failure artifacts: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "simrun: first failure artifacts=%s\n", directory)
		}
	}
	fmt.Fprintf(os.Stderr, "simrun: %s\n", aggregate.FirstFailure)
	os.Exit(1)
}
