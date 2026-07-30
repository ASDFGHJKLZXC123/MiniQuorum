// Command simreplay replays one deterministic Phase 4 seed and schedule.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"miniquorum/internal/simharness"
	"miniquorum/sim"
)

func main() {
	seed := flag.Int64("seed", 1, "deterministic simulator/workload seed")
	engine := flag.String("engine", "map", "state-machine engine: map or lsm")
	lsmFlushThreshold := flag.Int64("lsm-flush-threshold", 0, "LSM flush threshold (0 uses the default; lsm engine only)")
	schedulePath := flag.String("schedule", "", "versioned fault-schedule JSON file (default: generate from seed)")
	out := flag.String("out", "", "artifact directory (default: sim-artifacts/seed-N)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "simreplay: unexpected arguments: %v\n", flag.Args())
		os.Exit(2)
	}
	if *engine != "map" && *engine != "lsm" {
		fmt.Fprintf(os.Stderr, "simreplay: invalid -engine %q (want map or lsm)\n", *engine)
		os.Exit(2)
	}
	if *lsmFlushThreshold < 0 {
		fmt.Fprintln(os.Stderr, "simreplay: -lsm-flush-threshold must not be negative")
		os.Exit(2)
	}
	if *engine == "map" && *lsmFlushThreshold != 0 {
		fmt.Fprintln(os.Stderr, "simreplay: -lsm-flush-threshold requires -engine lsm")
		os.Exit(2)
	}

	var schedule *sim.FaultSchedule
	if *schedulePath != "" {
		data, err := os.ReadFile(*schedulePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "simreplay: read schedule: %v\n", err)
			os.Exit(2)
		}
		decoded, err := sim.DecodeFaultSchedule(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "simreplay: decode schedule: %v\n", err)
			os.Exit(2)
		}
		schedule = &decoded
	}

	result, runErr := simharness.RunSeed(simharness.RunConfig{Seed: *seed, Engine: *engine, LSMFlushThreshold: *lsmFlushThreshold, Schedule: schedule})
	directory := *out
	if directory == "" {
		directory = filepath.Join("sim-artifacts", fmt.Sprintf("seed-%d", *seed))
	}
	if err := simharness.WriteArtifacts(directory, result); err != nil {
		fmt.Fprintf(os.Stderr, "simreplay: write artifacts: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("seed=%d engine=%s lsm_flush_threshold=%d checker_ran=%t linearizable=%t logical=%d completed=%d open=%d attempts=%d schedule_version=%d artifacts=%s\n",
		result.Seed, result.Engine, result.LSMFlushThreshold, result.Summary.CheckerRan, result.Summary.Linearizable, result.Summary.LogicalOperations,
		result.Summary.CompletedOperations, result.Summary.OpenOperations,
		result.Summary.Attempts, result.Summary.ScheduleVersion, directory)
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "simreplay: %v\n", runErr)
		os.Exit(1)
	}
}
