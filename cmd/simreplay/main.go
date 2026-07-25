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
	schedulePath := flag.String("schedule", "", "versioned fault-schedule JSON file (default: generate from seed)")
	out := flag.String("out", "", "artifact directory (default: sim-artifacts/seed-N)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "simreplay: unexpected arguments: %v\n", flag.Args())
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

	result, runErr := simharness.RunSeed(simharness.RunConfig{Seed: *seed, Schedule: schedule})
	directory := *out
	if directory == "" {
		directory = filepath.Join("sim-artifacts", fmt.Sprintf("seed-%d", *seed))
	}
	if err := simharness.WriteArtifacts(directory, result); err != nil {
		fmt.Fprintf(os.Stderr, "simreplay: write artifacts: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("seed=%d checker_ran=%t linearizable=%t logical=%d completed=%d open=%d attempts=%d schedule_version=%d artifacts=%s\n",
		result.Seed, result.Summary.CheckerRan, result.Summary.Linearizable, result.Summary.LogicalOperations,
		result.Summary.CompletedOperations, result.Summary.OpenOperations,
		result.Summary.Attempts, result.Summary.ScheduleVersion, directory)
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "simreplay: %v\n", runErr)
		os.Exit(1)
	}
}
