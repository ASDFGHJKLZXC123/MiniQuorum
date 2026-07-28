// Command schedgen (re)writes the committed generated-schedule half of the
// Phase 4 regression corpus: corpus/generated/seed-<N>.json, one serialized
// versioned fault schedule per seed, produced by the current generator over
// the pinned harness cluster and fault horizon. Scripted scenario schedules
// and the pinned negative-control schedule are hand-maintained and are never
// touched by this command.
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
	out := flag.String("out", "corpus", "corpus root directory")
	start := flag.Int64("start", 1, "first seed (inclusive)")
	count := flag.Int("count", 100, "number of consecutive seeds")
	horizon := flag.Int64("horizon", int64(simharness.DefaultFaultHorizon), "fault-generation horizon in virtual milliseconds")
	flag.Parse()
	if *count <= 0 {
		fmt.Fprintln(os.Stderr, "schedgen: -count must be positive")
		os.Exit(2)
	}

	directory := filepath.Join(*out, simharness.CorpusGeneratedDir)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "schedgen: %v\n", err)
		os.Exit(2)
	}
	for i := 0; i < *count; i++ {
		seed := *start + int64(i)
		schedule, err := sim.GenerateFaultSchedule(seed, simharness.DefaultNodeIDs(), sim.VirtualTime(*horizon))
		if err != nil {
			fmt.Fprintf(os.Stderr, "schedgen: seed %d: %v\n", seed, err)
			os.Exit(2)
		}
		encoded, err := sim.EncodeFaultSchedule(schedule)
		if err != nil {
			fmt.Fprintf(os.Stderr, "schedgen: seed %d: %v\n", seed, err)
			os.Exit(2)
		}
		encoded = append(encoded, '\n')
		path := filepath.Join(directory, fmt.Sprintf("seed-%04d.json", seed))
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "schedgen: %v\n", err)
			os.Exit(2)
		}
	}
	fmt.Printf("schedgen: wrote %d schedules (seeds %d..%d, generator v%d) under %s\n",
		*count, *start, *start+int64(*count)-1, sim.FaultScheduleGeneratorVersion, directory)
}
