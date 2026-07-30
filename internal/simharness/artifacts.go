package simharness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/anishathalye/porcupine"

	"miniquorum/checker"
	"miniquorum/sim"
)

const (
	ScheduleFile      = "schedule.json"
	HistoryFile       = "history.json"
	SummaryFile       = "summary.json"
	TraceFile         = "trace.txt"
	ViolationFile     = "violation.txt"
	VisualizationFile = "porcupine.html"
)

type historyArtifact struct {
	ArtifactVersion   int             `json:"artifact_version"`
	Seed              int64           `json:"seed"`
	Engine            string          `json:"engine"`
	LSMFlushThreshold int64           `json:"lsm_flush_threshold"`
	History           checker.History `json:"history"`
}

// ArtifactBytes produces every replay artifact in memory. Identical results
// produce identical bytes, including Porcupine's self-contained HTML.
func ArtifactBytes(result Result) (map[string][]byte, error) {
	engine := result.Engine
	if engine == "" {
		engine = "map"
	}
	summaryValue := result.Summary
	summaryValue.Engine = engine
	summaryValue.LSMFlushThreshold = result.LSMFlushThreshold
	schedule, err := sim.EncodeFaultSchedule(result.Schedule)
	if err != nil {
		return nil, err
	}
	schedule = append(schedule, '\n')
	history, err := marshalIndented(historyArtifact{
		ArtifactVersion:   ArtifactVersion,
		Seed:              result.Seed,
		Engine:            engine,
		LSMFlushThreshold: result.LSMFlushThreshold,
		History:           result.History,
	})
	if err != nil {
		return nil, err
	}
	summary, err := marshalIndented(summaryValue)
	if err != nil {
		return nil, err
	}
	trace := []byte("")
	if len(result.Trace) > 0 {
		for _, line := range result.Trace {
			trace = append(trace, line...)
			trace = append(trace, '\n')
		}
	}
	violation := []byte(fmt.Sprintf(
		"seed=%d\nengine=%s\nlsm_flush_threshold=%d\nreproduce=go run ./cmd/simreplay -engine %s -lsm-flush-threshold %d -seed %d -schedule %s\nresult=%s\nlogical_operations=%d\ncompleted_operations=%d\nopen_operations=%d\nschedule=%s\nhistory=%s\nvisualization=%s\n",
		result.Seed, engine, result.LSMFlushThreshold, engine, result.LSMFlushThreshold, result.Seed, ScheduleFile, checkResultName(summaryValue), summaryValue.LogicalOperations,
		summaryValue.CompletedOperations, summaryValue.OpenOperations, ScheduleFile, HistoryFile, VisualizationFile,
	))
	checkResult, info := porcupine.CheckEventsVerbose(checker.KVModel, result.History.PorcupineEvents(), 0*time.Second)
	if summaryValue.CheckerRan && (checkResult == porcupine.Ok) != summaryValue.Linearizable {
		return nil, fmt.Errorf("verbose Porcupine result %s disagrees with summary linearizable=%t", checkResult, summaryValue.Linearizable)
	}
	var visualization bytes.Buffer
	if err := porcupine.Visualize(checker.KVModel, info, &visualization); err != nil {
		return nil, fmt.Errorf("render Porcupine visualization: %w", err)
	}
	return map[string][]byte{
		ScheduleFile:      schedule,
		HistoryFile:       history,
		SummaryFile:       summary,
		TraceFile:         trace,
		ViolationFile:     violation,
		VisualizationFile: visualization.Bytes(),
	}, nil
}

// WriteArtifacts writes (or overwrites) the deterministic artifact files in
// directory. The writes are plain per-file writes, not an atomic directory
// swap: a process killed mid-write can leave a partial artifact set, which a
// rerun of the same deterministic replay fully rewrites. Callers should use a
// disposable directory for repeated proof runs.
func WriteArtifacts(directory string, result Result) error {
	artifacts, err := ArtifactBytes(result)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	for _, name := range []string{ScheduleFile, HistoryFile, SummaryFile, TraceFile, ViolationFile, VisualizationFile} {
		if err := os.WriteFile(filepath.Join(directory, name), artifacts[name], 0o644); err != nil {
			return err
		}
	}
	return nil
}

func marshalIndented(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func checkResultName(summary Summary) string {
	// A run that aborted before Porcupine ran (for example on a simulator
	// invariant) has no checker verdict to report.
	if !summary.CheckerRan {
		return "unchecked"
	}
	if summary.Linearizable {
		return string(porcupine.Ok)
	}
	return string(porcupine.Illegal)
}
