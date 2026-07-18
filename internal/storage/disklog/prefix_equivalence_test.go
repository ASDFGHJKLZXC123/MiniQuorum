package disklog

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
	"miniquorum/sim"
)

func TestDisklogSimPrefixEquivalenceHardStateTruncateEntries(t *testing.T) {
	baseHard := raft.HardState{Term: 1, VotedFor: 1}
	baseEntries := makeEntries(1, 1, "alpha", "bravo", "charlie")
	nextHard := raft.HardState{Term: 2, VotedFor: 3}
	overwrite := makeEntries(2, 2, "prime")

	sourceDir := t.TempDir()
	disk := mustOpen(t, sourceDir, Options{})
	mustSave(t, disk, &baseHard, baseEntries)
	mustSave(t, disk, &nextHard, overwrite)
	if err := disk.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	segmentPath := filepath.Join(sourceDir, segmentName(1))
	segment, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", segmentPath, err)
	}
	frames := scanFrames(t, segmentPath)
	wantKinds := []string{"hard", "entries", "hard", "trunc", "entries"}
	if got := frameKinds(t, segmentPath); !reflect.DeepEqual(got, wantKinds) {
		t.Fatalf("disklog record order = %v, want %v", got, wantKinds)
	}

	// Measure the sim codec's corresponding logical record boundaries only
	// through CrashStorage's public behavior. Physical byte counts differ by
	// design; the survivor states at complete record boundaries must not.
	baseHardLen := simBatchBytes(t, &baseHard, nil)
	nextHardLen := simBatchBytes(t, &nextHard, nil)
	entriesLen := simBatchBytes(t, nil, overwrite)
	probe := mustSimStorage(t, sim.FaultSchedule{Crashes: []sim.CrashDirective{{
		Node: 1, Save: 2, Point: sim.CrashBeforeSync, RetainUnsynced: sim.RetainAllUnsynced,
	}}})
	mustSimSave(t, probe, &baseHard, baseEntries)
	if err := probe.Save(&nextHard, overwrite); !errors.Is(err, sim.ErrCrashed) {
		t.Fatalf("probe Save() error = %v, want ErrCrashed", err)
	}
	info, ok := probe.LastCrash()
	if !ok {
		t.Fatal("probe LastCrash() reported no crash")
	}
	truncateLen := info.UnsyncedBytes - nextHardLen - entriesLen
	if truncateLen <= 0 {
		t.Fatalf("sim overlapping batch = %d bytes, want a truncate record beyond hard=%d and entries=%d", info.UnsyncedBytes, nextHardLen, entriesLen)
	}

	// Every complete-record survivor boundary of the two-batch history, from
	// the empty log through the fully durable second batch. crashSave selects
	// which Save the sim crash cuts; the retention length walks that batch's
	// staged records one whole record at a time.
	boundaries := []struct {
		name      string
		diskCut   int
		crashSave uint64
		simRetain int
	}{
		{"empty log", 0, 1, 0},
		{"first hard state", frames[0].offset + frames[0].length, 1, baseHardLen},
		{"first batch complete", frames[2].offset, 2, 0},
		{"second hard state", frames[2].offset + frames[2].length, 2, nextHardLen},
		{"hard state plus truncate", frames[3].offset + frames[3].length, 2, nextHardLen + truncateLen},
		{"complete batch", frames[4].offset + frames[4].length, 2, nextHardLen + truncateLen + entriesLen},
	}
	for _, boundary := range boundaries {
		t.Run(boundary.name, func(t *testing.T) {
			dir := t.TempDir()
			writeSegmentBytes(t, dir, segment[:boundary.diskCut])
			recoveredDisk := mustOpen(t, dir, Options{})
			defer func() { _ = recoveredDisk.Close() }()

			recoveredSim := mustSimStorage(t, sim.FaultSchedule{Crashes: []sim.CrashDirective{{
				Node: 1, Save: boundary.crashSave, Point: sim.CrashBeforeSync, RetainUnsynced: boundary.simRetain,
			}}})
			if boundary.crashSave == 1 {
				if err := recoveredSim.Save(&baseHard, baseEntries); !errors.Is(err, sim.ErrCrashed) {
					t.Fatalf("sim Save() error = %v, want ErrCrashed", err)
				}
			} else {
				mustSimSave(t, recoveredSim, &baseHard, baseEntries)
				if err := recoveredSim.Save(&nextHard, overwrite); !errors.Is(err, sim.ErrCrashed) {
					t.Fatalf("sim Save() error = %v, want ErrCrashed", err)
				}
			}
			if err := recoveredSim.Recover(); err != nil {
				t.Fatalf("sim Recover() error = %v", err)
			}

			assertStorageEquivalent(t, recoveredDisk, recoveredSim)
		})
	}
}

func mustSimStorage(t *testing.T, schedule sim.FaultSchedule) *sim.CrashStorage {
	t.Helper()
	store, err := sim.NewCrashStorage(1, schedule)
	if err != nil {
		t.Fatalf("NewCrashStorage() error = %v", err)
	}
	return store
}

func mustSimSave(t *testing.T, store *sim.CrashStorage, hard *raft.HardState, entries []raftpb.Entry) {
	t.Helper()
	if err := store.Save(hard, entries); err != nil {
		t.Fatalf("CrashStorage.Save() error = %v", err)
	}
}

func simBatchBytes(t *testing.T, hard *raft.HardState, entries []raftpb.Entry) int {
	t.Helper()
	store := mustSimStorage(t, sim.FaultSchedule{})
	mustSimSave(t, store, hard, entries)
	return len(store.DurableBytes())
}

func assertStorageEquivalent(t *testing.T, left, right storage.Storage) {
	t.Helper()
	leftHard, leftErr := left.HardState()
	rightHard, rightErr := right.HardState()
	if leftErr != nil || rightErr != nil || leftHard != rightHard {
		t.Fatalf("HardState mismatch: disk=(%+v,%v) sim=(%+v,%v)", leftHard, leftErr, rightHard, rightErr)
	}
	if left.FirstIndex() != right.FirstIndex() || left.LastIndex() != right.LastIndex() {
		t.Fatalf("index bounds mismatch: disk=[%d,%d] sim=[%d,%d]", left.FirstIndex(), left.LastIndex(), right.FirstIndex(), right.LastIndex())
	}
	if left.LastIndex() < left.FirstIndex() {
		return
	}
	leftEntries, leftErr := left.Entries(left.FirstIndex(), left.LastIndex()+1)
	rightEntries, rightErr := right.Entries(right.FirstIndex(), right.LastIndex()+1)
	if leftErr != nil || rightErr != nil || len(leftEntries) != len(rightEntries) {
		t.Fatalf("Entries mismatch: disk=(%+v,%v) sim=(%+v,%v)", leftEntries, leftErr, rightEntries, rightErr)
	}
	for i := range leftEntries {
		if !proto.Equal(&leftEntries[i], &rightEntries[i]) {
			t.Fatalf("entry %d mismatch: disk=%+v sim=%+v", i, &leftEntries[i], &rightEntries[i])
		}
	}
}
