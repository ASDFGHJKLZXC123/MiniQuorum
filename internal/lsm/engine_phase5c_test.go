package lsm

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"

	raftpb "miniquorum/proto"
)

func TestEngineFlushThresholdForceEmptyAndWatermarkSeams(t *testing.T) {
	const dir = "/db"
	fs := NewSimFS()
	engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(401), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}

	fs.ResetEvents()
	if err := engine.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if events := fs.Events(); len(events) != 0 {
		t.Fatalf("empty ForceFlush performed filesystem operations: %+v", events)
	}

	if err := engine.Put([]byte("key"), []byte("value"), 5); err != nil {
		t.Fatal(err)
	}
	if engine.FlushedIndex() != 0 || len(engine.ReferencedSSTables()) != 0 {
		t.Fatal("write below threshold flushed unexpectedly")
	}
	if err := engine.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if engine.FlushedIndex() != 5 || len(engine.ReferencedSSTables()) != 1 || engine.PendingImmutables() != 0 {
		t.Fatalf("forced state = watermark:%d files:%v immutables:%d", engine.FlushedIndex(), engine.ReferencedSSTables(), engine.PendingImmutables())
	}
	if value, found, err := engine.Read([]byte("key")); err != nil || !found || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("Read(key) = %q,%v,%v", value, found, err)
	}

	if err := engine.AdvanceAppliedIndex(7); err != nil {
		t.Fatal(err)
	}
	fs.ResetEvents()
	if err := engine.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if engine.FlushedIndex() != 7 || len(engine.ReferencedSSTables()) != 1 {
		t.Fatalf("watermark-only flush = watermark:%d files:%v", engine.FlushedIndex(), engine.ReferencedSSTables())
	}
	for _, event := range fs.Events() {
		if event.Op == FSOpCreate && bytes.HasSuffix([]byte(event.Path), []byte(sstableFilenameSuffix)) {
			t.Fatalf("watermark-only flush created an SSTable: %+v", event)
		}
	}
	manifestBytes, err := fs.DurableBytes(filepath.Join(dir, manifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	state, _, torn, err := replayManifestBytes(manifestBytes)
	if err != nil || torn || state.records != 2 || state.flushedIndex != 7 {
		t.Fatalf("manifest after watermark-only edit = records:%d watermark:%d torn:%v err:%v", state.records, state.flushedIndex, torn, err)
	}

	if err := engine.AdvanceAppliedIndex(6); err == nil {
		t.Fatal("AdvanceAppliedIndex accepted a regression")
	}
	if err := engine.FlushThrough(6); err == nil {
		t.Fatal("FlushThrough accepted a flushed-index regression")
	}
	if err := engine.FlushThrough(8); err == nil {
		t.Fatal("FlushThrough accepted an unapplied future index")
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Crash(0); err != nil {
		t.Fatal(err)
	}
	if err := fs.Recover(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Options{FS: fs, Rand: newSeededRand(402)})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.FlushedIndex() != 7 {
		t.Fatalf("reopened FlushedIndex() = %d, want 7", reopened.FlushedIndex())
	}
	if reopened.AppliedIndex() != 0 {
		t.Fatalf("reopened AppliedIndex() = %d, want 0: FlushedIndex is not a recovery start point", reopened.AppliedIndex())
	}
	if value, found, err := reopened.Read([]byte("key")); err != nil || !found || string(value) != "value" {
		t.Fatalf("reopened Read(key) = %q,%v,%v", value, found, err)
	}

	thresholdFS := NewSimFS()
	key, value := []byte("k"), []byte("v")
	threshold := memtableEntrySize(key, value, false)
	thresholdEngine, err := Open("/threshold", Options{FS: thresholdFS, Rand: newSeededRand(403), FlushThreshold: threshold})
	if err != nil {
		t.Fatal(err)
	}
	if err := thresholdEngine.Put(key, value, 1); err != nil {
		t.Fatal(err)
	}
	if thresholdEngine.FlushedIndex() != 1 || len(thresholdEngine.ReferencedSSTables()) != 1 {
		t.Fatalf("exact threshold did not trigger: watermark=%d files=%v", thresholdEngine.FlushedIndex(), thresholdEngine.ReferencedSSTables())
	}
}

func TestEngineRealFSFlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir, Options{FS: RealFS{}, Rand: newSeededRand(450)})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("key"), []byte("value"), 6); err != nil {
		t.Fatal(err)
	}
	if err := engine.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Options{FS: RealFS{}, Rand: newSeededRand(451)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	if reopened.FlushedIndex() != 6 {
		t.Fatalf("reopened FlushedIndex() = %d, want 6", reopened.FlushedIndex())
	}
	if value, found, err := reopened.Read([]byte("key")); err != nil || !found || string(value) != "value" {
		t.Fatalf("reopened Read(key) = %q,%v,%v", value, found, err)
	}
}

func TestEngineReadHighestSequenceAcrossActiveImmutablesAndSSTables(t *testing.T) {
	t.Run("all immutable memtables and active", func(t *testing.T) {
		fs := NewSimFS()
		engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(501), FlushThreshold: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		boom := errors.New("create failure")
		if err := engine.Put([]byte("shared"), []byte("seq10"), 10); err != nil {
			t.Fatal(err)
		}
		if err := fs.SetErrorSchedule([]SimErrorDirective{{Op: FSOpCreate, Occurrence: 1, Point: SimBeforeOperation, Err: boom}}); err != nil {
			t.Fatal(err)
		}
		if err := engine.ForceFlush(); !errors.Is(err, boom) {
			t.Fatalf("first ForceFlush() error = %v", err)
		}
		if err := engine.Put([]byte("shared"), []byte("seq20"), 20); err != nil {
			t.Fatal(err)
		}
		if err := fs.SetErrorSchedule([]SimErrorDirective{{Op: FSOpCreate, Occurrence: 1, Point: SimBeforeOperation, Err: boom}}); err != nil {
			t.Fatal(err)
		}
		if err := engine.ForceFlush(); !errors.Is(err, boom) {
			t.Fatalf("second ForceFlush() error = %v", err)
		}
		if engine.PendingImmutables() != 2 {
			t.Fatalf("PendingImmutables() = %d, want 2", engine.PendingImmutables())
		}
		if value, tombstone, seq, found, err := engine.Lookup([]byte("shared")); err != nil || !found || tombstone || seq != 20 || string(value) != "seq20" {
			t.Fatalf("immutable Lookup(shared) = %q,%v,%d,%v,%v", value, tombstone, seq, found, err)
		}
		if err := engine.Put([]byte("shared"), []byte("seq30"), 30); err != nil {
			t.Fatal(err)
		}
		if value, _, seq, found, err := engine.Lookup([]byte("shared")); err != nil || !found || seq != 30 || string(value) != "seq30" {
			t.Fatalf("active Lookup(shared) = %q seq=%d found=%v err=%v", value, seq, found, err)
		}
	})

	t.Run("multiple SSTables creation order tombstones and boundaries", func(t *testing.T) {
		fs := NewSimFS()
		engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(502), FlushThreshold: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		flushPut := func(key, value []byte, seq uint64) {
			t.Helper()
			if err := engine.Put(key, value, seq); err != nil {
				t.Fatal(err)
			}
			if err := engine.ForceFlush(); err != nil {
				t.Fatal(err)
			}
		}
		flushDelete := func(key []byte, seq uint64) {
			t.Helper()
			if err := engine.Delete(key, seq); err != nil {
				t.Fatal(err)
			}
			if err := engine.ForceFlush(); err != nil {
				t.Fatal(err)
			}
		}

		flushPut([]byte("creation-order"), []byte("seq50"), 50)
		flushPut([]byte("creation-order"), []byte("seq20-newer-file"), 20)
		if value, _, seq, found, err := engine.Lookup([]byte("creation-order")); err != nil || !found || seq != 50 || string(value) != "seq50" {
			t.Fatalf("creation-order Lookup = %q seq=%d found=%v err=%v", value, seq, found, err)
		}

		flushPut([]byte("newer-delete"), []byte("live"), 10)
		flushDelete([]byte("newer-delete"), 30)
		if value, found, err := engine.Read([]byte("newer-delete")); err != nil || found || value != nil {
			t.Fatalf("Read(newer-delete) = %q,%v,%v, want not found", value, found, err)
		}
		if _, tombstone, seq, found, err := engine.Lookup([]byte("newer-delete")); err != nil || !found || !tombstone || seq != 30 {
			t.Fatalf("Lookup(newer-delete) = tombstone:%v seq:%d found:%v err:%v", tombstone, seq, found, err)
		}

		flushDelete([]byte("older-delete"), 11)
		flushPut([]byte("older-delete"), []byte("resurrected-by-newer-put"), 31)
		if value, found, err := engine.Read([]byte("older-delete")); err != nil || !found || string(value) != "resurrected-by-newer-put" {
			t.Fatalf("Read(older-delete) = %q,%v,%v", value, found, err)
		}

		large := bytes.Repeat([]byte("L"), 1<<20)
		flushPut(nil, large, 60)
		if value, found, err := engine.Read(nil); err != nil || !found || !bytes.Equal(value, large) {
			t.Fatalf("Read(empty key large value) = len:%d found:%v err:%v", len(value), found, err)
		}

		if err := engine.Put([]byte("creation-order"), []byte("seq70-active"), 70); err != nil {
			t.Fatal(err)
		}
		if value, _, seq, found, err := engine.Lookup([]byte("creation-order")); err != nil || !found || seq != 70 || string(value) != "seq70-active" {
			t.Fatalf("active-over-table Lookup = %q seq=%d found=%v err=%v", value, seq, found, err)
		}
	})
}

func TestEngineLowerReplayedSequenceNeverOverridesDurableVersion(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(601)})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("key"), []byte("durable-high"), 100); err != nil {
		t.Fatal(err)
	}
	if err := engine.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("key"), []byte("replayed-low"), 20); err != nil {
		t.Fatal(err)
	}
	if value, _, seq, found, err := engine.Lookup([]byte("key")); err != nil || !found || seq != 100 || string(value) != "durable-high" {
		t.Fatalf("Lookup after lower replay = %q seq=%d found=%v err=%v", value, seq, found, err)
	}
	if err := engine.Delete([]byte("key"), 30); err != nil {
		t.Fatal(err)
	}
	if value, tombstone, seq, found, err := engine.Lookup([]byte("key")); err != nil || !found || tombstone || seq != 100 || string(value) != "durable-high" {
		t.Fatalf("Lookup after lower replayed tombstone = %q tomb:%v seq=%d found=%v err=%v", value, tombstone, seq, found, err)
	}

	replayFS := NewSimFS()
	beforeCrash, err := Open("/replay", Options{FS: replayFS, Rand: newSeededRand(602)})
	if err != nil {
		t.Fatal(err)
	}
	if err := beforeCrash.Put([]byte("key"), []byte("durable-high"), 100); err != nil {
		t.Fatal(err)
	}
	if err := beforeCrash.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if err := beforeCrash.Close(); err != nil {
		t.Fatal(err)
	}
	replaying, err := Open("/replay", Options{FS: replayFS, Rand: newSeededRand(603), FlushThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	if replaying.AppliedIndex() != 0 || replaying.FlushedIndex() != 100 {
		t.Fatalf("reopen indices = applied:%d flushed:%d, want 0/100", replaying.AppliedIndex(), replaying.FlushedIndex())
	}
	if err := replaying.Put([]byte("key"), []byte("replayed-low"), 20); err != nil {
		t.Fatalf("size-triggered lower replay Put() error = %v", err)
	}
	if value, _, seq, found, err := replaying.Lookup([]byte("key")); err != nil || !found || seq != 100 || string(value) != "durable-high" {
		t.Fatalf("reopened lower replay Lookup = %q seq=%d found=%v err=%v", value, seq, found, err)
	}
}

func TestEngineFlushFailurePointsRetainReadableStateAndHonestWatermark(t *testing.T) {
	boom := errors.New("injected failure")
	tests := []struct {
		name      string
		directive SimErrorDirective
		committed bool
	}{
		{name: "create before", directive: SimErrorDirective{Op: FSOpCreate, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "create after", directive: SimErrorDirective{Op: FSOpCreate, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "sst write before", directive: SimErrorDirective{Op: FSOpWrite, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "sst write after", directive: SimErrorDirective{Op: FSOpWrite, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "sst sync before", directive: SimErrorDirective{Op: FSOpFileSync, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "sst close before", directive: SimErrorDirective{Op: FSOpClose, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "directory sync before", directive: SimErrorDirective{Op: FSOpDirSync, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "table stat before", directive: SimErrorDirective{Op: FSOpStat, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "table open before", directive: SimErrorDirective{Op: FSOpOpen, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "table read before", directive: SimErrorDirective{Op: FSOpRead, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest open before", directive: SimErrorDirective{Op: FSOpOpenAppend, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest write before", directive: SimErrorDirective{Op: FSOpWrite, Occurrence: 2, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest sync before", directive: SimErrorDirective{Op: FSOpFileSync, Occurrence: 2, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest sync after uncertain", directive: SimErrorDirective{Op: FSOpFileSync, Occurrence: 2, Point: SimAfterOperation, Err: boom}},
		{name: "manifest close after durable", directive: SimErrorDirective{Op: FSOpClose, Occurrence: 2, Point: SimBeforeOperation, Err: boom}, committed: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := NewSimFS()
			engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(701), FlushThreshold: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Put([]byte("key"), []byte("value"), 9); err != nil {
				t.Fatal(err)
			}
			if err := fs.SetErrorSchedule([]SimErrorDirective{test.directive}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			err = engine.ForceFlush()
			if err == nil {
				t.Fatal("ForceFlush() error = nil, want injected failure")
			}
			if value, _, seq, found, readErr := engine.Lookup([]byte("key")); readErr != nil || !found || seq != 9 || string(value) != "value" {
				t.Fatalf("state unreadable after failure: value=%q seq=%d found=%v err=%v", value, seq, found, readErr)
			}
			if test.committed {
				if engine.FlushedIndex() != 9 || engine.PendingImmutables() != 0 || len(engine.ReferencedSSTables()) != 1 {
					t.Fatalf("durable-close failure state = watermark:%d imm:%d files:%v", engine.FlushedIndex(), engine.PendingImmutables(), engine.ReferencedSSTables())
				}
			} else if engine.FlushedIndex() != 0 || engine.PendingImmutables() != 1 {
				t.Fatalf("pre/uncertain-commit failure advanced state: watermark=%d imm=%d", engine.FlushedIndex(), engine.PendingImmutables())
			}
		})
	}
}

func TestEngineRejectsFlushTargetBelowActiveSequence(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(801)})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("key"), []byte("value"), 10); err != nil {
		t.Fatal(err)
	}
	if err := engine.FlushThrough(9); err == nil {
		t.Fatal("FlushThrough accepted target below active max sequence")
	}
	if engine.PendingImmutables() != 0 || len(engine.Entries()) != 1 || engine.FlushedIndex() != 0 {
		t.Fatal("rejected target mutated lifecycle state")
	}
}

func TestEngineSizeTriggeredFlushFailureReturnsErrorAndKeepsWriteReadable(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(850), FlushThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("threshold flush failure")
	if err := fs.SetErrorSchedule([]SimErrorDirective{{
		Op: FSOpFileSync, Occurrence: 1, Point: SimBeforeOperation, Err: boom,
	}}); err != nil {
		t.Fatal(err)
	}
	err = engine.Put([]byte("key"), []byte("value"), 12)
	if !errors.Is(err, boom) {
		t.Fatalf("threshold-triggering Put() error = %v, want injected failure", err)
	}
	if engine.FlushedIndex() != 0 || engine.PendingImmutables() != 1 {
		t.Fatalf("failed threshold flush state = watermark:%d immutables:%d", engine.FlushedIndex(), engine.PendingImmutables())
	}
	if value, found, err := engine.Read([]byte("key")); err != nil || !found || string(value) != "value" {
		t.Fatalf("Read(key) after failed threshold flush = %q,%v,%v", value, found, err)
	}
}

func TestEngineManifestCrashRetentionNoPartialAndAllUnsyncedBytes(t *testing.T) {
	tests := []struct {
		name           string
		retain         int
		point          SimFaultPoint
		wantWatermark  uint64
		wantReferences int
		wantFound      bool
	}{
		{name: "none", retain: 0, point: SimBeforeOperation},
		{name: "partial", retain: 9, point: SimBeforeOperation},
		{name: "all", retain: RetainAllUnsynced, point: SimBeforeOperation, wantWatermark: 15, wantReferences: 1, wantFound: true},
		{name: "after sync before publish", retain: 0, point: SimAfterOperation, wantWatermark: 15, wantReferences: 1, wantFound: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const dir = "/db"
			fs := NewSimFS()
			engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(855)})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Put([]byte("key"), []byte("value"), 15); err != nil {
				t.Fatal(err)
			}
			if err := fs.SetCrashSchedule([]SimCrashDirective{{
				Op: FSOpFileSync, Occurrence: 2, Point: test.point, RetainUnsynced: test.retain,
			}}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if err := engine.ForceFlush(); !errors.Is(err, ErrSimulatedCrash) {
				t.Fatalf("ForceFlush() error = %v, want ErrSimulatedCrash", err)
			}
			if engine.FlushedIndex() != 0 {
				t.Fatalf("live FlushedIndex() = %d before sync, want 0", engine.FlushedIndex())
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir, Options{FS: fs, Rand: newSeededRand(856)})
			if err != nil {
				t.Fatal(err)
			}
			if reopened.FlushedIndex() != test.wantWatermark || len(reopened.ReferencedSSTables()) != test.wantReferences {
				t.Fatalf("reopened state = watermark:%d files:%v", reopened.FlushedIndex(), reopened.ReferencedSSTables())
			}
			value, found, err := reopened.Read([]byte("key"))
			if err != nil || found != test.wantFound || test.wantFound && string(value) != "value" {
				t.Fatalf("reopened Read(key) = %q,%v,%v", value, found, err)
			}
		})
	}
}

func TestEngineRecoveryDurabilityAndOrphanCleanupFailurePoints(t *testing.T) {
	boom := errors.New("recovery failure")
	recoveryTests := []SimErrorDirective{
		{Op: FSOpFileSync, Occurrence: 1, Point: SimBeforeOperation, Err: boom},
		{Op: FSOpDirSync, Occurrence: 1, Point: SimBeforeOperation, Err: boom},
		{Op: FSOpFileSync, Occurrence: 2, Point: SimBeforeOperation, Err: boom},
	}
	for _, directive := range recoveryTests {
		t.Run(string(directive.Op)+"-occurrence", func(t *testing.T) {
			const dir = "/db"
			fs := NewSimFS()
			engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(851)})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Put([]byte("key"), []byte("value"), 1); err != nil {
				t.Fatal(err)
			}
			if err := engine.ForceFlush(); err != nil {
				t.Fatal(err)
			}
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fs.SetErrorSchedule([]SimErrorDirective{directive}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if _, err := Open(dir, Options{FS: fs, Rand: newSeededRand(852)}); err == nil {
				t.Fatal("Open() succeeded through injected recovery durability failure")
			}
		})
	}

	for _, test := range []struct {
		name      string
		directive SimErrorDirective
	}{
		{name: "orphan remove", directive: SimErrorDirective{Op: FSOpRemove, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "orphan directory sync", directive: SimErrorDirective{Op: FSOpDirSync, Occurrence: 2, Point: SimBeforeOperation, Err: boom}},
	} {
		t.Run(test.name, func(t *testing.T) {
			const dir = "/db"
			orphanPath := filepath.Join(dir, "orphan.sst")
			fs := NewSimFS()
			engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(853)})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
			orphan, err := fs.Create(orphanPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := orphan.Write([]byte("orphan")); err != nil {
				t.Fatal(err)
			}
			if err := orphan.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := orphan.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fs.SyncDir(dir); err != nil {
				t.Fatal(err)
			}
			if err := fs.SetErrorSchedule([]SimErrorDirective{test.directive}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if _, err := Open(dir, Options{FS: fs, Rand: newSeededRand(854)}); err == nil {
				t.Fatal("Open() succeeded through injected orphan-cleanup failure")
			}
			if err := fs.Crash(0); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			if _, err := fs.LiveBytes(orphanPath); err != nil {
				t.Fatalf("failed/unsynced orphan cleanup survived as deletion: %v", err)
			}
		})
	}
}

func TestEngineReadLockCoversSSTableBlockReadAndBloomGates(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(901)})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("present"), []byte("value"), 1); err != nil {
		t.Fatal(err)
	}
	if err := engine.ForceFlush(); err != nil {
		t.Fatal(err)
	}

	counter := &countingReaderAt{base: engine.tables[0].reader.r}
	engine.tables[0].reader.r = counter
	if value, found, err := engine.Read([]byte("definitely-absent")); err != nil || found || value != nil {
		t.Fatalf("absent Read() = %q,%v,%v", value, found, err)
	}
	if counter.reads != 0 {
		t.Fatalf("Bloom-negative lookup performed %d block reads", counter.reads)
	}

	blocker := &blockingReaderAt{
		base:    counter.base,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	engine.tables[0].reader.r = blocker
	result := make(chan error, 1)
	go func() {
		value, found, err := engine.Read([]byte("present"))
		if err == nil && (!found || string(value) != "value") {
			err = errors.New("unexpected read result")
		}
		result <- err
	}()
	<-blocker.entered
	if engine.mu.TryLock() {
		engine.mu.Unlock()
		t.Fatal("engine write lock acquired while SSTable ReadAt was blocked; Read released its RLock too early")
	}
	close(blocker.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestEngineReopenRejectsReferencedTableCorruptionAndFutureSequence(t *testing.T) {
	t.Run("corrupted referenced bytes", func(t *testing.T) {
		const dir = "/db"
		fs := NewSimFS()
		engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1001)})
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.Put([]byte("key"), []byte("value"), 4); err != nil {
			t.Fatal(err)
		}
		if err := engine.ForceFlush(); err != nil {
			t.Fatal(err)
		}
		filename := engine.ReferencedSSTables()[0]
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		appendAndSyncTestFile(t, fs, filepath.Join(dir, filename), []byte("junk"))
		if _, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1002)}); !errors.Is(err, ErrManifestCorrupt) {
			t.Fatalf("Open() error = %v, want ErrManifestCorrupt", err)
		}
	})

	t.Run("table seq exceeds manifest watermark", func(t *testing.T) {
		const dir = "/db"
		fs := NewSimFS()
		engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1003)})
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		filename := "future.sst"
		path := filepath.Join(dir, filename)
		file, err := fs.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writer := NewSSTableWriter(file)
		if err := writer.Add(TableEntry{Key: []byte("k"), Seq: 10, Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
		if err := writer.Finish(); err != nil {
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if err := fs.SyncDir(dir); err != nil {
			t.Fatal(err)
		}
		frame := mustManifestFrame(t, &raftpb.VersionEdit{
			AddedFiles:   []*raftpb.AddedFile{{File: filename, MinKey: []byte("k"), MaxKey: []byte("k"), Count: 1}},
			FlushedIndex: 5,
		})
		appendAndSyncTestFile(t, fs, filepath.Join(dir, manifestFilename), frame)
		if _, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1004)}); !errors.Is(err, ErrManifestCorrupt) {
			t.Fatalf("Open() error = %v, want ErrManifestCorrupt", err)
		}
	})
}

func TestEngineReopenRejectsSameKeyAcrossSSTableDataBlocks(t *testing.T) {
	// This table is deliberately assembled below the writer: the production
	// writer keeps a same-key run in one block. Every block, the footer, index,
	// and Bloom filter remain structurally well-formed and CRC-valid.
	raw := buildRawSSTableBlocks(t, [][]TableEntry{
		{
			{Key: []byte("a"), Seq: 1, Value: []byte("a1")},
			{Key: []byte("z"), Seq: 10, Value: []byte("z10")},
		},
		{
			{Key: []byte("z"), Seq: 5, Value: []byte("z5")},
		},
	})
	reader, err := OpenSSTable(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("OpenSSTable(raw malformed table) error = %v; want footer/index/CRCs to remain valid", err)
	}
	entry, found, err := reader.Get([]byte("z"))
	if err != nil || !found || entry.Seq != 5 {
		t.Fatalf("unvalidated Get(z) = %+v,%v,%v, want stale block-selected z@5", entry, found, err)
	}
	if _, err := reader.AllEntries(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("AllEntries() error = %v, want ErrCorrupt for a same-key cross-block run", err)
	}

	const dir = "/db"
	const filename = "malformed.sst"
	fs := NewSimFS()
	engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1101)})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := fs.Create(filepath.Join(dir, filename))
	if err != nil {
		t.Fatal(err)
	}
	if written, err := file.Write(raw); err != nil || written != len(raw) {
		t.Fatalf("Write(raw table) = %d,%v", written, err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	appendAndSyncTestFile(t, fs, filepath.Join(dir, manifestFilename), mustManifestFrame(t, &raftpb.VersionEdit{
		AddedFiles: []*raftpb.AddedFile{{
			File: filename, MinKey: []byte("a"), MaxKey: []byte("z"), Count: 3,
		}},
		FlushedIndex: 10,
	}))
	if _, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1102)}); !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("Open() error = %v, want ErrManifestCorrupt", err)
	}
}

func TestEngineReopenRejectsEqualSequenceConflictWithinSSTableDataBlock(t *testing.T) {
	key := []byte("key")
	// Bypass SSTableWriter so the test can assemble a single malformed data
	// block while retaining valid data/index/Bloom/footer CRCs and metadata.
	raw := buildRawSSTableBlocks(t, [][]TableEntry{{
		{Key: key, Seq: 7, Value: []byte("first")},
		{Key: key, Seq: 7, Value: []byte("second")},
	}})
	reader, err := OpenSSTable(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("OpenSSTable(raw malformed table) error = %v; want footer, index, Bloom filter, and their CRCs to validate", err)
	}
	if reader.EntryCount() != 2 || len(reader.index) != 1 ||
		!bytes.Equal(reader.index[0].firstKey, key) ||
		!bytes.Equal(reader.minKey, key) || !bytes.Equal(reader.maxKey, key) ||
		!reader.bloom.MayContain(key) {
		t.Fatalf("validated table metadata = count:%d index:%+v min:%q max:%q bloom:%v",
			reader.EntryCount(), reader.index, reader.minKey, reader.maxKey, reader.bloom.MayContain(key))
	}
	if _, err := readBlock(reader.r, reader.index[0].offset, reader.index[0].length); err != nil {
		t.Fatalf("raw data block CRC validation error = %v", err)
	}
	if entry, found, err := reader.Get(key); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(key) = %+v,%v,%v; want ErrCorrupt instead of selecting one equal-sequence value", entry, found, err)
	}
	if entries, err := reader.AllEntries(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("AllEntries() = %+v,%v; want ErrCorrupt for equal sequences within one same-key run", entries, err)
	}

	const dir = "/db"
	const filename = "equal-sequence-conflict.sst"
	fs := NewSimFS()
	engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1151)})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := fs.Create(filepath.Join(dir, filename))
	if err != nil {
		t.Fatal(err)
	}
	if written, err := file.Write(raw); err != nil || written != len(raw) {
		t.Fatalf("Write(raw table) = %d,%v", written, err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	appendAndSyncTestFile(t, fs, filepath.Join(dir, manifestFilename), mustManifestFrame(t, &raftpb.VersionEdit{
		AddedFiles: []*raftpb.AddedFile{{
			File: filename, MinKey: key, MaxKey: key, Count: 2,
		}},
		FlushedIndex: 7,
	}))
	if _, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1152)}); !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("Open() error = %v, want ErrManifestCorrupt for a referenced ambiguous SSTable", err)
	}
}

func TestEngineCloseRetainsUnresolvedHandleOwnershipForRetry(t *testing.T) {
	boom := errors.New("injected close failure")
	for _, point := range []SimFaultPoint{SimBeforeOperation, SimAfterOperation} {
		name := map[SimFaultPoint]string{
			SimBeforeOperation: "before effect retries open handle",
			SimAfterOperation:  "after effect retires already closed handle",
		}[point]
		t.Run(name, func(t *testing.T) {
			fs := NewSimFS()
			engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(1201)})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Put([]byte("key"), []byte("value"), 1); err != nil {
				t.Fatal(err)
			}
			if err := engine.ForceFlush(); err != nil {
				t.Fatal(err)
			}
			if len(engine.tables) != 1 {
				t.Fatalf("open table handles = %d, want 1", len(engine.tables))
			}
			if err := fs.SetErrorSchedule([]SimErrorDirective{{
				Op: FSOpClose, Occurrence: 1, Point: point, Err: boom,
			}}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if err := engine.Close(); !errors.Is(err, boom) {
				t.Fatalf("first Close() error = %v, want injected failure", err)
			}
			if len(engine.tables) != 1 {
				t.Fatalf("first Close() retained handles = %d, want unresolved ownership of 1", len(engine.tables))
			}
			if _, _, err := engine.Read([]byte("key")); err == nil {
				t.Fatal("Read succeeded after Close began")
			}
			if err := engine.Put([]byte("other"), []byte("value"), 2); err == nil {
				t.Fatal("Put succeeded after Close began")
			}
			if err := engine.Close(); err != nil {
				t.Fatalf("retry Close() error = %v", err)
			}
			if len(engine.tables) != 0 {
				t.Fatalf("retry Close() retained handles = %d, want 0", len(engine.tables))
			}
			eventCount := len(fs.Events())
			if err := engine.Close(); err != nil {
				t.Fatalf("idempotent resolved Close() error = %v", err)
			}
			if len(fs.Events()) != eventCount {
				t.Fatal("resolved Close() performed another filesystem operation")
			}
		})
	}
}

func buildRawSSTableBlocks(t *testing.T, blocks [][]TableEntry) []byte {
	t.Helper()
	var table bytes.Buffer
	index := make([]indexEntry, 0, len(blocks))
	filterEntries := 0
	var minKey, maxKey []byte
	for blockIndex, entries := range blocks {
		if len(entries) == 0 {
			t.Fatalf("raw block %d is empty", blockIndex)
		}
		var data []byte
		for _, entry := range entries {
			record, err := encodeRecord(entry)
			if err != nil {
				t.Fatal(err)
			}
			data = append(data, record...)
			if filterEntries == 0 {
				minKey = cloneBytes(entry.Key)
			}
			maxKey = cloneBytes(entry.Key)
			filterEntries++
		}
		offset := uint64(table.Len())
		if err := appendBlock(&table, data); err != nil {
			t.Fatal(err)
		}
		index = append(index, indexEntry{
			firstKey: cloneBytes(entries[0].Key),
			offset:   offset,
			length:   uint64(table.Len()) - offset,
		})
	}

	indexOffset := uint64(table.Len())
	indexData, err := encodeIndex(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendBlock(&table, indexData); err != nil {
		t.Fatal(err)
	}
	indexLength := uint64(table.Len()) - indexOffset

	filter := NewBloomFilter(filterEntries)
	for _, entries := range blocks {
		for _, entry := range entries {
			filter.Add(entry.Key)
		}
	}
	bloomData, err := filter.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	bloomOffset := uint64(table.Len())
	if err := appendBlock(&table, bloomData); err != nil {
		t.Fatal(err)
	}
	bloomLength := uint64(table.Len()) - bloomOffset

	footer, err := encodeFooter(indexOffset, indexLength, bloomOffset, bloomLength, uint64(filterEntries), minKey, maxKey)
	if err != nil {
		t.Fatal(err)
	}
	table.Write(footer)
	return append([]byte(nil), table.Bytes()...)
}

type countingReaderAt struct {
	base  io.ReaderAt
	reads int
}

func (reader *countingReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	reader.reads++
	return reader.base.ReadAt(dst, offset)
}

type blockingReaderAt struct {
	base    io.ReaderAt
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (reader *blockingReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	reader.once.Do(func() {
		close(reader.entered)
		<-reader.release
	})
	return reader.base.ReadAt(dst, offset)
}
