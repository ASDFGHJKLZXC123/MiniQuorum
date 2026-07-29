package lsm

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunFirstFlushWatermarkAndOrphanCleanupCrash is Packet 5C's mandated
// first executable gate. It pins both halves of the durability claim:
//
//   - a successful flush orders SSTable sync, directory sync, one complete
//     manifest frame append, and MANIFEST sync before FlushedIndex advances;
//   - a crash before MANIFEST sync retains a missing or torn edit, while the
//     already file-and-directory-durable SSTable becomes an orphan. Reopen
//     trims the tail, never references the orphan, removes it, and syncs the
//     removal directory.
func TestRunFirstFlushWatermarkAndOrphanCleanupCrash(t *testing.T) {
	t.Run("successful ordering and watermark", func(t *testing.T) {
		const dir = "/db"
		fs := NewSimFS()
		engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(101), FlushThreshold: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.Put([]byte("key"), []byte("value"), 7); err != nil {
			t.Fatal(err)
		}
		fs.ResetEvents()
		if err := engine.ForceFlush(); err != nil {
			t.Fatal(err)
		}
		if got := engine.FlushedIndex(); got != 7 {
			t.Fatalf("FlushedIndex() = %d, want 7 after the complete MANIFEST frame is synced", got)
		}

		events := fs.Events()
		sstableSync := eventOrdinal(events, FSOpFileSync, sstableFilenameSuffix)
		dirSync := eventOrdinal(events, FSOpDirSync, dir)
		manifestWrite := eventOrdinal(events, FSOpWrite, manifestFilename)
		manifestSync := eventOrdinal(events, FSOpFileSync, manifestFilename)
		if sstableSync == 0 || dirSync == 0 || manifestWrite == 0 || manifestSync == 0 {
			t.Fatalf("missing durability evidence: events=%+v", events)
		}
		if sstableSync >= dirSync || dirSync >= manifestWrite || manifestWrite >= manifestSync {
			t.Fatalf("durability order = sst-sync:%d dir-sync:%d manifest-write:%d manifest-sync:%d, want strictly increasing", sstableSync, dirSync, manifestWrite, manifestSync)
		}

		manifestBytes, err := fs.DurableBytes(filepath.Join(dir, manifestFilename))
		if err != nil {
			t.Fatal(err)
		}
		state, complete, torn, err := replayManifestBytes(manifestBytes)
		if err != nil || torn || complete != len(manifestBytes) {
			t.Fatalf("durable manifest replay = complete:%d torn:%v err:%v len:%d", complete, torn, err, len(manifestBytes))
		}
		if state.flushedIndex != 7 || len(state.files) != 1 {
			t.Fatalf("durable manifest state = watermark:%d files:%d, want 7 and 1", state.flushedIndex, len(state.files))
		}
	})

	for _, retain := range []int{0, 7} {
		name := "missing"
		if retain != 0 {
			name = "torn"
		}
		t.Run(name+" manifest edit becomes orphan", func(t *testing.T) {
			const dir = "/db"
			fs := NewSimFS()
			engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(202), FlushThreshold: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Put([]byte("orphan-key"), []byte("orphan-value"), 11); err != nil {
				t.Fatal(err)
			}
			if err := fs.SetCrashSchedule([]SimCrashDirective{{
				Op: FSOpFileSync, Occurrence: 2, Point: SimBeforeOperation, RetainUnsynced: retain,
			}}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			err = engine.ForceFlush()
			if !errors.Is(err, ErrSimulatedCrash) {
				t.Fatalf("ForceFlush() error = %v, want ErrSimulatedCrash", err)
			}
			if got := engine.FlushedIndex(); got != 0 {
				t.Fatalf("FlushedIndex() = %d before MANIFEST sync, want 0", got)
			}
			if got := engine.PendingImmutables(); got != 1 {
				t.Fatalf("PendingImmutables() = %d after failed flush, want 1 readable immutable", got)
			}

			orphan := createdSSTable(fs.Events())
			if orphan == "" {
				t.Fatalf("no created SSTable in events: %+v", fs.Events())
			}
			orphanPath := filepath.Join(dir, orphan)
			if durable, err := fs.DurableBytes(orphanPath); err != nil || len(durable) == 0 {
				t.Fatalf("orphan was not file-and-directory durable before crash: bytes=%d err=%v", len(durable), err)
			}
			manifestBytes, err := fs.DurableBytes(filepath.Join(dir, manifestFilename))
			if err != nil {
				t.Fatal(err)
			}
			if len(manifestBytes) != retain {
				t.Fatalf("retained MANIFEST bytes = %d, want %d", len(manifestBytes), retain)
			}

			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			reopened, err := Open(dir, Options{FS: fs, Rand: newSeededRand(303)})
			if err != nil {
				t.Fatal(err)
			}
			if got := reopened.FlushedIndex(); got != 0 {
				t.Fatalf("reopened FlushedIndex() = %d, want 0", got)
			}
			if files := reopened.ReferencedSSTables(); len(files) != 0 {
				t.Fatalf("reopened references = %v, want no reference to orphan %q", files, orphan)
			}
			if _, err := fs.LiveBytes(orphanPath); !errors.Is(err, ErrNotExist) {
				t.Fatalf("orphan still visible after cleanup: %v", err)
			}
			cleanupEvents := fs.Events()
			remove := eventOrdinal(cleanupEvents, FSOpRemove, orphan)
			cleanupSync := eventOrdinalAfter(cleanupEvents, FSOpDirSync, dir, remove)
			if remove == 0 || cleanupSync == 0 {
				t.Fatalf("orphan cleanup lacks remove+later directory sync evidence: %+v", cleanupEvents)
			}

			if err := fs.Crash(0); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			if _, err := fs.LiveBytes(orphanPath); !errors.Is(err, ErrNotExist) {
				t.Fatalf("orphan removal did not survive a second crash: %v", err)
			}
		})
	}
}

func eventOrdinal(events []FSEvent, op FSOp, pathSuffix string) uint64 {
	return eventOrdinalAfter(events, op, pathSuffix, 0)
}

func eventOrdinalAfter(events []FSEvent, op FSOp, pathSuffix string, after uint64) uint64 {
	for _, event := range events {
		if event.Ordinal > after && event.Completed && event.Op == op && strings.HasSuffix(event.Path, pathSuffix) {
			return event.Ordinal
		}
	}
	return 0
}

func createdSSTable(events []FSEvent) string {
	for _, event := range events {
		if event.Completed && event.Op == FSOpCreate && strings.HasSuffix(event.Path, sstableFilenameSuffix) {
			return filepath.Base(event.Path)
		}
	}
	return ""
}
