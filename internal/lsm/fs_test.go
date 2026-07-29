package lsm

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRealFSCompleteOperationSurface(t *testing.T) {
	fs := RealFS{}
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	file, err := fs.Create(source)
	if err != nil {
		t.Fatal(err)
	}
	if written, err := file.Write([]byte("abcdef")); err != nil || written != 6 {
		t.Fatalf("Write() = %d,%v", written, err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(4); err != nil {
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
	if _, err := fs.Create(source); !errors.Is(err, ErrExist) {
		t.Fatalf("Create(existing) error = %v, want ErrExist", err)
	}
	info, err := fs.Stat(source)
	if err != nil || info.Size != 4 || info.IsDir {
		t.Fatalf("Stat() = %+v,%v, want size 4 file", info, err)
	}
	opened, err := fs.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 4)
	if _, err := opened.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); !errors.Is(err, ErrFSClosed) {
		t.Fatalf("second Close() error = %v, want ErrFSClosed", err)
	}
	if !bytes.Equal(data, []byte("abcd")) {
		t.Fatalf("ReadAt() = %q", data)
	}

	link := filepath.Join(dir, "link")
	renamed := filepath.Join(dir, "renamed")
	if err := fs.Link(source, link); err != nil {
		t.Fatal(err)
	}
	if err := fs.Rename(link, renamed); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	if names, err := fs.List(dir); err != nil || !reflect.DeepEqual(names, []string{"renamed", "source"}) {
		t.Fatalf("List() = %v,%v", names, err)
	}
	if err := fs.Remove(renamed); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Open(renamed); !errors.Is(err, ErrNotExist) {
		t.Fatalf("Open(removed) error = %v, want ErrNotExist", err)
	}
}

func TestSimFSDirtyPrefixRetentionNoPartialAndAll(t *testing.T) {
	tests := []struct {
		name   string
		retain int
		want   string
	}{
		{name: "none", retain: 0, want: "base"},
		{name: "partial", retain: 3, want: "basedir"},
		{name: "all", retain: RetainAllUnsynced, want: "basedirty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const dir = "/db"
			path := filepath.Join(dir, "file")
			fs := NewSimFS()
			file, err := fs.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write([]byte("base")); err != nil {
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
			file, err = fs.OpenAppend(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write([]byte("dirty")); err != nil {
				t.Fatal(err)
			}
			if err := fs.Crash(test.retain); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			got, err := fs.LiveBytes(path)
			if err != nil || string(got) != test.want {
				t.Fatalf("survivor = %q,%v, want %q", got, err, test.want)
			}
		})
	}
}

func TestSimFSGlobalUnsyncedBytePrefixInterleavesFilesChronologically(t *testing.T) {
	tests := []struct {
		name   string
		retain int
		wantA  string
		wantB  string
	}{
		{name: "none", retain: 0},
		{name: "first byte", retain: 1, wantA: "a"},
		{name: "first write", retain: 2, wantA: "a1"},
		{name: "crosses into second file", retain: 3, wantA: "a1", wantB: "b"},
		{name: "first two writes", retain: 4, wantA: "a1", wantB: "b1"},
		{name: "crosses back to first file", retain: 5, wantA: "a1a", wantB: "b1"},
		{name: "all finite", retain: 6, wantA: "a1a2", wantB: "b1"},
		{name: "all sentinel", retain: RetainAllUnsynced, wantA: "a1a2", wantB: "b1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs, a, b, pathA, pathB := interleavedDirtyFiles(t)
			if err := fs.Crash(test.retain); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			assertSimFileBytes(t, fs, pathA, test.wantA)
			assertSimFileBytes(t, fs, pathB, test.wantB)
			if _, err := a.Write([]byte("stale")); !errors.Is(err, ErrFSClosed) {
				t.Fatalf("pre-crash A handle Write() error = %v, want ErrFSClosed", err)
			}
			if err := b.Sync(); !errors.Is(err, ErrFSClosed) {
				t.Fatalf("pre-crash B handle Sync() error = %v, want ErrFSClosed", err)
			}
		})
	}
}

func TestSimFSFileSyncRemovesOnlyThatInodesPendingMutations(t *testing.T) {
	tests := []struct {
		name   string
		retain int
		wantA  string
		wantB  string
	}{
		{name: "none", retain: 0, wantA: "a1a2"},
		{name: "partial other file", retain: 1, wantA: "a1a2", wantB: "b"},
		{name: "all other file", retain: 2, wantA: "a1a2", wantB: "b1"},
		{name: "then newly dirty synced file", retain: 3, wantA: "a1a2a", wantB: "b1"},
		{name: "all sentinel", retain: RetainAllUnsynced, wantA: "a1a2a3", wantB: "b1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const dir = "/db"
			fs := NewSimFS()
			pathA, pathB := filepath.Join(dir, "A"), filepath.Join(dir, "B")
			a, err := fs.Create(pathA)
			if err != nil {
				t.Fatal(err)
			}
			b, err := fs.Create(pathB)
			if err != nil {
				t.Fatal(err)
			}
			if err := fs.SyncDir(dir); err != nil {
				t.Fatal(err)
			}
			if _, err := a.Write([]byte("a1")); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Write([]byte("b1")); err != nil {
				t.Fatal(err)
			}
			if _, err := a.Write([]byte("a2")); err != nil {
				t.Fatal(err)
			}
			if err := a.Sync(); err != nil {
				t.Fatal(err)
			}
			if _, err := a.Write([]byte("a3")); err != nil {
				t.Fatal(err)
			}
			if err := fs.Crash(test.retain); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			assertSimFileBytes(t, fs, pathA, test.wantA)
			assertSimFileBytes(t, fs, pathB, test.wantB)
		})
	}
}

func TestSimFSPrefixRetentionAppendOverwriteAndTruncate(t *testing.T) {
	t.Run("append", func(t *testing.T) {
		fs, file, path := durableSimFile(t, "base")
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		file, err := fs.OpenAppend(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("tail")); err != nil {
			t.Fatal(err)
		}
		if err := fs.Crash(2); err != nil {
			t.Fatal(err)
		}
		if err := fs.Recover(); err != nil {
			t.Fatal(err)
		}
		assertSimFileBytes(t, fs, path, "baseta")
	})

	t.Run("overwrite", func(t *testing.T) {
		fs, file, path := durableSimFile(t, "base")
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		file, err := fs.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("XY")); err != nil {
			t.Fatal(err)
		}
		if err := fs.Crash(1); err != nil {
			t.Fatal(err)
		}
		if err := fs.Recover(); err != nil {
			t.Fatal(err)
		}
		assertSimFileBytes(t, fs, path, "Xase")
	})

	for _, test := range []struct {
		name   string
		size   int64
		retain int
		want   string
	}{
		{name: "shrink none", size: 3, retain: 0, want: "abcdef"},
		{name: "shrink partial", size: 3, retain: 2, want: "abcd"},
		{name: "shrink all", size: 3, retain: RetainAllUnsynced, want: "abc"},
		{name: "grow partial", size: 9, retain: 2, want: "abcdef\x00\x00"},
		{name: "grow all", size: 9, retain: RetainAllUnsynced, want: "abcdef\x00\x00\x00"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, file, path := durableSimFile(t, "abcdef")
			if err := file.Truncate(test.size); err != nil {
				t.Fatal(err)
			}
			if err := fs.Crash(test.retain); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			assertSimFileBytes(t, fs, path, test.want)
		})
	}
}

func TestSimFSInterleavedSameScheduleSameBytes(t *testing.T) {
	run := func() ([]FSEvent, []byte, []byte) {
		t.Helper()
		fs, _, _, pathA, pathB := interleavedDirtyFiles(t)
		if err := fs.SetCrashSchedule([]SimCrashDirective{{
			Op: FSOpStat, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 4,
		}}); err != nil {
			t.Fatal(err)
		}
		fs.ResetEvents()
		if _, err := fs.Stat(pathA); !errors.Is(err, ErrSimulatedCrash) {
			t.Fatalf("scheduled Stat() error = %v, want ErrSimulatedCrash", err)
		}
		events := fs.Events()
		if err := fs.Recover(); err != nil {
			t.Fatal(err)
		}
		a, err := fs.LiveBytes(pathA)
		if err != nil {
			t.Fatal(err)
		}
		b, err := fs.LiveBytes(pathB)
		if err != nil {
			t.Fatal(err)
		}
		return events, a, b
	}
	firstEvents, firstA, firstB := run()
	secondEvents, secondA, secondB := run()
	if !reflect.DeepEqual(firstEvents, secondEvents) || !bytes.Equal(firstA, secondA) || !bytes.Equal(firstB, secondB) {
		t.Fatalf("same interleaved schedule diverged:\nfirst events=%+v A=%q B=%q\nsecond events=%+v A=%q B=%q",
			firstEvents, firstA, firstB, secondEvents, secondA, secondB)
	}
	if string(firstA) != "a1" || string(firstB) != "b1" {
		t.Fatalf("scheduled survivors = A:%q B:%q, want a1/b1", firstA, firstB)
	}
}

func interleavedDirtyFiles(t *testing.T) (*SimFS, File, File, string, string) {
	t.Helper()
	const dir = "/db"
	fs := NewSimFS()
	pathA, pathB := filepath.Join(dir, "A"), filepath.Join(dir, "B")
	a, err := fs.Create(pathA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := fs.Create(pathB)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write([]byte("a1")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write([]byte("b1")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write([]byte("a2")); err != nil {
		t.Fatal(err)
	}
	return fs, a, b, pathA, pathB
}

func durableSimFile(t *testing.T, contents string) (*SimFS, File, string) {
	t.Helper()
	const dir = "/db"
	path := filepath.Join(dir, "file")
	fs := NewSimFS()
	file, err := fs.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(contents)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	return fs, file, path
}

func assertSimFileBytes(t *testing.T, fs *SimFS, path, want string) {
	t.Helper()
	got, err := fs.LiveBytes(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("LiveBytes(%s) = %q, want %q", path, got, want)
	}
}

func TestSimFSDirectoryDurabilityForCreateRemoveLinkAndRename(t *testing.T) {
	const dir = "/db"
	fs := NewSimFS()
	path := filepath.Join(dir, "new")
	file, err := fs.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("durable bytes, unsynced name")); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Crash(RetainAllUnsynced); err != nil {
		t.Fatal(err)
	}
	if err := fs.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.LiveBytes(path); !errors.Is(err, ErrNotExist) {
		t.Fatalf("unsynced created name survived: %v", err)
	}

	file, err = fs.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("v")); err != nil {
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
	if err := fs.Link(path, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := fs.Rename(filepath.Join(dir, "link"), filepath.Join(dir, "renamed")); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := fs.Crash(0); err != nil {
		t.Fatal(err)
	}
	if err := fs.Recover(); err != nil {
		t.Fatal(err)
	}
	if names, err := fs.List(dir); err != nil || !reflect.DeepEqual(names, []string{"new", "renamed"}) {
		t.Fatalf("durable names = %v,%v", names, err)
	}

	if err := fs.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := fs.Crash(0); err != nil {
		t.Fatal(err)
	}
	if err := fs.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.LiveBytes(path); err != nil {
		t.Fatalf("unsynced removal did not resurrect name: %v", err)
	}
	if err := fs.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := fs.Crash(0); err != nil {
		t.Fatal(err)
	}
	if err := fs.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.LiveBytes(path); !errors.Is(err, ErrNotExist) {
		t.Fatalf("directory-synced removal resurrected: %v", err)
	}
}

func TestSimFSScheduledCrashAndSameScheduleSameBytes(t *testing.T) {
	run := func() ([]FSEvent, []byte) {
		t.Helper()
		const dir = "/db"
		path := filepath.Join(dir, manifestFilename)
		fs := NewSimFS()
		file, err := fs.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("base")); err != nil {
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
		if err := fs.SetCrashSchedule([]SimCrashDirective{{
			Op: FSOpWrite, Occurrence: 1, Point: SimAfterOperation, RetainUnsynced: 2,
		}}); err != nil {
			t.Fatal(err)
		}
		fs.ResetEvents()
		file, err = fs.OpenAppend(path)
		if err != nil {
			t.Fatal(err)
		}
		if written, err := file.Write([]byte("dirty")); written != 5 || !errors.Is(err, ErrSimulatedCrash) {
			t.Fatalf("scheduled Write() = %d,%v", written, err)
		}
		events := fs.Events()
		if err := fs.Recover(); err != nil {
			t.Fatal(err)
		}
		bytes, err := fs.LiveBytes(path)
		if err != nil {
			t.Fatal(err)
		}
		return events, bytes
	}
	firstEvents, firstBytes := run()
	secondEvents, secondBytes := run()
	if !reflect.DeepEqual(firstEvents, secondEvents) || !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("same schedule diverged:\nfirst events=%+v bytes=%q\nsecond events=%+v bytes=%q", firstEvents, firstBytes, secondEvents, secondBytes)
	}
	if string(firstBytes) != "basedi" {
		t.Fatalf("scheduled survivor = %q, want basedi", firstBytes)
	}
}

func TestSimFSErrorScheduleBeforeAndAfterEffects(t *testing.T) {
	boom := errors.New("boom")
	for _, point := range []SimFaultPoint{SimBeforeOperation, SimAfterOperation} {
		t.Run(map[SimFaultPoint]string{SimBeforeOperation: "before", SimAfterOperation: "after"}[point], func(t *testing.T) {
			const dir = "/db"
			path := filepath.Join(dir, "f")
			fs := NewSimFS()
			if err := fs.SetErrorSchedule([]SimErrorDirective{{
				Op: FSOpCreate, Occurrence: 1, Point: point, Err: boom,
			}}); err != nil {
				t.Fatal(err)
			}
			if _, err := fs.Create(path); !errors.Is(err, boom) {
				t.Fatalf("Create() error = %v, want boom", err)
			}
			_, err := fs.Stat(path)
			if point == SimBeforeOperation && !errors.Is(err, ErrNotExist) {
				t.Fatalf("before-error create had an effect: %v", err)
			}
			if point == SimAfterOperation && err != nil {
				t.Fatalf("after-error create lacked its effect: %v", err)
			}
		})
	}
}

func TestSimFSRejectsUnknownFaultScheduleOperations(t *testing.T) {
	unknown := FSOp("file-snyc")
	fs := NewSimFS()
	if err := fs.SetCrashSchedule([]SimCrashDirective{{
		Op: unknown, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 0,
	}}); err == nil {
		t.Fatalf("SetCrashSchedule accepted unknown operation %q", unknown)
	}
	if err := fs.SetErrorSchedule([]SimErrorDirective{{
		Op: unknown, Occurrence: 1, Point: SimBeforeOperation, Err: errors.New("boom"),
	}}); err == nil {
		t.Fatalf("SetErrorSchedule accepted unknown operation %q", unknown)
	}

	declared := []FSOp{
		FSOpCreate,
		FSOpOpen,
		FSOpOpenAppend,
		FSOpRead,
		FSOpWrite,
		FSOpFileSync,
		FSOpDirSync,
		FSOpTruncate,
		FSOpRename,
		FSOpLink,
		FSOpRemove,
		FSOpList,
		FSOpStat,
		FSOpClose,
	}
	for _, op := range declared {
		if err := validateSimFault(op, 1, SimBeforeOperation); err != nil {
			t.Fatalf("validateSimFault rejected declared operation %q: %v", op, err)
		}
	}
}
