package lsm

import (
	"bytes"
	"io"
	"testing"
)

func TestCountingFSCountsActualShortReadAndWriteBytes(t *testing.T) {
	counted := NewCountingFS(shortFS{FS: NewSimFS()})
	manifest, err := counted.Create(manifestFilename)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := manifest.Write([]byte("manifest")); n != len("manifes") || err != io.ErrShortWrite {
		t.Fatalf("short manifest write = %d, %v", n, err)
	}
	if err := manifest.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := manifest.Close(); err != nil {
		t.Fatal(err)
	}

	table, err := counted.Create("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := table.Write([]byte("table")); n != len("tabl") || err != io.ErrShortWrite {
		t.Fatalf("short table write = %d, %v", n, err)
	}
	if err := table.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := table.Close(); err != nil {
		t.Fatal(err)
	}

	table, err = counted.Open("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 32)
	if n, err := table.Read(buffer); n != 1 || err != nil {
		t.Fatalf("short table read = %d, %v", n, err)
	}
	if n, err := table.ReadAt(buffer, 0); n != 1 || err != nil {
		t.Fatalf("short table ReadAt = %d, %v", n, err)
	}
	if err := table.Close(); err != nil {
		t.Fatal(err)
	}

	stats := counted.Snapshot()
	if stats.ManifestWriteCalls != 1 || stats.ManifestWriteBytes != uint64(len("manifes")) {
		t.Fatalf("manifest accounting = %+v", stats)
	}
	if stats.SSTableWriteCalls != 1 || stats.SSTableWriteBytes != uint64(len("tabl")) || stats.SSTableReadCalls != 2 || stats.SSTableReadBytes != 2 {
		t.Fatalf("sstable accounting = %+v", stats)
	}
	counted.Reset()
	if stats = counted.Snapshot(); stats != (CountingStats{}) {
		t.Fatalf("reset accounting = %+v", stats)
	}
}

func TestRandomHeightMeasurementSeamUsesProductionTower(t *testing.T) {
	if got := RandomHeight(&scriptedHeightRand{values: []int{0, 0, 1}}); got != 3 {
		t.Fatalf("height = %d, want 3", got)
	}
	values := make([]int, maxHeight)
	for i := range values {
		values[i] = 0
	}
	if got := RandomHeight(&scriptedHeightRand{values: values}); got != maxHeight {
		t.Fatalf("capped height = %d, want %d", got, maxHeight)
	}
}

func TestBenchmarkBloomSwitchPreservesResultsAndCountersAreMonotonic(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir, Options{Rand: &scriptedHeightRand{values: []int{1}}, FlushThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("present"), []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put([]byte("present"), []byte("new"), 2); err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete([]byte("deleted"), 3); err != nil {
		t.Fatal(err)
	}
	if err := engine.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	baseline := engine.SnapshotBenchmarkCounters()
	if baseline.CompletedFlushes == 0 {
		t.Fatal("flush counter did not advance")
	}
	if got := len(engine.ReferencedSSTables()); got != 3 {
		t.Fatalf("highest-sequence fixture has %d SSTables, want 3", got)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	bloomOn, err := Open(dir, Options{Rand: &scriptedHeightRand{values: []int{1}}, SkipBloom: false})
	if err != nil {
		t.Fatal(err)
	}
	bloomOff, err := Open(dir, Options{Rand: &scriptedHeightRand{values: []int{1}}, SkipBloom: true})
	if err != nil {
		_ = bloomOn.Close()
		t.Fatal(err)
	}
	defer func() { _ = bloomOn.Close() }()
	defer func() { _ = bloomOff.Close() }()
	for _, key := range [][]byte{[]byte("present"), []byte("deleted"), []byte("absent")} {
		onValue, onFound, onErr := bloomOn.Read(key)
		offValue, offFound, offErr := bloomOff.Read(key)
		if onErr != nil || offErr != nil || onFound != offFound || !bytes.Equal(onValue, offValue) {
			t.Fatalf("Bloom switch changed read for %q: on=(%q,%t,%v) off=(%q,%t,%v)", key, onValue, onFound, onErr, offValue, offFound, offErr)
		}
	}
	value, found, err := bloomOn.Read([]byte("present"))
	if err != nil || !found || !bytes.Equal(value, []byte("new")) {
		t.Fatalf("highest sequence across SSTables = (%q,%t,%v), want (new,true,nil)", value, found, err)
	}
	for _, key := range [][]byte{[]byte("deleted"), []byte("absent")} {
		if _, found, err := bloomOn.Read(key); err != nil || found {
			t.Fatalf("expected %q absent, found=%t err=%v", key, found, err)
		}
	}
}

func TestZeroMeasurementFlagsKeepProductionDefaultsAndCountersMonotonic(t *testing.T) {
	engine, err := Open(t.TempDir(), Options{Rand: &scriptedHeightRand{values: []int{1}}, FlushThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = engine.Close() }()
	if !engine.readWithBloom {
		t.Fatal("zero measurement flags disabled production Bloom")
	}
	before := engine.SnapshotBenchmarkCounters()
	for i := 0; i < 4; i++ {
		if err := engine.Put([]byte{byte(i)}, []byte("v"), uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	after := engine.SnapshotBenchmarkCounters()
	if after.CompletedFlushes < before.CompletedFlushes+4 || after.CompletedCompactions <= before.CompletedCompactions {
		t.Fatalf("zero flags disabled production flush/compaction: before=%+v after=%+v", before, after)
	}
}

type shortFS struct{ FS }

func (fs shortFS) Create(name string) (File, error) {
	file, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return shortFile{File: file}, nil
}

func (fs shortFS) Open(name string) (File, error) {
	file, err := fs.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return shortFile{File: file}, nil
}

func (fs shortFS) OpenAppend(name string) (File, error) {
	file, err := fs.FS.OpenAppend(name)
	if err != nil {
		return nil, err
	}
	return shortFile{File: file}, nil
}

type shortFile struct{ File }

func (file shortFile) Read(dst []byte) (int, error) {
	if len(dst) > 1 {
		dst = dst[:1]
	}
	return file.File.Read(dst)
}

func (file shortFile) ReadAt(dst []byte, offset int64) (int, error) {
	if len(dst) > 1 {
		dst = dst[:1]
	}
	return file.File.ReadAt(dst, offset)
}

func (file shortFile) Write(src []byte) (int, error) {
	if len(src) > 1 {
		n, err := file.File.Write(src[:len(src)-1])
		if err != nil {
			return n, err
		}
		return n, io.ErrShortWrite
	}
	return file.File.Write(src)
}

type scriptedHeightRand struct {
	values []int
	index  int
}

func (rnd *scriptedHeightRand) IntN(n int) int {
	if rnd.index >= len(rnd.values) {
		return 1 % n
	}
	value := rnd.values[rnd.index]
	rnd.index++
	return value % n
}
