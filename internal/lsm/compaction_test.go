package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	raftpb "miniquorum/proto"
)

type sliceTableIterator struct {
	entries []TableEntry
	index   int
	errAt   int
	err     error
}

func (iterator *sliceTableIterator) Next() (TableEntry, bool, error) {
	if iterator.err != nil && iterator.index == iterator.errAt {
		return TableEntry{}, false, iterator.err
	}
	if iterator.index == len(iterator.entries) {
		return TableEntry{}, false, nil
	}
	entry := cloneTableEntry(iterator.entries[iterator.index])
	iterator.index++
	return entry, true, nil
}

func TestKWayMergeOrderingHighestSequenceAndBoundaries(t *testing.T) {
	large := bytes.Repeat([]byte("L"), 1<<20)
	inputs := []tableEntryIterator{
		&sliceTableIterator{entries: []TableEntry{
			{Key: nil, Seq: 2, Value: []byte("empty-old")},
			{Key: []byte("b"), Seq: 9, Value: []byte("b-high")},
			{Key: []byte("z"), Seq: 1, Value: large},
		}},
		&sliceTableIterator{entries: []TableEntry{
			{Key: nil, Seq: 7, Tombstone: true},
			{Key: []byte("a"), Seq: 3, Value: []byte("a")},
			{Key: []byte("b"), Seq: 4, Value: []byte("b-low")},
		}},
		&sliceTableIterator{entries: []TableEntry{
			{Key: []byte("b"), Seq: 9, Value: []byte("b-high")},
			{Key: []byte("c"), Seq: 8, Value: []byte("c")},
		}},
		&sliceTableIterator{},
	}
	merge, err := newKWayMergeIterator(inputs)
	if err != nil {
		t.Fatal(err)
	}
	var got []TableEntry
	for {
		entry, found, err := merge.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		got = append(got, entry)
	}
	if len(got) != 5 {
		t.Fatalf("merged entries = %d, want 5: %+v", len(got), got)
	}
	for index := 1; index < len(got); index++ {
		if bytes.Compare(got[index-1].Key, got[index].Key) >= 0 {
			t.Fatalf("merged keys are not strictly ordered: %q then %q", got[index-1].Key, got[index].Key)
		}
	}
	if len(got[0].Key) != 0 || !got[0].Tombstone || got[0].Seq != 7 {
		t.Fatalf("empty-key winner = %+v, want seq-7 tombstone", got[0])
	}
	if string(got[2].Key) != "b" || string(got[2].Value) != "b-high" || got[2].Seq != 9 {
		t.Fatalf("overlap winner = %+v, want b-high at seq 9", got[2])
	}
	if string(got[4].Key) != "z" || !bytes.Equal(got[4].Value, large) {
		t.Fatalf("large-value winner = key:%q len:%d", got[4].Key, len(got[4].Value))
	}

	t.Run("conflicting equal sequence is corruption", func(t *testing.T) {
		merge, err := newKWayMergeIterator([]tableEntryIterator{
			&sliceTableIterator{entries: []TableEntry{{Key: []byte("same"), Seq: 11, Value: []byte("left")}}},
			&sliceTableIterator{entries: []TableEntry{{Key: []byte("same"), Seq: 11, Value: []byte("right")}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := merge.Next(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Next() error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("conflicting lower sequence is still corruption", func(t *testing.T) {
		merge, err := newKWayMergeIterator([]tableEntryIterator{
			&sliceTableIterator{entries: []TableEntry{
				{Key: []byte("same"), Seq: 20, Value: []byte("winner")},
				{Key: []byte("same"), Seq: 11, Value: []byte("left")},
			}},
			&sliceTableIterator{entries: []TableEntry{{Key: []byte("same"), Seq: 11, Value: []byte("right")}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := merge.Next(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Next() error = %v, want ErrCorrupt for hidden lower conflict", err)
		}
	})

	t.Run("iterator error propagates", func(t *testing.T) {
		boom := errors.New("input failure")
		merge, err := newKWayMergeIterator([]tableEntryIterator{
			&sliceTableIterator{entries: []TableEntry{{Key: []byte("a"), Seq: 1, Value: []byte("a")}}, errAt: 1, err: boom},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := merge.Next(); !errors.Is(err, boom) {
			t.Fatalf("Next() error = %v, want input failure", err)
		}
	})
}

func TestTableSizeClassBoundariesAndOverflow(t *testing.T) {
	const base = sizeClassBaseBytes
	tests := []struct {
		size int64
		want uint32
	}{
		{size: -1, want: 0},
		{size: 0, want: 0},
		{size: base - 1, want: 0},
		{size: base, want: 1},
		{size: base + 1, want: 1},
		{size: base*4 - 1, want: 1},
		{size: base * 4, want: 2},
		{size: base*4 + 1, want: 2},
		{size: math.MaxInt64, want: 26},
	}
	for _, test := range tests {
		if got := tableSizeClass(test.size); got != test.want {
			t.Errorf("tableSizeClass(%d) = %d, want %d", test.size, got, test.want)
		}
	}
}

func TestFlushPersistsPhysicalSizeClassAcrossReopen(t *testing.T) {
	payloadSizes := []int{1, int(sizeClassBaseBytes * 2), int(sizeClassBaseBytes * 8)}
	var observed []uint32
	for index, payloadSize := range payloadSizes {
		t.Run(fmt.Sprintf("payload-%d", payloadSize), func(t *testing.T) {
			dir := fmt.Sprintf("/flush-class-%d", index)
			fs := NewSimFS()
			engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(int64(4700 + index))})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Put([]byte("key"), bytes.Repeat([]byte{byte('a' + index)}, payloadSize), uint64(index+1)); err != nil {
				t.Fatal(err)
			}
			if err := engine.ForceFlush(); err != nil {
				t.Fatal(err)
			}
			if len(engine.tables) != 1 {
				t.Fatalf("flush tables = %d, want 1", len(engine.tables))
			}
			table := engine.tables[0]
			stat, err := fs.Stat(filepath.Join(dir, table.metadata.file))
			if err != nil {
				t.Fatal(err)
			}
			wantTier := tableSizeClass(stat.Size)
			if table.size != stat.Size || table.metadata.tier != wantTier || engine.manifest.files[table.metadata.file].tier != wantTier {
				t.Fatalf("flush class = handle-size:%d stat:%d handle-tier:%d manifest-tier:%d want:%d",
					table.size, stat.Size, table.metadata.tier, engine.manifest.files[table.metadata.file].tier, wantTier)
			}
			observed = append(observed, wantTier)
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir, Options{FS: fs, Rand: newSeededRand(int64(4750 + index))})
			if err != nil {
				t.Fatal(err)
			}
			if len(reopened.tables) != 1 || reopened.tables[0].metadata.tier != wantTier {
				t.Fatalf("reopened tier = %d, want persisted %d", reopened.tables[0].metadata.tier, wantTier)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if len(observed) != len(payloadSizes) || observed[0] >= observed[1] || observed[1] >= observed[2] || observed[1] == 0 {
		t.Fatalf("payload classes = %v, want three increasing classes above tier zero", observed)
	}
}

func TestCompactionSelectionDeterministicThresholdAndPermutations(t *testing.T) {
	makeTables := func(tier uint32, count int, prefix string) []*tableHandle {
		tables := make([]*tableHandle, count)
		for index := range tables {
			tables[index] = &tableHandle{metadata: fileMetadata{
				tier: tier,
				file: fmt.Sprintf("%s-%02d.sst", prefix, index),
			}}
		}
		return tables
	}

	if _, _, found := selectCompactionInputs(makeTables(0, 3, "three")); found {
		t.Fatal("three files triggered compaction")
	}
	for _, count := range []int{4, 5, 8} {
		t.Run(fmt.Sprintf("%d-files", count), func(t *testing.T) {
			tables := makeTables(3, count, "file")
			slices.Reverse(tables)
			tier, selected, found := selectCompactionInputs(tables)
			if !found || tier != 3 || len(selected) != compactionFanIn {
				t.Fatalf("selection = tier:%d count:%d found:%v", tier, len(selected), found)
			}
			for index, table := range selected {
				want := fmt.Sprintf("file-%02d.sst", index)
				if table.metadata.file != want {
					t.Fatalf("selected[%d] = %s, want %s", index, table.metadata.file, want)
				}
			}
		})
	}

	low := makeTables(1, 4, "low")
	high := makeTables(7, 8, "high")
	all := append(high, low...)
	slices.Reverse(all)
	tier, selected, found := selectCompactionInputs(all)
	if !found || tier != 1 || len(selected) != 4 || !strings.HasPrefix(selected[0].metadata.file, "low") {
		t.Fatalf("simultaneous class selection = tier:%d selected:%v found:%v", tier, selectedNames(selected), found)
	}

	// Physical sizes are deliberately inconsistent with metadata tiers:
	// selection must remain driven by the persisted MANIFEST tier.
	for index, table := range low {
		table.size = math.MaxInt64 - int64(index)
	}
	if tier, _, found := selectCompactionInputs(low); !found || tier != 1 {
		t.Fatalf("selection reclassified persisted tiers from Stat sizes: tier=%d found=%v", tier, found)
	}

	inputEntries := [][]TableEntry{
		{{Key: []byte("a"), Seq: 1, Value: []byte("old")}, {Key: []byte("d"), Seq: 8, Value: []byte("d")}},
		{{Key: []byte("a"), Seq: 9, Value: []byte("new")}, {Key: []byte("c"), Seq: 3, Tombstone: true}},
		{{Key: []byte("b"), Seq: 5, Value: []byte("b")}},
		{{Key: []byte("e"), Seq: 7, Value: []byte("e")}},
	}
	canonical := mergedSSTableBytes(t, inputEntries, []int{0, 1, 2, 3})
	for _, permutation := range [][]int{
		{3, 2, 1, 0},
		{1, 3, 0, 2},
		{2, 0, 3, 1},
	} {
		if got := mergedSSTableBytes(t, inputEntries, permutation); !bytes.Equal(got, canonical) {
			t.Fatalf("merged output changed under input permutation %v", permutation)
		}
	}
}

func TestCompactionOutputEditAndEventsIgnoreInputAndMapPermutation(t *testing.T) {
	type result struct {
		events   []FSEvent
		manifest []byte
		output   []byte
		files    []string
	}
	run := func(permutation []int) result {
		const dir = "/permutation"
		fs := NewSimFS()
		engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(4901)})
		if err != nil {
			t.Fatal(err)
		}
		inputs := [][]TableEntry{
			{{Key: []byte("a"), Seq: 9, Value: []byte("a-new")}, {Key: []byte("d"), Seq: 1, Value: []byte("d")}},
			{{Key: []byte("a"), Seq: 3, Value: []byte("a-old")}, {Key: []byte("c"), Seq: 8, Tombstone: true}},
			{{Key: []byte("b"), Seq: 5, Value: []byte("b")}},
			{{Key: []byte("e"), Seq: 7, Value: []byte("e")}},
		}
		for _, entries := range inputs {
			installTestTable(t, engine, 0, entries)
		}

		permutedTables := make([]*tableHandle, len(permutation))
		for index, source := range permutation {
			permutedTables[index] = engine.tables[source]
		}
		engine.tables = permutedTables
		reversedFiles := make(map[string]fileMetadata, len(engine.manifest.files))
		names := engine.ReferencedSSTables()
		slices.Reverse(names)
		for _, name := range names {
			reversedFiles[name] = engine.manifest.files[name]
		}
		engine.manifest.files = reversedFiles

		fs.ResetEvents()
		if err := engine.compact(); err != nil {
			t.Fatal(err)
		}
		files := engine.ReferencedSSTables()
		if len(files) != 1 {
			t.Fatalf("permuted compaction files = %v", files)
		}
		manifest, err := fs.DurableBytes(filepath.Join(dir, manifestFilename))
		if err != nil {
			t.Fatal(err)
		}
		output, err := fs.DurableBytes(filepath.Join(dir, files[0]))
		if err != nil {
			t.Fatal(err)
		}
		return result{events: fs.Events(), manifest: manifest, output: output, files: files}
	}

	canonical := run([]int{0, 1, 2, 3})
	for _, permutation := range [][]int{
		{3, 2, 1, 0},
		{1, 3, 0, 2},
		{2, 0, 3, 1},
	} {
		if got := run(permutation); !reflect.DeepEqual(got, canonical) {
			t.Fatalf("compaction changed under permutation %v:\ngot=%+v\ncanonical=%+v", permutation, got, canonical)
		}
	}
}

func TestCompactionTriggerAtomicEditMetadataAndOrdering(t *testing.T) {
	const dir = "/db"
	fs := NewSimFS()
	engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(5001), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}

	flush := func(entries []TableEntry) {
		t.Helper()
		for _, entry := range entries {
			if entry.Tombstone {
				err = engine.Delete(entry.Key, entry.Seq)
			} else {
				err = engine.Put(entry.Key, entry.Value, entry.Seq)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := engine.ForceFlush(); err != nil {
			t.Fatal(err)
		}
	}
	flush([]TableEntry{{Key: []byte("a"), Seq: 1, Value: []byte("a")}, {Key: []byte("shared"), Seq: 40, Value: []byte("seq40")}})
	flush([]TableEntry{{Key: []byte("b"), Seq: 2, Value: []byte("b")}, {Key: []byte("shared"), Seq: 10, Value: []byte("seq10-created-later")}})
	flush([]TableEntry{{Key: []byte("c"), Seq: 3, Value: []byte("c")}, {Key: []byte("victim"), Seq: 15, Value: []byte("live")}})
	if got := len(engine.ReferencedSSTables()); got != 3 {
		t.Fatalf("three files referenced = %d, want 3 (no compaction)", got)
	}

	inputs := append([]string(nil), engine.ReferencedSSTables()...)
	selectedTier := engine.tables[0].metadata.tier
	for _, table := range engine.tables {
		if table.metadata.tier != selectedTier {
			t.Fatalf("pre-trigger inputs span tiers: %d and %d", selectedTier, table.metadata.tier)
		}
	}
	fs.ResetEvents()
	flush([]TableEntry{{Key: []byte("d"), Seq: 4, Value: []byte("d")}, {Key: []byte("victim"), Seq: 50, Tombstone: true}})
	references := engine.ReferencedSSTables()
	if len(references) != 1 {
		t.Fatalf("four-file trigger references = %v, want one output", references)
	}
	outputName := references[0]
	output := engine.tables[0]
	if output.metadata.tier != 1 || output.metadata.count != 6 || output.metadata.file != outputName {
		t.Fatalf("output metadata = tier:%d count:%d file:%s, want tier 1 count 6 file %s", output.metadata.tier, output.metadata.count, output.metadata.file, outputName)
	}
	if output.metadata.tier != selectedTier+1 {
		t.Fatalf("output tier %d is not selected tier %d plus one", output.metadata.tier, selectedTier)
	}
	if engine.FlushedIndex() != 50 {
		t.Fatalf("compaction changed watermark to %d, want 50", engine.FlushedIndex())
	}
	if value, _, seq, found, err := engine.Lookup([]byte("shared")); err != nil || !found || seq != 40 || string(value) != "seq40" {
		t.Fatalf("creation-order Lookup(shared) = %q seq:%d found:%v err:%v", value, seq, found, err)
	}
	if value, found, err := engine.Read([]byte("victim")); err != nil || found || value != nil {
		t.Fatalf("Read(victim) = %q,%v,%v, want retained tombstone", value, found, err)
	}
	if _, tombstone, seq, found, err := engine.Lookup([]byte("victim")); err != nil || !found || !tombstone || seq != 50 {
		t.Fatalf("Lookup(victim) = tomb:%v seq:%d found:%v err:%v", tombstone, seq, found, err)
	}

	manifestBytes, err := fs.DurableBytes(filepath.Join(dir, manifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	edits := decodeManifestEditsForTest(t, manifestBytes)
	if len(edits) != 5 {
		t.Fatalf("manifest edits = %d, want four flushes plus one compaction", len(edits))
	}
	last := edits[len(edits)-1]
	if last.GetFlushedIndex() != 50 || len(last.GetAddedFiles()) != 1 || len(last.GetRemovedFiles()) != 4 {
		t.Fatalf("atomic compaction VersionEdit = %+v", last)
	}
	if !reflect.DeepEqual(last.GetRemovedFiles(), append(inputs, "000004.sst")) {
		t.Fatalf("removed inputs = %v, want exact deterministic four", last.GetRemovedFiles())
	}
	added := last.GetAddedFiles()[0]
	if added.GetFile() != outputName || added.GetTier() != 1 || added.GetCount() != 6 ||
		!bytes.Equal(added.GetMinKey(), []byte("a")) || !bytes.Equal(added.GetMaxKey(), []byte("victim")) {
		t.Fatalf("added output metadata = %+v", added)
	}

	events := fs.Events()
	outputWrite := eventOrdinal(events, FSOpWrite, outputName)
	outputSync := eventOrdinal(events, FSOpFileSync, outputName)
	outputDirSync := eventOrdinalAfter(events, FSOpDirSync, dir, outputSync)
	manifestWrite := eventOrdinalAfter(events, FSOpWrite, manifestFilename, outputDirSync)
	manifestSync := eventOrdinalAfter(events, FSOpFileSync, manifestFilename, manifestWrite)
	firstRemoval := eventOrdinalAfter(events, FSOpRemove, sstableFilenameSuffix, manifestSync)
	cleanupDirSync := eventOrdinalAfter(events, FSOpDirSync, dir, firstRemoval)
	if outputWrite == 0 || outputSync == 0 || outputDirSync == 0 || manifestWrite == 0 || manifestSync == 0 || firstRemoval == 0 || cleanupDirSync == 0 {
		t.Fatalf("missing compaction ordering evidence: %+v", events)
	}
	if outputWrite >= outputSync || outputSync >= outputDirSync || outputDirSync >= manifestWrite ||
		manifestWrite >= manifestSync || manifestSync >= firstRemoval || firstRemoval >= cleanupDirSync {
		t.Fatalf("compaction durability order = write:%d sync:%d dir:%d manifest-write:%d manifest-sync:%d remove:%d cleanup-dir:%d",
			outputWrite, outputSync, outputDirSync, manifestWrite, manifestSync, firstRemoval, cleanupDirSync)
	}
	for _, input := range last.GetRemovedFiles() {
		if _, err := fs.LiveBytes(filepath.Join(dir, input)); !errors.Is(err, ErrNotExist) {
			t.Fatalf("obsolete input %s remains live: %v", input, err)
		}
	}
	var removalOrder []string
	for _, event := range events {
		if event.Completed && event.Op == FSOpRemove {
			removalOrder = append(removalOrder, filepath.Base(event.Path))
		}
	}
	if !reflect.DeepEqual(removalOrder, last.GetRemovedFiles()) {
		t.Fatalf("input removal order = %v, want deterministic edit order %v", removalOrder, last.GetRemovedFiles())
	}
}

func TestCompactionCascadesSynchronously(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/cascade", Options{FS: fs, Rand: newSeededRand(5101), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < compactionFanIn*compactionFanIn; index++ {
		key := []byte(fmt.Sprintf("key-%02d", index))
		if err := engine.Put(key, []byte("value"), uint64(index+1)); err != nil {
			t.Fatal(err)
		}
		if err := engine.ForceFlush(); err != nil {
			t.Fatal(err)
		}
	}
	if len(engine.tables) != 1 || engine.tables[0].metadata.tier != 2 || engine.tables[0].metadata.count != 16 {
		t.Fatalf("cascade state = tables:%d tier:%d count:%d, want one tier-2 table with 16 keys",
			len(engine.tables), engine.tables[0].metadata.tier, engine.tables[0].metadata.count)
	}
	for index := 0; index < 16; index++ {
		key := []byte(fmt.Sprintf("key-%02d", index))
		if value, found, err := engine.Read(key); err != nil || !found || string(value) != "value" {
			t.Fatalf("Read(%q) = %q,%v,%v", key, value, found, err)
		}
	}
}

func TestCompactionRetainsTombstoneThroughCascade(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/cascade-tombstone", Options{FS: fs, Rand: newSeededRand(5121)})
	if err != nil {
		t.Fatal(err)
	}
	installTestTable(t, engine, 3, []TableEntry{{Key: []byte("victim"), Seq: 10, Value: []byte("outside-old-live")}})
	installTestTable(t, engine, 0, []TableEntry{
		{Key: []byte("key-00"), Seq: 20, Value: []byte("value")},
		{Key: []byte("victim"), Seq: 100, Tombstone: true},
	})
	for index := 1; index < compactionFanIn*compactionFanIn; index++ {
		installTestTable(t, engine, 0, []TableEntry{{
			Key: []byte(fmt.Sprintf("key-%02d", index)), Seq: uint64(20 + index), Value: []byte("value"),
		}})
	}
	if err := engine.compact(); err != nil {
		t.Fatal(err)
	}
	if len(engine.tables) != 2 {
		t.Fatalf("cascade tombstone tables = %d, want outside plus tier-2 output", len(engine.tables))
	}
	var compacted *tableHandle
	for _, table := range engine.tables {
		if table.metadata.tier == 2 {
			compacted = table
		}
	}
	if compacted == nil {
		t.Fatalf("no tier-2 cascade output: %+v", engine.manifest.files)
	}
	assertVictimDeleted(t, engine, 100)
	assertPublishedTombstone(t, engine, "victim", 100)
}

func TestAutomaticThresholdCrossingTriggersFourthFileCompaction(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/automatic-trigger", Options{FS: fs, Rand: newSeededRand(5131), FlushThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if err := engine.Put([]byte(fmt.Sprintf("key-%d", index)), []byte("value"), uint64(index+1)); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(engine.ReferencedSSTables()); got != 3 {
		t.Fatalf("three automatic flushes referenced %d files, want 3", got)
	}
	if err := engine.Put([]byte("key-3"), []byte("value"), 4); err != nil {
		t.Fatal(err)
	}
	if got := len(engine.ReferencedSSTables()); got != 1 {
		t.Fatalf("fourth automatic flush references %d files, want compacted output", got)
	}
	if len(engine.tables) != 1 || engine.tables[0].metadata.tier != 1 {
		t.Fatalf("automatic compaction output = tables:%d tier:%d", len(engine.tables), engine.tables[0].metadata.tier)
	}
}

func TestCompactionTierOverflowFailsWithoutMutation(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/tier-overflow", Options{FS: fs, Rand: newSeededRand(5151)})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < compactionFanIn; index++ {
		installTestTable(t, engine, math.MaxUint32, []TableEntry{{
			Key: []byte(fmt.Sprintf("key-%d", index)), Seq: uint64(index + 1), Value: []byte("value"),
		}})
	}
	before := engine.ReferencedSSTables()
	fs.ResetEvents()
	if err := engine.compact(); err == nil || !strings.Contains(err.Error(), "tier exhausted") {
		t.Fatalf("compact() error = %v, want explicit tier overflow", err)
	}
	if got := engine.ReferencedSSTables(); !reflect.DeepEqual(got, before) {
		t.Fatalf("tier overflow changed references: got %v want %v", got, before)
	}
	if events := fs.Events(); len(events) != 0 {
		t.Fatalf("tier overflow performed filesystem operations: %+v", events)
	}
}

func TestCompactionTombstoneResurrectionMatrix(t *testing.T) {
	for mask := 0; mask < 1<<(compactionFanIn-1); mask++ {
		t.Run(fmt.Sprintf("selected-key-subset-%03b", mask), func(t *testing.T) {
			fs := NewSimFS()
			engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(int64(5200 + mask))})
			if err != nil {
				t.Fatal(err)
			}
			installTestTable(t, engine, 1, []TableEntry{{Key: []byte("victim"), Seq: 10, Value: []byte("outside-old-live")}})
			installTestTable(t, engine, 0, []TableEntry{
				{Key: []byte("selected-0"), Seq: 90, Value: []byte("filler")},
				{Key: []byte("victim"), Seq: 100, Tombstone: true},
			})
			for input := 0; input < compactionFanIn-1; input++ {
				entries := []TableEntry{{Key: []byte(fmt.Sprintf("selected-%d", input+1)), Seq: uint64(20 + input), Value: []byte("filler")}}
				if mask&(1<<input) != 0 {
					entries = append(entries, TableEntry{Key: []byte("victim"), Seq: uint64(30 + input), Value: []byte("selected-old-live")})
				}
				installTestTable(t, engine, 0, entries)
			}
			if err := engine.compact(); err != nil {
				t.Fatal(err)
			}
			assertVictimDeleted(t, engine, 100)
			if len(engine.tables) != 2 {
				t.Fatalf("tables after subset compaction = %d, want outside plus output", len(engine.tables))
			}
			assertPublishedTombstone(t, engine, "victim", 100)
		})
	}

	t.Run("fifth same-tier table stays outside selected inputs", func(t *testing.T) {
		fs := NewSimFS()
		engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(5301)})
		if err != nil {
			t.Fatal(err)
		}
		installTestTable(t, engine, 0, []TableEntry{{Key: []byte("victim"), Seq: 100, Tombstone: true}})
		for index := 1; index < 4; index++ {
			installTestTable(t, engine, 0, []TableEntry{{Key: []byte(fmt.Sprintf("filler-%d", index)), Seq: uint64(index), Value: []byte("filler")}})
		}
		outside := installTestTable(t, engine, 0, []TableEntry{{Key: []byte("victim"), Seq: 10, Value: []byte("fifth-old-live")}})
		if err := engine.compact(); err != nil {
			t.Fatal(err)
		}
		assertVictimDeleted(t, engine, 100)
		if _, present := engine.manifest.files[outside.metadata.file]; !present {
			t.Fatalf("fifth table %s was compacted despite deterministic four-input selection", outside.metadata.file)
		}
	})

	t.Run("newer outside live legitimately wins", func(t *testing.T) {
		fs := NewSimFS()
		engine, err := Open("/db", Options{FS: fs, Rand: newSeededRand(5302)})
		if err != nil {
			t.Fatal(err)
		}
		installTestTable(t, engine, 2, []TableEntry{{Key: []byte("victim"), Seq: 200, Value: []byte("outside-new-live")}})
		installTestTable(t, engine, 0, []TableEntry{{Key: []byte("victim"), Seq: 100, Tombstone: true}})
		for index := 1; index < 4; index++ {
			installTestTable(t, engine, 0, []TableEntry{{Key: []byte(fmt.Sprintf("filler-%d", index)), Seq: uint64(index), Value: []byte("filler")}})
		}
		if err := engine.compact(); err != nil {
			t.Fatal(err)
		}
		if value, found, err := engine.Read([]byte("victim")); err != nil || !found || string(value) != "outside-new-live" {
			t.Fatalf("Read(victim) = %q,%v,%v, want legitimate seq-200 live winner", value, found, err)
		}
		assertPublishedTombstone(t, engine, "victim", 100)
	})
}

func TestCompactionRejectsConflictingEqualSequence(t *testing.T) {
	fs := NewSimFS()
	engine, err := Open("/conflict", Options{FS: fs, Rand: newSeededRand(5401)})
	if err != nil {
		t.Fatal(err)
	}
	installTestTable(t, engine, 0, []TableEntry{
		{Key: []byte("same"), Seq: 20, Value: []byte("higher-winner")},
		{Key: []byte("same"), Seq: 7, Value: []byte("left")},
	})
	installTestTable(t, engine, 0, []TableEntry{{Key: []byte("same"), Seq: 7, Value: []byte("right")}})
	installTestTable(t, engine, 0, []TableEntry{{Key: []byte("x"), Seq: 1, Value: []byte("x")}})
	installTestTable(t, engine, 0, []TableEntry{{Key: []byte("y"), Seq: 2, Value: []byte("y")}})
	before := engine.ReferencedSSTables()
	if err := engine.compact(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("compact() error = %v, want ErrCorrupt", err)
	}
	if got := engine.ReferencedSSTables(); !reflect.DeepEqual(got, before) {
		t.Fatalf("failed merge published files %v, want old %v", got, before)
	}
	assertNoSSTableOrphans(t, fs, "/conflict", before)
}

func TestCompactionCleanupEventOrderDeterministic(t *testing.T) {
	var canonical []string
	for run := 0; run < 16; run++ {
		const dir = "/cleanup-order"
		fs, engine := setupCompactionScenario(t, dir, int64(5450+run))
		_, selected, found := selectCompactionInputs(engine.tables)
		if !found {
			t.Fatal("setup has no eligible compaction")
		}
		want := selectedNames(selected)
		fs.ResetEvents()
		if err := engine.compact(); err != nil {
			t.Fatal(err)
		}
		events := fs.Events()
		manifestSync := eventOrdinal(events, FSOpFileSync, manifestFilename)
		var closes, removes []string
		for _, event := range events {
			if !event.Completed || event.Ordinal <= manifestSync {
				continue
			}
			switch event.Op {
			case FSOpClose:
				if strings.HasSuffix(event.Path, sstableFilenameSuffix) {
					closes = append(closes, filepath.Base(event.Path))
				}
			case FSOpRemove:
				removes = append(removes, filepath.Base(event.Path))
			}
		}
		if !reflect.DeepEqual(closes, want) || !reflect.DeepEqual(removes, want) {
			t.Fatalf("cleanup order run %d = closes:%v removes:%v, want %v", run, closes, removes, want)
		}
		trace := append(append([]string(nil), closes...), removes...)
		if run == 0 {
			canonical = trace
		} else if !reflect.DeepEqual(trace, canonical) {
			t.Fatalf("cleanup trace run %d = %v, canonical %v", run, trace, canonical)
		}
	}
}

func TestCompactionReaderPublicationIsAtomic(t *testing.T) {
	for _, stage := range []compactionStage{compactionBeforePublish, compactionAfterPublish} {
		t.Run(fmt.Sprintf("stage-%d", stage), func(t *testing.T) {
			fs := NewSimFS()
			engine, err := Open("/atomic", Options{FS: fs, Rand: newSeededRand(5501 + int64(stage))})
			if err != nil {
				t.Fatal(err)
			}
			for index := 0; index < 4; index++ {
				installTestTable(t, engine, 0, []TableEntry{{Key: []byte("key"), Seq: uint64(index + 1), Value: []byte(fmt.Sprintf("v%d", index+1))}})
			}

			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			engine.compactionHook = func(got compactionStage) {
				if got == stage {
					once.Do(func() {
						close(entered)
						<-release
					})
				}
			}
			compacted := make(chan error, 1)
			go func() { compacted <- engine.compact() }()
			<-entered

			readerStarted := make(chan struct{})
			readResult := make(chan error, 1)
			go func() {
				close(readerStarted)
				value, _, seq, found, err := engine.Lookup([]byte("key"))
				if err == nil && (!found || seq != 4 || string(value) != "v4") {
					err = fmt.Errorf("mixed read = %q seq:%d found:%v", value, seq, found)
				}
				readResult <- err
			}()
			<-readerStarted
			select {
			case err := <-readResult:
				t.Fatalf("reader completed inside compaction publication boundary: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			if engine.mu.TryRLock() {
				engine.mu.RUnlock()
				t.Fatal("read lock acquired while compaction held the version write lock")
			}
			close(release)
			if err := <-compacted; err != nil {
				t.Fatal(err)
			}
			if err := <-readResult; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionUncertainManifestAppendPoisonsMutationsButAllowsRecovery(t *testing.T) {
	boom := errors.New("uncertain manifest append")
	tests := []struct {
		name      string
		directive SimErrorDirective
		wantNew   bool
	}{
		{
			name: "write after effect recovers old version",
			directive: SimErrorDirective{
				Op: FSOpWrite, Occurrence: 2, Point: SimAfterOperation, Err: boom,
			},
		},
		{
			name: "sync after effect recovers new version",
			directive: SimErrorDirective{
				Op: FSOpFileSync, Occurrence: 2, Point: SimAfterOperation, Err: boom,
			},
			wantNew: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const dir = "/poison"
			fs, engine := setupCompactionScenario(t, dir, 5601)
			beforeFiles := engine.ReferencedSSTables()
			_, newFiles := expectedCompactionFileSets(t, engine)
			beforeApplied := engine.AppliedIndex()
			beforeEntries := engine.Entries()

			if err := fs.SetErrorSchedule([]SimErrorDirective{test.directive}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if err := engine.compact(); !errors.Is(err, boom) {
				t.Fatalf("compact() error = %v, want injected uncertainty", err)
			}
			if engine.poisoned == nil {
				t.Fatal("uncertain MANIFEST append did not poison the engine")
			}
			if got := engine.ReferencedSSTables(); !reflect.DeepEqual(got, beforeFiles) {
				t.Fatalf("uncertain append published files %v, want old %v", got, beforeFiles)
			}

			eventCount := len(fs.Events())
			blocked := []struct {
				name string
				call func() error
			}{
				{name: "Put", call: func() error { return engine.Put([]byte("blocked-put"), []byte("value"), 1000) }},
				{name: "Delete", call: func() error { return engine.Delete([]byte("blocked-delete"), 1001) }},
				{name: "AdvanceAppliedIndex", call: func() error { return engine.AdvanceAppliedIndex(1002) }},
				{name: "ForceFlush", call: engine.ForceFlush},
				{name: "compaction", call: engine.compact},
			}
			for _, operation := range blocked {
				if err := operation.call(); !errors.Is(err, boom) || !strings.Contains(err.Error(), "fail-stopped") {
					t.Fatalf("%s error = %v, want latched fail-stop cause", operation.name, err)
				}
			}
			if got := len(fs.Events()); got != eventCount {
				t.Fatalf("latched mutations performed %d filesystem events, want %d", got, eventCount)
			}
			if engine.AppliedIndex() != beforeApplied || !reflect.DeepEqual(engine.Entries(), beforeEntries) ||
				!reflect.DeepEqual(engine.ReferencedSSTables(), beforeFiles) {
				t.Fatal("latched mutation changed in-memory state")
			}
			assertVictimDeleted(t, engine, 100)

			if err := engine.Close(); err != nil {
				t.Fatalf("Close() after poison = %v", err)
			}
			if len(engine.unresolved) != 0 {
				t.Fatalf("Close retained %d unresolved handles", len(engine.unresolved))
			}
			if err := fs.Crash(0); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			reopened := assertRecoveredCompactionState(t, fs, dir)
			wantFiles := beforeFiles
			if test.wantNew {
				wantFiles = newFiles
			}
			if got := reopened.ReferencedSSTables(); !reflect.DeepEqual(got, wantFiles) {
				t.Fatalf("recovery files = %v, want %v", got, wantFiles)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionSyncedManifestCloseFailurePublishesAndRetainsOwnership(t *testing.T) {
	boom := errors.New("manifest close failure")
	for _, point := range []SimFaultPoint{SimBeforeOperation, SimAfterOperation} {
		t.Run(fmt.Sprintf("point-%d", point), func(t *testing.T) {
			const dir = "/manifest-close"
			fs, engine := setupCompactionScenario(t, dir, 5701+int64(point))
			if err := fs.SetErrorSchedule([]SimErrorDirective{{
				Op: FSOpClose, Occurrence: 2, Point: point, Err: boom,
			}}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if err := engine.compact(); !errors.Is(err, boom) {
				t.Fatalf("compact() error = %v, want close failure", err)
			}
			if engine.poisoned != nil {
				t.Fatalf("post-sync close failure poisoned safe state: %v", engine.poisoned)
			}
			if len(engine.ReferencedSSTables()) != 2 {
				t.Fatalf("post-sync close failure did not publish new version: %v", engine.ReferencedSSTables())
			}
			assertVictimDeleted(t, engine, 100)
			manifestBytes, err := fs.DurableBytes(filepath.Join(dir, manifestFilename))
			if err != nil {
				t.Fatal(err)
			}
			state, complete, torn, err := replayManifestBytes(manifestBytes)
			if err != nil || torn || complete != len(manifestBytes) || len(state.files) != 2 {
				t.Fatalf("durable new manifest = files:%d complete:%d torn:%v err:%v", len(state.files), complete, torn, err)
			}
			if err := engine.Close(); err != nil {
				t.Fatalf("retry-safe Close() = %v", err)
			}
			if len(engine.unresolved) != 0 || len(engine.obsolete) != 0 || engine.obsoleteDirDirty {
				t.Fatalf("Close left ownership unresolved: handles=%d obsolete=%d dirDirty=%v",
					len(engine.unresolved), len(engine.obsolete), engine.obsoleteDirDirty)
			}
		})
	}
}

func TestCompactionObsoleteHandleAndDirectoryRemovalRetries(t *testing.T) {
	boom := errors.New("obsolete cleanup failure")
	tests := []struct {
		name      string
		directive SimErrorDirective
	}{
		{
			name: "input close before effect",
			directive: SimErrorDirective{
				Op: FSOpClose, Occurrence: 3, Point: SimBeforeOperation, Err: boom,
			},
		},
		{
			name: "input close after effect",
			directive: SimErrorDirective{
				Op: FSOpClose, Occurrence: 3, Point: SimAfterOperation, Err: boom,
			},
		},
		{
			name: "remove before effect",
			directive: SimErrorDirective{
				Op: FSOpRemove, Occurrence: 1, Point: SimBeforeOperation, Err: boom,
			},
		},
		{
			name: "remove after effect",
			directive: SimErrorDirective{
				Op: FSOpRemove, Occurrence: 1, Point: SimAfterOperation, Err: boom,
			},
		},
		{
			name: "cleanup directory sync before effect",
			directive: SimErrorDirective{
				Op: FSOpDirSync, Occurrence: 2, Point: SimBeforeOperation, Err: boom,
			},
		},
		{
			name: "cleanup directory sync after effect",
			directive: SimErrorDirective{
				Op: FSOpDirSync, Occurrence: 2, Point: SimAfterOperation, Err: boom,
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const dir = "/cleanup"
			fs, engine := setupCompactionScenario(t, dir, int64(5800+index))
			if err := fs.SetErrorSchedule([]SimErrorDirective{test.directive}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if err := engine.compact(); !errors.Is(err, boom) {
				t.Fatalf("compact() error = %v, want cleanup failure", err)
			}
			if len(engine.ReferencedSSTables()) != 2 {
				t.Fatalf("cleanup failure exposed old version: %v", engine.ReferencedSSTables())
			}
			assertVictimDeleted(t, engine, 100)
			if err := engine.Close(); err != nil {
				t.Fatalf("retry Close() = %v", err)
			}
			if len(engine.obsolete) != 0 || engine.obsoleteDirDirty {
				t.Fatalf("retry did not discharge obsolete ownership: pending=%d dirty=%v", len(engine.obsolete), engine.obsoleteDirDirty)
			}

			// A second crash proves an after-effect Remove error was followed
			// by a durable directory reconciliation, rather than merely
			// disappearing from the live namespace.
			if err := fs.Crash(0); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			reopened := assertRecoveredCompactionState(t, fs, dir)
			if len(reopened.ReferencedSSTables()) != 2 {
				t.Fatalf("second-crash recovery reverted cleanup: %v", reopened.ReferencedSSTables())
			}
			assertNoSSTableOrphans(t, fs, dir, reopened.ReferencedSSTables())
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionCrashMatrixRecoversOldOrNewWithoutOrphans(t *testing.T) {
	tests := []struct {
		name      string
		directive SimCrashDirective
	}{
		{name: "create before", directive: SimCrashDirective{Op: FSOpCreate, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "create after", directive: SimCrashDirective{Op: FSOpCreate, Occurrence: 1, Point: SimAfterOperation, RetainUnsynced: 0}},
		{name: "partial output write", directive: SimCrashDirective{Op: FSOpWrite, Occurrence: 1, Point: SimAfterOperation, RetainUnsynced: 11}},
		{name: "output sync before partial retention", directive: SimCrashDirective{Op: FSOpFileSync, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 17}},
		{name: "output sync after", directive: SimCrashDirective{Op: FSOpFileSync, Occurrence: 1, Point: SimAfterOperation, RetainUnsynced: 0}},
		{name: "output close before", directive: SimCrashDirective{Op: FSOpClose, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "output directory sync before", directive: SimCrashDirective{Op: FSOpDirSync, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "output directory sync after", directive: SimCrashDirective{Op: FSOpDirSync, Occurrence: 1, Point: SimAfterOperation, RetainUnsynced: 0}},
		{name: "output stat before", directive: SimCrashDirective{Op: FSOpStat, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "output open after", directive: SimCrashDirective{Op: FSOpOpen, Occurrence: 1, Point: SimAfterOperation, RetainUnsynced: 0}},
		{name: "output validation read", directive: SimCrashDirective{Op: FSOpRead, Occurrence: 5, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "manifest open before", directive: SimCrashDirective{Op: FSOpOpenAppend, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "manifest write partial", directive: SimCrashDirective{Op: FSOpFileSync, Occurrence: 2, Point: SimBeforeOperation, RetainUnsynced: 9}},
		{name: "manifest write all retained", directive: SimCrashDirective{Op: FSOpFileSync, Occurrence: 2, Point: SimBeforeOperation, RetainUnsynced: RetainAllUnsynced}},
		{name: "manifest sync after before publish", directive: SimCrashDirective{Op: FSOpFileSync, Occurrence: 2, Point: SimAfterOperation, RetainUnsynced: 0}},
		{name: "manifest close after", directive: SimCrashDirective{Op: FSOpClose, Occurrence: 2, Point: SimAfterOperation, RetainUnsynced: 0}},
		{name: "publication before input close", directive: SimCrashDirective{Op: FSOpClose, Occurrence: 3, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "input remove before", directive: SimCrashDirective{Op: FSOpRemove, Occurrence: 1, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "input remove after", directive: SimCrashDirective{Op: FSOpRemove, Occurrence: 1, Point: SimAfterOperation, RetainUnsynced: 0}},
		{name: "cleanup directory sync before", directive: SimCrashDirective{Op: FSOpDirSync, Occurrence: 2, Point: SimBeforeOperation, RetainUnsynced: 0}},
		{name: "cleanup directory sync after", directive: SimCrashDirective{Op: FSOpDirSync, Occurrence: 2, Point: SimAfterOperation, RetainUnsynced: 0}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const dir = "/crash"
			fs, engine := setupCompactionScenario(t, dir, int64(5900+index))
			oldFiles, newFiles := expectedCompactionFileSets(t, engine)
			if err := fs.SetCrashSchedule([]SimCrashDirective{test.directive}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if err := engine.compact(); !errors.Is(err, ErrSimulatedCrash) {
				t.Fatalf("compact() error = %v, want ErrSimulatedCrash; events=%+v", err, fs.Events())
			}
			if !fs.Crashed() {
				t.Fatal("scheduled crash did not crash SimFS")
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			reopened := assertRecoveredCompactionState(t, fs, dir)
			references := reopened.ReferencedSSTables()
			if !reflect.DeepEqual(references, oldFiles) && !reflect.DeepEqual(references, newFiles) {
				t.Fatalf("recovery mixed version files = %v, old=%v new=%v", references, oldFiles, newFiles)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fs.Crash(0); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			second := assertRecoveredCompactionState(t, fs, dir)
			if got := second.ReferencedSSTables(); !reflect.DeepEqual(got, references) {
				t.Fatalf("second crash changed recovered version: first=%v second=%v", references, got)
			}
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionOrdinaryFailureMatrixRecoversConsistently(t *testing.T) {
	boom := errors.New("compaction boundary failure")
	tests := []struct {
		name      string
		directive SimErrorDirective
	}{
		{name: "create before", directive: SimErrorDirective{Op: FSOpCreate, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "create after", directive: SimErrorDirective{Op: FSOpCreate, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "output write before", directive: SimErrorDirective{Op: FSOpWrite, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "output write after", directive: SimErrorDirective{Op: FSOpWrite, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "output sync before", directive: SimErrorDirective{Op: FSOpFileSync, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "output sync after", directive: SimErrorDirective{Op: FSOpFileSync, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "output close before", directive: SimErrorDirective{Op: FSOpClose, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "output close after", directive: SimErrorDirective{Op: FSOpClose, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "output directory sync before", directive: SimErrorDirective{Op: FSOpDirSync, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "output directory sync after", directive: SimErrorDirective{Op: FSOpDirSync, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "output stat before", directive: SimErrorDirective{Op: FSOpStat, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "output open after", directive: SimErrorDirective{Op: FSOpOpen, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "output validation read", directive: SimErrorDirective{Op: FSOpRead, Occurrence: 5, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest open before", directive: SimErrorDirective{Op: FSOpOpenAppend, Occurrence: 1, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest open after", directive: SimErrorDirective{Op: FSOpOpenAppend, Occurrence: 1, Point: SimAfterOperation, Err: boom}},
		{name: "manifest write before uncertain", directive: SimErrorDirective{Op: FSOpWrite, Occurrence: 2, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest write after uncertain", directive: SimErrorDirective{Op: FSOpWrite, Occurrence: 2, Point: SimAfterOperation, Err: boom}},
		{name: "manifest sync before uncertain", directive: SimErrorDirective{Op: FSOpFileSync, Occurrence: 2, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest sync after uncertain", directive: SimErrorDirective{Op: FSOpFileSync, Occurrence: 2, Point: SimAfterOperation, Err: boom}},
		{name: "manifest close before committed", directive: SimErrorDirective{Op: FSOpClose, Occurrence: 2, Point: SimBeforeOperation, Err: boom}},
		{name: "manifest close after committed", directive: SimErrorDirective{Op: FSOpClose, Occurrence: 2, Point: SimAfterOperation, Err: boom}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const dir = "/failure"
			fs, engine := setupCompactionScenario(t, dir, int64(6200+index))
			oldFiles, newFiles := expectedCompactionFileSets(t, engine)
			if err := fs.SetErrorSchedule([]SimErrorDirective{test.directive}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			compactErr := engine.compact()
			matched := errors.Is(compactErr, boom)
			if test.directive.Op == FSOpRead && compactErr != nil {
				matched = strings.Contains(compactErr.Error(), boom.Error())
			}
			if !matched {
				t.Fatalf("compact() error = %v, want injected failure; events=%+v", compactErr, fs.Events())
			}
			assertVictimDeleted(t, engine, 100)
			if err := engine.Close(); err != nil {
				t.Fatalf("Close() after boundary failure = %v", err)
			}
			if len(engine.unresolved) != 0 || len(engine.obsolete) != 0 || engine.obsoleteDirDirty {
				t.Fatalf("Close left ownership unresolved: handles=%d obsolete=%d dirty=%v",
					len(engine.unresolved), len(engine.obsolete), engine.obsoleteDirDirty)
			}
			if err := fs.Crash(0); err != nil {
				t.Fatal(err)
			}
			if err := fs.Recover(); err != nil {
				t.Fatal(err)
			}
			reopened := assertRecoveredCompactionState(t, fs, dir)
			if got := reopened.ReferencedSSTables(); !reflect.DeepEqual(got, oldFiles) && !reflect.DeepEqual(got, newFiles) {
				t.Fatalf("failure recovery mixed files = %v, old=%v new=%v", got, oldFiles, newFiles)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionFailedOpenRetainsHandleUntilRetry(t *testing.T) {
	readBoom := errors.New("validation read failure")
	closeBoom := errors.New("validation handle close failure")
	const dir = "/failed-open"
	fs, engine := setupCompactionScenario(t, dir, 6301)
	if err := fs.SetErrorSchedule([]SimErrorDirective{
		{Op: FSOpRead, Occurrence: 5, Point: SimBeforeOperation, Err: readBoom},
		{Op: FSOpClose, Occurrence: 2, Point: SimBeforeOperation, Err: closeBoom},
	}); err != nil {
		t.Fatal(err)
	}
	fs.ResetEvents()
	err := engine.compact()
	if !strings.Contains(err.Error(), readBoom.Error()) || !errors.Is(err, closeBoom) {
		t.Fatalf("compact() error = %v, want read and close failures", err)
	}
	if len(engine.obsolete) != 1 || engine.obsolete[0].file == nil {
		t.Fatalf("failed-open ownership = %+v, want one retained handle", engine.obsolete)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("retry Close() = %v", err)
	}
	if len(engine.obsolete) != 0 || engine.obsoleteDirDirty {
		t.Fatalf("retry did not close/remove failed output: pending=%d dirty=%v", len(engine.obsolete), engine.obsoleteDirDirty)
	}
}

func TestCompactionInputReadFailureKeepsOldVersionAndReconcilesOutput(t *testing.T) {
	boom := errors.New("selected input read failure")
	const dir = "/input-read-failure"
	fs, engine := setupCompactionScenario(t, dir, 6321)
	before := engine.ReferencedSSTables()
	if err := fs.SetErrorSchedule([]SimErrorDirective{{
		Op: FSOpRead, Occurrence: 1, Point: SimBeforeOperation, Err: boom,
	}}); err != nil {
		t.Fatal(err)
	}
	fs.ResetEvents()
	err := engine.compact()
	if err == nil || !strings.Contains(err.Error(), boom.Error()) {
		t.Fatalf("compact() error = %v, want selected-input read failure", err)
	}
	if got := engine.ReferencedSSTables(); !reflect.DeepEqual(got, before) {
		t.Fatalf("input read failure published %v, want old %v", got, before)
	}
	events := fs.Events()
	var outputCreated, outputClosed, outputRemoved, cleanupSynced bool
	for _, event := range events {
		switch {
		case event.Completed && event.Op == FSOpCreate && strings.HasSuffix(event.Path, sstableFilenameSuffix):
			outputCreated = true
		case event.Completed && event.Op == FSOpClose && strings.HasSuffix(event.Path, sstableFilenameSuffix):
			outputClosed = true
		case event.Completed && event.Op == FSOpRemove && strings.HasSuffix(event.Path, sstableFilenameSuffix):
			outputRemoved = true
		case event.Completed && event.Op == FSOpDirSync && event.Path == dir:
			cleanupSynced = true
		case event.Op == FSOpOpenAppend || (event.Op == FSOpWrite && strings.HasSuffix(event.Path, manifestFilename)):
			t.Fatalf("input read failure reached MANIFEST: %+v", event)
		}
	}
	if !outputCreated || !outputClosed || !outputRemoved || !cleanupSynced {
		t.Fatalf("input read cleanup = create:%v close:%v remove:%v dir-sync:%v events=%+v",
			outputCreated, outputClosed, outputRemoved, cleanupSynced, events)
	}
	assertNoSSTableOrphans(t, fs, dir, before)
	assertVictimDeleted(t, engine, 100)
}

func TestFlushPostSyncCloseFailureRetainsHandleUntilClose(t *testing.T) {
	boom := errors.New("flush output close failure")
	for _, point := range []SimFaultPoint{SimBeforeOperation, SimAfterOperation} {
		t.Run(fmt.Sprintf("point-%d", point), func(t *testing.T) {
			fs := NewSimFS()
			engine, err := Open("/flush-close", Options{FS: fs, Rand: newSeededRand(6351 + int64(point))})
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Put([]byte("key"), []byte("value"), 1); err != nil {
				t.Fatal(err)
			}
			if err := fs.SetErrorSchedule([]SimErrorDirective{{
				Op: FSOpClose, Occurrence: 1, Point: point, Err: boom,
			}}); err != nil {
				t.Fatal(err)
			}
			fs.ResetEvents()
			if err := engine.ForceFlush(); !errors.Is(err, boom) {
				t.Fatalf("ForceFlush() error = %v, want close failure", err)
			}
			if len(engine.unresolved) != 1 {
				t.Fatalf("unresolved handles = %d, want retained flush output", len(engine.unresolved))
			}
			if err := engine.Close(); err != nil {
				t.Fatalf("Close() retry = %v", err)
			}
			if len(engine.unresolved) != 0 {
				t.Fatalf("Close retained %d handles", len(engine.unresolved))
			}
		})
	}
}

func TestCompactionScheduleAndFinalStateAreByteDeterministic(t *testing.T) {
	type result struct {
		events   []FSEvent
		files    []string
		manifest []byte
		sstables [][]byte
	}
	run := func() result {
		const dir = "/deterministic"
		fs, engine := setupCompactionScenario(t, dir, 6001)
		if err := fs.SetCrashSchedule([]SimCrashDirective{{
			Op: FSOpFileSync, Occurrence: 2, Point: SimBeforeOperation, RetainUnsynced: 13,
		}}); err != nil {
			t.Fatal(err)
		}
		fs.ResetEvents()
		if err := engine.compact(); !errors.Is(err, ErrSimulatedCrash) {
			t.Fatalf("compact() error = %v", err)
		}
		events := fs.Events()
		if err := fs.Recover(); err != nil {
			t.Fatal(err)
		}
		reopened := assertRecoveredCompactionState(t, fs, dir)
		files := reopened.ReferencedSSTables()
		manifest, err := fs.DurableBytes(filepath.Join(dir, manifestFilename))
		if err != nil {
			t.Fatal(err)
		}
		var tables [][]byte
		for _, name := range files {
			data, err := fs.DurableBytes(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			tables = append(tables, data)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
		return result{events: events, files: files, manifest: manifest, sstables: tables}
	}
	first, second := run(), run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("identical schedule diverged:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func setupCompactionScenario(t *testing.T, dir string, seed int64) (*SimFS, *Engine) {
	t.Helper()
	fs := NewSimFS()
	engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(seed)})
	if err != nil {
		t.Fatal(err)
	}
	installTestTable(t, engine, 1, []TableEntry{{Key: []byte("victim"), Seq: 10, Value: []byte("outside-old-live")}})
	installTestTable(t, engine, 0, []TableEntry{
		{Key: []byte("key-0"), Seq: 20, Value: []byte("value-0")},
		{Key: []byte("victim"), Seq: 100, Tombstone: true},
	})
	for index := 1; index < 4; index++ {
		installTestTable(t, engine, 0, []TableEntry{{
			Key: []byte(fmt.Sprintf("key-%d", index)), Seq: uint64(20 + index), Value: []byte(fmt.Sprintf("value-%d", index)),
		}})
	}
	return fs, engine
}

func expectedCompactionFileSets(t *testing.T, engine *Engine) (oldFiles, newFiles []string) {
	t.Helper()
	oldFiles = engine.ReferencedSSTables()
	_, selected, found := selectCompactionInputs(engine.tables)
	if !found {
		t.Fatal("expected eligible compaction inputs")
	}
	selectedNames := make(map[string]struct{}, len(selected))
	for _, table := range selected {
		selectedNames[table.metadata.file] = struct{}{}
	}
	for _, name := range oldFiles {
		if _, removed := selectedNames[name]; !removed {
			newFiles = append(newFiles, name)
		}
	}
	newFiles = append(newFiles, fmt.Sprintf("%0*d%s", defaultFileNumberWidth, engine.nextFileNumber, sstableFilenameSuffix))
	slices.Sort(newFiles)
	return oldFiles, newFiles
}

func assertRecoveredCompactionState(t *testing.T, fs *SimFS, dir string) *Engine {
	t.Helper()
	reopened, err := Open(dir, Options{FS: fs, Rand: newSeededRand(6101)})
	if err != nil {
		t.Fatalf("Open() after compaction fault = %v", err)
	}
	assertVictimDeleted(t, reopened, 100)
	for index := 0; index < 4; index++ {
		key := []byte(fmt.Sprintf("key-%d", index))
		want := fmt.Sprintf("value-%d", index)
		if value, found, err := reopened.Read(key); err != nil || !found || string(value) != want {
			t.Fatalf("reopened Read(%q) = %q,%v,%v, want %q", key, value, found, err, want)
		}
	}
	references := reopened.ReferencedSSTables()
	for _, name := range references {
		data, err := fs.DurableBytes(filepath.Join(dir, name))
		if err != nil || len(data) == 0 {
			t.Fatalf("referenced SSTable %s is not durable: bytes=%d err=%v", name, len(data), err)
		}
	}
	assertNoSSTableOrphans(t, fs, dir, references)
	manifestBytes, err := fs.DurableBytes(filepath.Join(dir, manifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	state, complete, torn, err := replayManifestBytes(manifestBytes)
	if err != nil || torn || complete != len(manifestBytes) || len(state.files) != len(references) {
		t.Fatalf("recovered manifest = files:%d refs:%d complete:%d/%d torn:%v err:%v",
			len(state.files), len(references), complete, len(manifestBytes), torn, err)
	}
	return reopened
}

func selectedNames(tables []*tableHandle) []string {
	names := make([]string, len(tables))
	for index, table := range tables {
		names[index] = table.metadata.file
	}
	return names
}

func mergedSSTableBytes(t *testing.T, entries [][]TableEntry, permutation []int) []byte {
	t.Helper()
	inputs := make([]tableEntryIterator, 0, len(permutation))
	for _, index := range permutation {
		inputs = append(inputs, &sliceTableIterator{entries: entries[index]})
	}
	merge, err := newKWayMergeIterator(inputs)
	if err != nil {
		t.Fatal(err)
	}
	var table bytes.Buffer
	writer := NewSSTableWriter(&table)
	for {
		entry, found, err := merge.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		if err := writer.Add(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), table.Bytes()...)
}

func installTestTable(t *testing.T, engine *Engine, tier uint32, entries []TableEntry) *tableHandle {
	t.Helper()
	entries = append([]TableEntry(nil), entries...)
	slices.SortFunc(entries, func(left, right TableEntry) int {
		if cmp := bytes.Compare(left.Key, right.Key); cmp != 0 {
			return cmp
		}
		switch {
		case left.Seq > right.Seq:
			return -1
		case left.Seq < right.Seq:
			return 1
		default:
			return 0
		}
	})
	if len(entries) == 0 {
		t.Fatal("installTestTable requires at least one entry")
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	filename, err := engine.allocateFilenameLocked()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(engine.dir, filename)
	file, err := engine.fs.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := NewSSTableWriter(file)
	maxSeq := engine.manifest.flushedIndex
	for _, entry := range entries {
		if err := writer.Add(entry); err != nil {
			t.Fatal(err)
		}
		maxSeq = max(maxSeq, entry.Seq)
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
	if err := engine.fs.SyncDir(engine.dir); err != nil {
		t.Fatal(err)
	}
	metadata := fileMetadata{
		tier:   tier,
		file:   filename,
		minKey: cloneBytes(entries[0].Key),
		maxKey: cloneBytes(entries[len(entries)-1].Key),
		count:  uint64(len(entries)),
	}
	table, err := engine.openTable(metadata, maxSeq)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := engine.commitEditLocked(&raftpb.VersionEdit{
		AddedFiles:   []*raftpb.AddedFile{metadata.proto()},
		FlushedIndex: maxSeq,
	}, table)
	if err != nil || !committed {
		t.Fatalf("install manifest edit = committed:%v err:%v", committed, err)
	}
	return table
}

func assertVictimDeleted(t *testing.T, engine *Engine, wantSeq uint64) {
	t.Helper()
	if value, found, err := engine.Read([]byte("victim")); err != nil || found || value != nil {
		t.Fatalf("Read(victim) = %q,%v,%v, want not found", value, found, err)
	}
	if _, tombstone, seq, found, err := engine.Lookup([]byte("victim")); err != nil || !found || !tombstone || seq != wantSeq {
		t.Fatalf("Lookup(victim) = tomb:%v seq:%d found:%v err:%v", tombstone, seq, found, err)
	}
}

func assertPublishedTombstone(t *testing.T, engine *Engine, key string, wantSeq uint64) {
	t.Helper()
	for _, table := range engine.tables {
		entries, err := table.reader.AllEntries()
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if string(entry.Key) == key && entry.Tombstone && entry.Seq == wantSeq {
				return
			}
		}
	}
	t.Fatalf("no published tombstone for %q at seq %d", key, wantSeq)
}

func decodeManifestEditsForTest(t *testing.T, data []byte) []*raftpb.VersionEdit {
	t.Helper()
	var edits []*raftpb.VersionEdit
	for offset := 0; offset < len(data); {
		if data[offset] != manifestFrameStart {
			t.Fatalf("manifest byte %d = 0x%02x, want frame start", offset, data[offset])
		}
		end := bytes.IndexByte(data[offset+1:], manifestFrameEnd)
		if end < 0 {
			t.Fatalf("manifest frame at %d is torn", offset)
		}
		end += offset + 1
		edit, err := decodeManifestFrame(data[offset+1 : end])
		if err != nil {
			t.Fatal(err)
		}
		edits = append(edits, edit)
		offset = end + 1
	}
	return edits
}

func assertNoSSTableOrphans(t *testing.T, fs *SimFS, dir string, references []string) {
	t.Helper()
	names, err := fs.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for _, name := range names {
		if strings.HasSuffix(name, sstableFilenameSuffix) {
			tables = append(tables, name)
		}
	}
	slices.Sort(tables)
	want := append([]string(nil), references...)
	slices.Sort(want)
	if !reflect.DeepEqual(tables, want) {
		t.Fatalf("SSTables on disk = %v, references = %v", tables, want)
	}
}
