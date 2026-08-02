package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"

	raftpb "miniquorum/proto"
)

const (
	compactionFanIn    = 4
	sizeClassGrowth    = int64(4)
	sizeClassBaseBytes = int64(dataBlockTarget)
)

// tableSizeClass is the deterministic Packet 5D size-class rule. Class zero
// contains files smaller than one 4 KiB data-block target. Every following
// half-open class begins at B*4^n and is four times as wide:
//
//	0: [0, B)
//	1: [B, 4B)
//	2: [4B, 16B)
//
// A freshly flushed table is classified once from the exact bytes emitted by
// SSTableWriter and that tier is persisted in MANIFEST. Recovery and input
// selection trust the persisted tier rather than reclassifying from Stat, so
// a compacted table remains in its mandated next logical class even when
// duplicate removal makes its physical bytes smaller. The final int64 class
// saturates without multiplying past MaxInt64.
func tableSizeClass(size int64) uint32 {
	if size < sizeClassBaseBytes {
		return 0
	}
	class := uint32(1)
	boundary := sizeClassBaseBytes * sizeClassGrowth
	for size >= boundary {
		class++
		if boundary > math.MaxInt64/sizeClassGrowth {
			return class
		}
		boundary *= sizeClassGrowth
	}
	return class
}

type tableEntryIterator interface {
	Next() (TableEntry, bool, error)
}

// sstableEntryIterator streams one validated data block at a time. It does
// not materialize a whole input table during compaction.
type sstableEntryIterator struct {
	reader      *SSTableReader
	block       int
	entries     []TableEntry
	entry       int
	seen        uint64
	haveFirst   bool
	firstKey    []byte
	haveLastKey bool
	lastKey     []byte
	done        bool
}

func newSSTableEntryIterator(reader *SSTableReader) *sstableEntryIterator {
	return &sstableEntryIterator{reader: reader}
}

func (iterator *sstableEntryIterator) Next() (TableEntry, bool, error) {
	if iterator == nil || iterator.reader == nil {
		return TableEntry{}, false, errors.New("lsm: nil SSTable iterator")
	}
	if iterator.done {
		return TableEntry{}, false, nil
	}
	for iterator.entry == len(iterator.entries) {
		if iterator.block == len(iterator.reader.index) {
			iterator.done = true
			if iterator.seen != iterator.reader.entryCount {
				return TableEntry{}, false, corruptf("iterator entry count is %d, footer says %d", iterator.seen, iterator.reader.entryCount)
			}
			if iterator.seen != 0 && (!bytes.Equal(iterator.firstKey, iterator.reader.minKey) || !bytes.Equal(iterator.lastKey, iterator.reader.maxKey)) {
				return TableEntry{}, false, corruptf("iterator key bounds do not match footer")
			}
			return TableEntry{}, false, nil
		}

		index := iterator.reader.index[iterator.block]
		data, err := readBlock(iterator.reader.r, index.offset, index.length)
		if err != nil {
			return TableEntry{}, false, err
		}
		entries, err := decodeRecords(data)
		if err != nil {
			return TableEntry{}, false, err
		}
		if len(entries) == 0 || !bytes.Equal(entries[0].Key, index.firstKey) {
			return TableEntry{}, false, corruptf("index first key does not match data block")
		}
		if iterator.haveLastKey && bytes.Compare(entries[0].Key, iterator.lastKey) <= 0 {
			return TableEntry{}, false, corruptf("records out of order or same key spans data blocks")
		}
		for _, entry := range entries {
			if !iterator.reader.bloom.MayContain(entry.Key) {
				return TableEntry{}, false, corruptf("bloom false negative for stored key")
			}
		}
		if !iterator.haveFirst {
			iterator.firstKey = cloneBytes(entries[0].Key)
			iterator.haveFirst = true
		}
		iterator.lastKey = cloneBytes(entries[len(entries)-1].Key)
		iterator.haveLastKey = true
		iterator.entries = entries
		iterator.entry = 0
		iterator.block++
	}

	entry := iterator.entries[iterator.entry]
	iterator.entry++
	iterator.seen++
	if iterator.seen > iterator.reader.entryCount {
		return TableEntry{}, false, corruptf("iterator produced more records than footer count")
	}
	return cloneTableEntry(entry), true, nil
}

type mergeHeapItem struct {
	entry TableEntry
	input int
}

// kWayMergeIterator uses a hand-built binary min-heap. Next drains every
// version of the smallest key across all inputs and emits exactly its
// highest-sequence record, including tombstones.
type kWayMergeIterator struct {
	inputs []tableEntryIterator
	heap   []mergeHeapItem
}

func newKWayMergeIterator(inputs []tableEntryIterator) (*kWayMergeIterator, error) {
	iterator := &kWayMergeIterator{inputs: append([]tableEntryIterator(nil), inputs...)}
	for input, source := range iterator.inputs {
		if source == nil {
			return nil, fmt.Errorf("lsm: nil merge input %d", input)
		}
		entry, found, err := source.Next()
		if err != nil {
			return nil, fmt.Errorf("lsm: initialize merge input %d: %w", input, err)
		}
		if found {
			iterator.push(mergeHeapItem{entry: entry, input: input})
		}
	}
	return iterator, nil
}

func (iterator *kWayMergeIterator) Next() (TableEntry, bool, error) {
	if iterator == nil {
		return TableEntry{}, false, errors.New("lsm: nil merge iterator")
	}
	if len(iterator.heap) == 0 {
		return TableEntry{}, false, nil
	}

	key := cloneBytes(iterator.heap[0].entry.Key)
	var winner TableEntry
	haveWinner := false
	versions := make(map[uint64]TableEntry)
	for len(iterator.heap) != 0 && bytes.Equal(iterator.heap[0].entry.Key, key) {
		item := iterator.pop()
		candidate := item.entry
		if prior, exists := versions[candidate.Seq]; exists {
			if !sameTableRecord(candidate, prior) {
				return TableEntry{}, false, fmt.Errorf("%w: conflicting versions for key %x at seq %d", ErrCorrupt, key, candidate.Seq)
			}
		} else {
			versions[candidate.Seq] = cloneTableEntry(candidate)
		}
		switch {
		case !haveWinner || candidate.Seq > winner.Seq:
			winner = cloneTableEntry(candidate)
			haveWinner = true
		}

		next, found, err := iterator.inputs[item.input].Next()
		if err != nil {
			return TableEntry{}, false, fmt.Errorf("lsm: advance merge input %d: %w", item.input, err)
		}
		if found {
			iterator.push(mergeHeapItem{entry: next, input: item.input})
		}
	}
	return winner, true, nil
}

func sameTableRecord(left, right TableEntry) bool {
	return left.Tombstone == right.Tombstone && (left.Tombstone || bytes.Equal(left.Value, right.Value))
}

func cloneTableEntry(entry TableEntry) TableEntry {
	return TableEntry{
		Key:       cloneBytes(entry.Key),
		Seq:       entry.Seq,
		Value:     cloneBytes(entry.Value),
		Tombstone: entry.Tombstone,
	}
}

func (iterator *kWayMergeIterator) push(item mergeHeapItem) {
	iterator.heap = append(iterator.heap, item)
	index := len(iterator.heap) - 1
	for index != 0 {
		parent := (index - 1) / 2
		if !mergeHeapLess(iterator.heap[index], iterator.heap[parent]) {
			break
		}
		iterator.heap[index], iterator.heap[parent] = iterator.heap[parent], iterator.heap[index]
		index = parent
	}
}

func (iterator *kWayMergeIterator) pop() mergeHeapItem {
	item := iterator.heap[0]
	last := len(iterator.heap) - 1
	iterator.heap[0] = iterator.heap[last]
	iterator.heap = iterator.heap[:last]
	for index := 0; index < len(iterator.heap); {
		left := index*2 + 1
		if left >= len(iterator.heap) {
			break
		}
		smallest := left
		right := left + 1
		if right < len(iterator.heap) && mergeHeapLess(iterator.heap[right], iterator.heap[left]) {
			smallest = right
		}
		if !mergeHeapLess(iterator.heap[smallest], iterator.heap[index]) {
			break
		}
		iterator.heap[index], iterator.heap[smallest] = iterator.heap[smallest], iterator.heap[index]
		index = smallest
	}
	return item
}

func mergeHeapLess(left, right mergeHeapItem) bool {
	if cmp := bytes.Compare(left.entry.Key, right.entry.Key); cmp != 0 {
		return cmp < 0
	}
	if left.entry.Seq != right.entry.Seq {
		return left.entry.Seq > right.entry.Seq
	}
	return left.input < right.input
}

type compactionStage uint8

const (
	compactionBeforePublish compactionStage = iota + 1
	compactionAfterPublish
)

type obsoleteFile struct {
	path string
	file File
}

// compact is the package-private deterministic test/scheduler seam. Normal
// operation invokes the same path synchronously after every successful flush.
func (engine *Engine) compact() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if err := engine.checkOpenLocked(); err != nil {
		return err
	}
	return engine.compactAllLocked()
}

func (engine *Engine) compactAllLocked() error {
	if err := engine.reconcileObsoleteLocked(); err != nil {
		return err
	}
	for {
		tier, inputs, found := selectCompactionInputs(engine.tables)
		if !found {
			return nil
		}
		if tier == math.MaxUint32 {
			return errors.New("lsm: compaction tier exhausted")
		}
		if err := engine.compactInputsLocked(tier, inputs); err != nil {
			return err
		}
	}
}

// selectCompactionInputs chooses the lowest eligible persisted tier and then
// its four lexicographically smallest filenames. It is independent of table
// slice order, map iteration, creation-time observations, and key ranges.
func selectCompactionInputs(tables []*tableHandle) (uint32, []*tableHandle, bool) {
	byTier := make(map[uint32][]*tableHandle)
	for _, table := range tables {
		if table == nil {
			continue
		}
		byTier[table.metadata.tier] = append(byTier[table.metadata.tier], table)
	}
	tiers := make([]uint32, 0, len(byTier))
	for tier, candidates := range byTier {
		if len(candidates) >= compactionFanIn {
			tiers = append(tiers, tier)
		}
	}
	if len(tiers) == 0 {
		return 0, nil, false
	}
	slices.Sort(tiers)
	tier := tiers[0]
	candidates := byTier[tier]
	slices.SortFunc(candidates, func(left, right *tableHandle) int {
		return bytes.Compare([]byte(left.metadata.file), []byte(right.metadata.file))
	})
	return tier, append([]*tableHandle(nil), candidates[:compactionFanIn]...), true
}

func (engine *Engine) compactInputsLocked(tier uint32, inputs []*tableHandle) error {
	if len(inputs) != compactionFanIn {
		return fmt.Errorf("lsm: compaction selected %d inputs, want %d", len(inputs), compactionFanIn)
	}
	inputs = append([]*tableHandle(nil), inputs...)
	slices.SortFunc(inputs, func(left, right *tableHandle) int {
		if left == nil && right == nil {
			return 0
		}
		if left == nil {
			return -1
		}
		if right == nil {
			return 1
		}
		return bytes.Compare([]byte(left.metadata.file), []byte(right.metadata.file))
	})
	selected := make(map[*tableHandle]struct{}, len(inputs))
	for _, input := range inputs {
		if input == nil || input.metadata.tier != tier {
			return errors.New("lsm: compaction inputs do not share selected tier")
		}
		if _, duplicate := selected[input]; duplicate {
			return errors.New("lsm: duplicate compaction input")
		}
		selected[input] = struct{}{}
	}
	for _, input := range inputs {
		present := false
		for _, table := range engine.tables {
			if table == input {
				present = true
				break
			}
		}
		if !present {
			return fmt.Errorf("lsm: selected input %s is not published", input.metadata.file)
		}
	}

	output, err := engine.buildCompactionOutputLocked(tier+1, inputs)
	if err != nil {
		return err
	}
	removed := make([]string, len(inputs))
	for index, input := range inputs {
		removed[index] = input.metadata.file
	}
	edit := &raftpb.VersionEdit{
		AddedFiles:   []*raftpb.AddedFile{output.metadata.proto()},
		RemovedFiles: removed,
		FlushedIndex: engine.manifest.flushedIndex,
	}
	return engine.commitCompactionLocked(edit, output, selected)
}

func (engine *Engine) buildCompactionOutputLocked(tier uint32, inputs []*tableHandle) (*tableHandle, error) {
	filename, err := engine.allocateFilenameLocked()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(engine.dir, filename)
	file, err := engine.fs.Create(path)
	if err != nil {
		return nil, engine.discardCompactionOutputLocked(path, nil, fmt.Errorf("lsm: create compaction SSTable %s: %w", filename, err))
	}

	sources := make([]tableEntryIterator, len(inputs))
	for index, input := range inputs {
		sources[index] = newSSTableEntryIterator(input.reader)
	}
	merge, err := newKWayMergeIterator(sources)
	if err != nil {
		return nil, engine.discardCompactionOutputLocked(path, file, err)
	}
	writer := NewSSTableWriter(file)
	var firstKey, lastKey []byte
	count := uint64(0)
	for {
		entry, found, nextErr := merge.Next()
		if nextErr != nil {
			return nil, engine.discardCompactionOutputLocked(path, file, nextErr)
		}
		if !found {
			break
		}
		if count == 0 {
			firstKey = cloneBytes(entry.Key)
		}
		lastKey = cloneBytes(entry.Key)
		count++
		if err := writer.Add(entry); err != nil {
			return nil, engine.discardCompactionOutputLocked(path, file, fmt.Errorf("lsm: add compaction output %s: %w", filename, err))
		}
	}
	if count == 0 {
		return nil, engine.discardCompactionOutputLocked(path, file, errors.New("lsm: compaction produced an empty table"))
	}
	if err := writer.Finish(); err != nil {
		return nil, engine.discardCompactionOutputLocked(path, file, fmt.Errorf("lsm: finish compaction SSTable %s: %w", filename, err))
	}
	if err := file.Sync(); err != nil {
		return nil, engine.discardCompactionOutputLocked(path, file, fmt.Errorf("lsm: sync compaction SSTable %s: %w", filename, err))
	}
	if err := file.Close(); err != nil {
		return nil, engine.discardCompactionOutputLocked(path, file, fmt.Errorf("lsm: close compaction SSTable %s: %w", filename, err))
	}
	if err := engine.fs.SyncDir(engine.dir); err != nil {
		return nil, engine.discardCompactionOutputLocked(path, nil, fmt.Errorf("lsm: sync compaction SSTable directory for %s: %w", filename, err))
	}

	metadata := fileMetadata{tier: tier, file: filename, minKey: firstKey, maxKey: lastKey, count: count}
	table, err := engine.openTableOwned(metadata, engine.manifest.flushedIndex)
	if err != nil {
		var owned File
		if table != nil {
			owned = table.file
		}
		return nil, engine.discardCompactionOutputLocked(path, owned, err)
	}
	return table, nil
}

func (engine *Engine) commitCompactionLocked(edit *raftpb.VersionEdit, output *tableHandle, selected map[*tableHandle]struct{}) error {
	next := manifestState{
		files:        make(map[string]fileMetadata, len(engine.manifest.files)+1),
		flushedIndex: engine.manifest.flushedIndex,
		records:      engine.manifest.records,
	}
	for name, metadata := range engine.manifest.files {
		next.files[name] = metadata
	}
	if err := next.apply(edit); err != nil {
		return engine.discardCompactionOutputLocked(filepath.Join(engine.dir, output.metadata.file), output.file, fmt.Errorf("lsm: invalid compaction manifest edit: %w", err))
	}

	committed, unresolved, appendErr := appendManifestEdit(engine.fs, engine.dir, edit)
	if unresolved != nil {
		engine.unresolved = append(engine.unresolved, unresolved)
	}
	if !committed {
		if isUncertainManifestAppend(appendErr) {
			engine.poisonLocked(appendErr)
			closeErr := output.file.Close()
			if closeErr != nil && !errors.Is(closeErr, ErrFSClosed) {
				engine.unresolved = append(engine.unresolved, output.file)
			}
			return errors.Join(appendErr, closeErr)
		}
		return engine.discardCompactionOutputLocked(filepath.Join(engine.dir, output.metadata.file), output.file, appendErr)
	}

	if engine.compactionHook != nil {
		engine.compactionHook(compactionBeforePublish)
	}
	published := make([]*tableHandle, 0, len(engine.tables)-len(selected)+1)
	for _, table := range engine.tables {
		if _, remove := selected[table]; remove {
			continue
		}
		published = append(published, table)
	}
	published = append(published, output)
	slices.SortFunc(published, func(left, right *tableHandle) int {
		return bytes.Compare([]byte(left.metadata.file), []byte(right.metadata.file))
	})
	engine.manifest = next
	engine.tables = published
	obsoleteInputs := make([]*tableHandle, 0, len(selected))
	for table := range selected {
		obsoleteInputs = append(obsoleteInputs, table)
	}
	slices.SortFunc(obsoleteInputs, func(left, right *tableHandle) int {
		return bytes.Compare([]byte(left.metadata.file), []byte(right.metadata.file))
	})
	for _, table := range obsoleteInputs {
		engine.obsolete = append(engine.obsolete, obsoleteFile{
			path: filepath.Join(engine.dir, table.metadata.file),
			file: table.file,
		})
	}
	if engine.compactionHook != nil {
		engine.compactionHook(compactionAfterPublish)
	}
	engine.completedCompactions++
	cleanupErr := engine.reconcileObsoleteLocked()
	return errors.Join(appendErr, cleanupErr)
}

func (engine *Engine) discardCompactionOutputLocked(path string, file File, cause error) error {
	engine.obsolete = append(engine.obsolete, obsoleteFile{path: path, file: file})
	return errors.Join(cause, engine.reconcileObsoleteLocked())
}

// reconcileObsoleteLocked closes every retained handle before removing its
// now-unreferenced name. Failed closes and removes retain ownership for a
// later Close/retry. Once any removal succeeds, the directory remains marked
// dirty until SyncDir succeeds.
func (engine *Engine) reconcileObsoleteLocked() error {
	if engine.fs == nil {
		return nil
	}
	var combined error
	pending := make([]obsoleteFile, 0, len(engine.obsolete))
	for _, obsolete := range engine.obsolete {
		if obsolete.file != nil {
			closeErr := obsolete.file.Close()
			if closeErr != nil && !errors.Is(closeErr, ErrFSClosed) {
				combined = errors.Join(combined, closeErr)
				pending = append(pending, obsolete)
				continue
			}
			obsolete.file = nil
		}
		// The generic FS contract cannot distinguish a before-effect error
		// from an after-effect error. Once Remove is attempted, conservatively
		// require a directory sync before clearing the durability obligation.
		engine.obsoleteDirDirty = true
		removeErr := engine.fs.Remove(obsolete.path)
		switch {
		case removeErr == nil:
		case errors.Is(removeErr, ErrNotExist):
			// The name never became visible, was already reconciled, or was
			// discarded by a crash. The conservative directory sync above is
			// harmless and also discharges any prior uncertain removal.
		default:
			combined = errors.Join(combined, removeErr)
			pending = append(pending, obsolete)
		}
	}
	engine.obsolete = pending
	if engine.obsoleteDirDirty {
		if err := engine.fs.SyncDir(engine.dir); err != nil {
			combined = errors.Join(combined, err)
		} else {
			engine.obsoleteDirDirty = false
		}
	}
	return combined
}
