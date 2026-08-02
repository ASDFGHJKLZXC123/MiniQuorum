package lsm

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
)

// RetainAllUnsynced asks SimFS to retain every dirty byte at a crash. Any
// non-negative value retains that many bytes from the deterministic global
// dirty-write prefix.
const RetainAllUnsynced = -1

// SimFaultPoint selects whether a scheduled crash/error fires immediately
// before or immediately after its named operation.
type SimFaultPoint uint8

const (
	SimBeforeOperation SimFaultPoint = iota + 1
	SimAfterOperation
)

// SimCrashDirective schedules a crash at the occurrence-th invocation of Op.
// Occurrences are one-based and are reset by ResetEvents.
type SimCrashDirective struct {
	Op             FSOp
	Occurrence     uint64
	Point          SimFaultPoint
	RetainUnsynced int
}

// SimErrorDirective schedules an ordinary operation failure without crashing.
// A before-operation failure has no effect; an after-operation failure returns
// after the named effect has happened.
type SimErrorDirective struct {
	Op         FSOp
	Occurrence uint64
	Point      SimFaultPoint
	Err        error
}

// FSEvent is deterministic durability-order evidence. Completed is false when
// a before-operation fault prevented the operation's effect.
type FSEvent struct {
	Ordinal    uint64
	Op         FSOp
	Path       string
	OtherPath  string
	Occurrence uint64
	Completed  bool
}

type simFaultKey struct {
	op         FSOp
	occurrence uint64
	point      SimFaultPoint
}

type simInode struct {
	data    []byte
	durable []byte
}

type simMutationKind uint8

const (
	simMutationWrite simMutationKind = iota + 1
	simMutationTruncate
)

// simMutation is one chronological, still-unsynced byte mutation. Writes
// contribute their payload bytes. Truncates contribute one byte of growth or
// shrinkage per byte by which the file length changes. This makes every
// finite crash cut a deterministic prefix while retaining exact append,
// overwrite, and truncate effects.
type simMutation struct {
	node    *simInode
	kind    simMutationKind
	offset  int64
	data    []byte
	oldSize int64
	newSize int64
}

// SimFS is a synchronous deterministic filesystem. It keeps a live namespace
// and a separately synced directory namespace, plus live and file-synced
// bytes for each inode. Crash retains a scheduled prefix of dirty bytes,
// restores the last directory-synced names, and invalidates all old handles.
type SimFS struct {
	mu sync.Mutex

	live         map[string]*simInode
	durableNames map[string]*simInode
	dirty        []simMutation
	generation   uint64
	crashed      bool

	events      []FSEvent
	opCounts    map[FSOp]uint64
	crashFaults map[simFaultKey]SimCrashDirective
	errorFaults map[simFaultKey]SimErrorDirective
}

var _ FS = (*SimFS)(nil)

// NewSimFS constructs an empty filesystem. Directories are implicit; only
// their child-name durability is modeled.
func NewSimFS() *SimFS {
	return &SimFS{
		live:         make(map[string]*simInode),
		durableNames: make(map[string]*simInode),
		opCounts:     make(map[FSOp]uint64),
		crashFaults:  make(map[simFaultKey]SimCrashDirective),
		errorFaults:  make(map[simFaultKey]SimErrorDirective),
	}
}

// SetCrashSchedule replaces the crash schedule. Occurrences are counted from
// the next operation because setting a schedule also clears operation counts.
func (fs *SimFS) SetCrashSchedule(directives []SimCrashDirective) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	faults := make(map[simFaultKey]SimCrashDirective, len(directives))
	for _, directive := range directives {
		if err := validateSimFault(directive.Op, directive.Occurrence, directive.Point); err != nil {
			return err
		}
		if directive.RetainUnsynced < RetainAllUnsynced {
			return fmt.Errorf("lsm simfs: retain %d is below %d", directive.RetainUnsynced, RetainAllUnsynced)
		}
		key := simFaultKey{op: directive.Op, occurrence: directive.Occurrence, point: directive.Point}
		if _, exists := faults[key]; exists {
			return fmt.Errorf("lsm simfs: duplicate crash directive for %s occurrence %d", directive.Op, directive.Occurrence)
		}
		faults[key] = directive
	}
	fs.crashFaults = faults
	fs.opCounts = make(map[FSOp]uint64)
	return nil
}

// SetErrorSchedule replaces the ordinary failure schedule and resets
// operation occurrence counts.
func (fs *SimFS) SetErrorSchedule(directives []SimErrorDirective) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	faults := make(map[simFaultKey]SimErrorDirective, len(directives))
	for _, directive := range directives {
		if err := validateSimFault(directive.Op, directive.Occurrence, directive.Point); err != nil {
			return err
		}
		if directive.Err == nil {
			return errors.New("lsm simfs: scheduled error must not be nil")
		}
		key := simFaultKey{op: directive.Op, occurrence: directive.Occurrence, point: directive.Point}
		if _, exists := faults[key]; exists {
			return fmt.Errorf("lsm simfs: duplicate error directive for %s occurrence %d", directive.Op, directive.Occurrence)
		}
		faults[key] = directive
	}
	fs.errorFaults = faults
	fs.opCounts = make(map[FSOp]uint64)
	return nil
}

func validateSimFault(op FSOp, occurrence uint64, point SimFaultPoint) error {
	if op == "" {
		return errors.New("lsm simfs: fault operation is empty")
	}
	if !isDeclaredFSOp(op) {
		return fmt.Errorf("lsm simfs: unknown fault operation %q", op)
	}
	if occurrence == 0 {
		return errors.New("lsm simfs: fault occurrence is zero")
	}
	if point != SimBeforeOperation && point != SimAfterOperation {
		return fmt.Errorf("lsm simfs: invalid fault point %d", point)
	}
	return nil
}

func isDeclaredFSOp(op FSOp) bool {
	switch op {
	case FSOpCreate,
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
		FSOpClose:
		return true
	default:
		return false
	}
}

// ResetEvents clears evidence and per-operation occurrence counters. It does
// not change filesystem state or installed schedules.
func (fs *SimFS) ResetEvents() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.events = nil
	fs.opCounts = make(map[FSOp]uint64)
}

// Events returns a copy of the ordered operation evidence.
func (fs *SimFS) Events() []FSEvent {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]FSEvent(nil), fs.events...)
}

// Crash applies the Phase 3-style dirty-prefix retention rule and restores
// the last directory-synced namespace.
func (fs *SimFS) Crash(retainUnsynced int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.crashed {
		return ErrSimulatedCrash
	}
	if retainUnsynced < RetainAllUnsynced {
		return fmt.Errorf("lsm simfs: retain %d is below %d", retainUnsynced, RetainAllUnsynced)
	}
	fs.crashLocked(retainUnsynced)
	return nil
}

// Recover makes the surviving platter image available to new handles.
func (fs *SimFS) Recover() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if !fs.crashed {
		return errors.New("lsm simfs: Recover called while running")
	}
	fs.crashed = false
	return nil
}

// Crashed reports whether operations are currently unavailable.
func (fs *SimFS) Crashed() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.crashed
}

// DurableBytes returns a copy of a directory-synced file's synced platter
// bytes. A live but unsynced name reports ErrNotExist.
func (fs *SimFS) DurableBytes(name string) ([]byte, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	node, ok := fs.durableNames[cleanPath(name)]
	if !ok {
		return nil, ErrNotExist
	}
	return append([]byte(nil), node.durable...), nil
}

// LiveBytes returns the currently visible bytes, including dirty writes.
func (fs *SimFS) LiveBytes(name string) ([]byte, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	node, ok := fs.live[cleanPath(name)]
	if !ok {
		return nil, ErrNotExist
	}
	return append([]byte(nil), node.data...), nil
}

func (fs *SimFS) Create(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	name = cleanPath(name)
	event, err := fs.beginLocked(FSOpCreate, name, "")
	if err != nil {
		return nil, err
	}
	if _, exists := fs.live[name]; exists {
		return nil, ErrExist
	}
	node := &simInode{}
	fs.live[name] = node
	if err := fs.finishLocked(event); err != nil {
		return nil, err
	}
	return &simFile{fs: fs, node: node, path: name, generation: fs.generation}, nil
}

func (fs *SimFS) Open(name string) (File, error) {
	return fs.open(name, false)
}

func (fs *SimFS) OpenAppend(name string) (File, error) {
	return fs.open(name, true)
}

func (fs *SimFS) open(name string, appendMode bool) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	name = cleanPath(name)
	op := FSOpOpen
	if appendMode {
		op = FSOpOpenAppend
	}
	event, err := fs.beginLocked(op, name, "")
	if err != nil {
		return nil, err
	}
	node, ok := fs.live[name]
	if !ok {
		return nil, ErrNotExist
	}
	offset := int64(0)
	if appendMode {
		offset = int64(len(node.data))
	}
	if err := fs.finishLocked(event); err != nil {
		return nil, err
	}
	return &simFile{fs: fs, node: node, path: name, offset: offset, generation: fs.generation}, nil
}

func (fs *SimFS) SyncDir(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	name = cleanPath(name)
	event, err := fs.beginLocked(FSOpDirSync, name, "")
	if err != nil {
		return err
	}
	for durableName := range fs.durableNames {
		if filepath.Dir(durableName) == name {
			delete(fs.durableNames, durableName)
		}
	}
	for liveName, node := range fs.live {
		if filepath.Dir(liveName) == name {
			fs.durableNames[liveName] = node
		}
	}
	return fs.finishLocked(event)
}

func (fs *SimFS) Rename(oldName, newName string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	oldName, newName = cleanPath(oldName), cleanPath(newName)
	event, err := fs.beginLocked(FSOpRename, oldName, newName)
	if err != nil {
		return err
	}
	node, ok := fs.live[oldName]
	if !ok {
		return ErrNotExist
	}
	delete(fs.live, oldName)
	fs.live[newName] = node
	return fs.finishLocked(event)
}

func (fs *SimFS) Link(oldName, newName string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	oldName, newName = cleanPath(oldName), cleanPath(newName)
	event, err := fs.beginLocked(FSOpLink, oldName, newName)
	if err != nil {
		return err
	}
	node, ok := fs.live[oldName]
	if !ok {
		return ErrNotExist
	}
	if _, exists := fs.live[newName]; exists {
		return ErrExist
	}
	fs.live[newName] = node
	return fs.finishLocked(event)
}

func (fs *SimFS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	name = cleanPath(name)
	event, err := fs.beginLocked(FSOpRemove, name, "")
	if err != nil {
		return err
	}
	if _, ok := fs.live[name]; !ok {
		return ErrNotExist
	}
	delete(fs.live, name)
	return fs.finishLocked(event)
}

func (fs *SimFS) List(name string) ([]string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	name = cleanPath(name)
	event, err := fs.beginLocked(FSOpList, name, "")
	if err != nil {
		return nil, err
	}
	var names []string
	for liveName := range fs.live {
		if filepath.Dir(liveName) == name {
			names = append(names, filepath.Base(liveName))
		}
	}
	sort.Strings(names)
	if err := fs.finishLocked(event); err != nil {
		return nil, err
	}
	return names, nil
}

func (fs *SimFS) Stat(name string) (FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	name = cleanPath(name)
	event, err := fs.beginLocked(FSOpStat, name, "")
	if err != nil {
		return FileInfo{}, err
	}
	node, ok := fs.live[name]
	if !ok {
		return FileInfo{}, ErrNotExist
	}
	info := FileInfo{Size: int64(len(node.data))}
	if err := fs.finishLocked(event); err != nil {
		return FileInfo{}, err
	}
	return info, nil
}

func (fs *SimFS) beginLocked(op FSOp, path, otherPath string) (int, error) {
	if fs.crashed {
		return -1, ErrSimulatedCrash
	}
	fs.opCounts[op]++
	occurrence := fs.opCounts[op]
	fs.events = append(fs.events, FSEvent{
		Ordinal:    uint64(len(fs.events) + 1),
		Op:         op,
		Path:       path,
		OtherPath:  otherPath,
		Occurrence: occurrence,
	})
	event := len(fs.events) - 1
	key := simFaultKey{op: op, occurrence: occurrence, point: SimBeforeOperation}
	if directive, ok := fs.errorFaults[key]; ok {
		delete(fs.errorFaults, key)
		return event, directive.Err
	}
	if directive, ok := fs.crashFaults[key]; ok {
		delete(fs.crashFaults, key)
		fs.crashLocked(directive.RetainUnsynced)
		return event, ErrSimulatedCrash
	}
	return event, nil
}

func (fs *SimFS) finishLocked(event int) error {
	fs.events[event].Completed = true
	entry := fs.events[event]
	key := simFaultKey{op: entry.Op, occurrence: entry.Occurrence, point: SimAfterOperation}
	if directive, ok := fs.errorFaults[key]; ok {
		delete(fs.errorFaults, key)
		return directive.Err
	}
	if directive, ok := fs.crashFaults[key]; ok {
		delete(fs.crashFaults, key)
		fs.crashLocked(directive.RetainUnsynced)
		return ErrSimulatedCrash
	}
	return nil
}

func (fs *SimFS) recordWriteLocked(node *simInode, offset int64, data []byte) {
	if len(data) == 0 {
		return
	}
	fs.dirty = append(fs.dirty, simMutation{
		node:   node,
		kind:   simMutationWrite,
		offset: offset,
		data:   append([]byte(nil), data...),
	})
}

func (fs *SimFS) clearDirtyLocked(node *simInode) {
	retained := fs.dirty[:0]
	for _, mutation := range fs.dirty {
		if mutation.node != node {
			retained = append(retained, mutation)
		}
	}
	clear(fs.dirty[len(retained):])
	fs.dirty = retained
}

func (fs *SimFS) crashLocked(retain int) {
	remaining := retain
	survivors := make(map[*simInode][]byte)
	image := func(node *simInode) []byte {
		if survivor, ok := survivors[node]; ok {
			return survivor
		}
		survivor := append([]byte(nil), node.durable...)
		survivors[node] = survivor
		return survivor
	}

	for _, mutation := range fs.dirty {
		available := mutation.byteCount()
		keep := available
		if retain != RetainAllUnsynced {
			if remaining <= 0 {
				break
			}
			keep = min(remaining, available)
			remaining -= keep
		}
		survivors[mutation.node] = mutation.applyPrefix(image(mutation.node), keep)
		if keep != available {
			break
		}
	}

	seen := make(map[*simInode]struct{})
	for _, node := range fs.durableNames {
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		if survivor, ok := survivors[node]; ok {
			node.data = append([]byte(nil), survivor...)
			node.durable = append([]byte(nil), survivor...)
		} else {
			node.data = append([]byte(nil), node.durable...)
		}
	}
	fs.live = make(map[string]*simInode, len(fs.durableNames))
	for name, node := range fs.durableNames {
		fs.live[name] = node
	}
	fs.dirty = nil
	fs.generation++
	fs.crashed = true
}

func (mutation simMutation) byteCount() int {
	switch mutation.kind {
	case simMutationWrite:
		return len(mutation.data)
	case simMutationTruncate:
		if mutation.newSize >= mutation.oldSize {
			return int(mutation.newSize - mutation.oldSize)
		}
		return int(mutation.oldSize - mutation.newSize)
	default:
		panic("lsm simfs: unknown mutation kind")
	}
}

func (mutation simMutation) applyPrefix(image []byte, retained int) []byte {
	switch mutation.kind {
	case simMutationWrite:
		end := mutation.offset + int64(retained)
		if end > int64(len(image)) {
			image = append(image, make([]byte, int(end)-len(image))...)
		}
		copy(image[int(mutation.offset):int(end)], mutation.data[:retained])
		return image
	case simMutationTruncate:
		if mutation.newSize < mutation.oldSize {
			return image[:len(image)-retained]
		}
		return append(image, make([]byte, retained)...)
	default:
		panic("lsm simfs: unknown mutation kind")
	}
}

func cleanPath(name string) string { return filepath.Clean(name) }

type simFile struct {
	fs         *SimFS
	node       *simInode
	path       string
	offset     int64
	generation uint64
	closed     bool
}

var _ File = (*simFile)(nil)

func (file *simFile) Read(dst []byte) (int, error) {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	event, err := file.beginLocked(FSOpRead)
	if err != nil {
		return 0, err
	}
	if file.offset >= int64(len(file.node.data)) {
		if err := file.fs.finishLocked(event); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	n := copy(dst, file.node.data[file.offset:])
	file.offset += int64(n)
	finishErr := file.fs.finishLocked(event)
	if finishErr != nil {
		return n, finishErr
	}
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

func (file *simFile) ReadAt(dst []byte, offset int64) (int, error) {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	event, err := file.beginLocked(FSOpRead)
	if err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, errors.New("lsm simfs: negative read offset")
	}
	if offset >= int64(len(file.node.data)) {
		if err := file.fs.finishLocked(event); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	n := copy(dst, file.node.data[offset:])
	finishErr := file.fs.finishLocked(event)
	if finishErr != nil {
		return n, finishErr
	}
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

func (file *simFile) Write(src []byte) (int, error) {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	event, err := file.beginLocked(FSOpWrite)
	if err != nil {
		return 0, err
	}
	if file.offset < 0 || file.offset > int64(^uint(0)>>1) {
		return 0, errors.New("lsm simfs: invalid write offset")
	}
	end := file.offset + int64(len(src))
	if end < file.offset || uint64(end) > uint64(^uint(0)>>1) {
		return 0, errors.New("lsm simfs: write size overflow")
	}
	file.fs.recordWriteLocked(file.node, file.offset, src)
	if int(end) > len(file.node.data) {
		file.node.data = append(file.node.data, make([]byte, int(end)-len(file.node.data))...)
	}
	copy(file.node.data[int(file.offset):int(end)], src)
	file.offset = end
	if err := file.fs.finishLocked(event); err != nil {
		return len(src), err
	}
	return len(src), nil
}

func (file *simFile) Sync() error {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	event, err := file.beginLocked(FSOpFileSync)
	if err != nil {
		return err
	}
	file.node.durable = append([]byte(nil), file.node.data...)
	file.fs.clearDirtyLocked(file.node)
	return file.fs.finishLocked(event)
}

func (file *simFile) Truncate(size int64) error {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	event, err := file.beginLocked(FSOpTruncate)
	if err != nil {
		return err
	}
	if size < 0 || uint64(size) > uint64(^uint(0)>>1) {
		return errors.New("lsm simfs: invalid truncate size")
	}
	oldSize := int64(len(file.node.data))
	if size != oldSize {
		file.fs.dirty = append(file.fs.dirty, simMutation{
			node:    file.node,
			kind:    simMutationTruncate,
			oldSize: oldSize,
			newSize: size,
		})
	}
	if int(size) <= len(file.node.data) {
		file.node.data = file.node.data[:int(size)]
	} else {
		file.node.data = append(file.node.data, make([]byte, int(size)-len(file.node.data))...)
	}
	if file.offset > size {
		file.offset = size
	}
	return file.fs.finishLocked(event)
}

func (file *simFile) Close() error {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	if file.closed {
		return ErrFSClosed
	}
	event, err := file.beginLocked(FSOpClose)
	if err != nil {
		return err
	}
	file.closed = true
	return file.fs.finishLocked(event)
}

func (file *simFile) beginLocked(op FSOp) (int, error) {
	if file.closed || file.generation != file.fs.generation {
		return -1, ErrFSClosed
	}
	return file.fs.beginLocked(op, file.path, "")
}
