// Package disklog implements the durable Raft storage boundary as a
// segmented, append-only, CRC-framed write-ahead log. The Raft log IS the
// write-ahead log: each Save appends one framed record batch to the active
// segment and returns only after that file is synced, so the Phase 0
// contract — one Ready, one Save, one batch, one sync — is structural.
//
// On-disk format: numbered segment files (000001.seg, ...) rotated once the
// active segment reaches Options.RotateSize (default 8 MiB). Every record is
// framed as 0xFA || E(B) || 0xFB, where B is a little-endian uint32 payload
// length, a little-endian CRC32-C (Castagnoli) of the payload, and a
// marshaled raftpb.LogRecord. E maps each body byte to two bytes, high nibble
// first, with nibble n represented by 0x40+n; only 0x40..0x4F is legal
// between the markers. A batch is never split across segments — rotation
// happens between batches — so one Save is one write and one sync on one
// file, and a batch landing exactly at RotateSize fills its segment while the
// following Save rotates.
//
// Suffix truncation is logical, via TruncateRecord frames (see
// docs/NOTES.md); segment bytes are rewritten only when recovery physically
// truncates a torn tail.
//
// Durability: Save syncs with (*os.File).Sync — fsync, a superset of the
// fdatasync guarantee the phase spec names; the standard library has no
// portable fdatasync and this package adds no dependency for one. Creating a
// segment (fresh Open, every rotation) also syncs the directory so the new
// name itself survives a crash. Recovery scans segments in order, verifies
// every CRC, and rebuilds the hard state and entry sequence in an in-memory
// mirror that serves all reads; before Open returns it syncs every segment
// file and then the directory, because bytes and names that merely read back
// are not evidence of durability — a previous process may have died between
// write and sync — and recovered state can drive responses (a re-granted
// vote arrives with no new HardState to Save), so it must be durable first.
package disklog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

// DefaultRotateSize is the segment rotation threshold used when
// Options.RotateSize is zero.
const DefaultRotateSize = 8 << 20

const (
	segmentSuffix    = ".seg"
	snapshotMetaName = "SNAPSHOT"
	snapshotTempName = "SNAPSHOT.tmp"
	frameStart       = byte(0xFA)
	frameEnd         = byte(0xFB)
	frameSymbolBase  = byte(0x40)
	frameSymbolLimit = byte(0x4F)
	frameHeaderSize  = 8 // uint32 payload length + uint32 crc32c
	frameMarkerBytes = 2
	encodedByteWidth = 2
	snapshotMetaSize = 52 // magic, state snapshot, compacted base, segment floor, crc32c
)

var snapshotMetaMagic = [8]byte{'M', 'Q', 'S', 'N', 'A', 'P', '0', '2'}

// ErrCorrupt marks startup damage that cannot be attributed to the sole
// removable torn tail: EOF after a start marker and before its terminator in
// the final record of the final segment. Errors wrapping it name the segment
// and byte offset.
var ErrCorrupt = errors.New("disklog: corrupt log")

var errClosed = errors.New("disklog: closed")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Options configures Open.
type Options struct {
	// RotateSize is the segment size in bytes past which the next Save
	// opens a new segment. Zero selects DefaultRotateSize. A batch is never
	// split, so a single Save larger than RotateSize yields one oversized
	// segment.
	RotateSize int64

	// syncFile and syncDir are in-package test seams for the durability
	// points: the segment sync ending every Save, the directory sync after
	// every segment creation, and the recovery syncs Open performs before it
	// serves anything. Nil selects (*os.File).Sync.
	syncFile func(*os.File) error
	syncDir  func(*os.File) error
}

// DiskLog is a storage.Storage backed by segmented append-only files. Reads
// are served from an in-memory mirror rebuilt by Open; disk moves strictly
// forward except when recovery truncates a torn tail.
type DiskLog struct {
	mu         sync.RWMutex
	dirPath    string
	dir        *os.File
	rotateSize int64
	syncFile   func(*os.File) error
	syncDir    func(*os.File) error

	active     *os.File
	activeSeq  uint64
	activeSize int64

	hard         raft.HardState
	snapshot     raft.SnapshotMeta
	compacted    raft.SnapshotMeta
	segmentFloor uint64
	entries      []raftpb.Entry

	// failed poisons every later Save after a failed durable step: bytes
	// appended after a partial frame would read back as mid-log corruption.
	failed error
	closed bool
}

var _ storage.Storage = (*DiskLog)(nil)

// Open recovers the log in dir, which must already exist: disklog syncs
// everything it creates, and only the deployer can make dir itself durable
// in its parent. An empty dir starts a fresh log with one empty segment.
func Open(dir string, opts Options) (*DiskLog, error) {
	if opts.RotateSize < 0 {
		return nil, fmt.Errorf("disklog: negative rotate size %d", opts.RotateSize)
	}
	l := &DiskLog{
		dirPath:      dir,
		rotateSize:   opts.RotateSize,
		syncFile:     opts.syncFile,
		syncDir:      opts.syncDir,
		segmentFloor: 1,
	}
	if l.rotateSize == 0 {
		l.rotateSize = DefaultRotateSize
	}
	if l.syncFile == nil {
		l.syncFile = (*os.File).Sync
	}
	if l.syncDir == nil {
		l.syncDir = (*os.File).Sync
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("disklog: open dir: %w", err)
	}
	l.dir = dirFile
	err = l.removeSnapshotTemp()
	if err == nil {
		l.snapshot, l.compacted, l.segmentFloor, err = l.loadSnapshotMeta()
	}
	seqs := []uint64(nil)
	if err == nil {
		seqs, err = listSegments(dir)
	}
	if err == nil && len(seqs) > 0 {
		seqs, err = l.discardRetiredSegmentPrefix(seqs)
	}
	if err == nil {
		if len(seqs) == 0 {
			if l.snapshot != (raft.SnapshotMeta{}) || l.compacted != (raft.SnapshotMeta{}) {
				err = fmt.Errorf("%w: snapshot metadata exists but every Raft segment is missing", ErrCorrupt)
			} else {
				l.active, err = l.createSegment(1)
				l.activeSeq, l.activeSize = 1, 0
			}
		} else {
			err = l.recover(seqs)
		}
	}
	if err != nil {
		if l.active != nil {
			_ = l.active.Close()
		}
		_ = dirFile.Close()
		return nil, err
	}
	return l, nil
}

// Save appends one durable batch: an optional HardStateRecord, a
// TruncateRecord when the entries overwrite an existing suffix, then the
// EntriesRecord. It returns only after the batch bytes are written and the
// segment file synced — one Ready, one Save, one sync. Entries are
// deep-copied into the read mirror; the caller keeps ownership of its slice.
func (l *DiskLog) Save(hs *raft.HardState, entries []raftpb.Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	if l.failed != nil {
		return fmt.Errorf("disklog: poisoned by earlier failure: %w", l.failed)
	}
	if hs == nil && len(entries) == 0 {
		return nil // nothing new to make durable
	}
	last := l.lastIndexLocked()
	if err := checkContiguous(entries, last); err != nil {
		return err // nothing hit the disk; the log stays usable
	}
	batch, err := encodeBatch(hs, entries, last)
	if err != nil {
		return err
	}
	batchSize := int64(len(batch))
	if err := l.rotateIfNeeded(batchSize); err != nil {
		l.failed = err
		return err
	}
	if n, err := l.active.Write(batch); err != nil {
		l.failed = err
		return fmt.Errorf("disklog: write %s: %w", segmentName(l.activeSeq), err)
	} else if n != len(batch) {
		l.failed = io.ErrShortWrite
		return fmt.Errorf("disklog: write %s: wrote %d of %d bytes: %w", segmentName(l.activeSeq), n, len(batch), io.ErrShortWrite)
	}
	if err := l.syncFile(l.active); err != nil {
		l.failed = err
		return fmt.Errorf("disklog: sync %s: %w", segmentName(l.activeSeq), err)
	}
	l.activeSize += batchSize
	if hs != nil {
		l.hard = *hs
	}
	if len(entries) > 0 {
		l.truncateEntries(entries[0].Index)
		l.entries = append(l.entries, cloneEntries(entries)...)
	}
	return nil
}

// SaveSnapshot durably publishes the newest snapshot position in a separate
// fixed-size record. The state-machine snapshot directory is already durable
// when the server calls this method; file sync, rename, then directory sync
// make this metadata the later side of that ordering boundary.
func (l *DiskLog) SaveSnapshot(meta raft.SnapshotMeta) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	if l.failed != nil {
		return fmt.Errorf("disklog: poisoned by earlier failure: %w", l.failed)
	}
	if err := validateSnapshotAdvance(l.snapshot, meta); err != nil {
		return err
	}
	if meta == l.snapshot {
		return nil
	}
	compacted := l.compacted
	installed := false
	if meta.Index != 0 {
		if term, ok := l.entryTermLocked(meta.Index); !ok || term != meta.Term {
			// An installed snapshot need not have a matching local entry X.
			// With no valid overlap, publish X as both state snapshot and log
			// base; Compact(X) may later reclaim whole physical segments.
			compacted = meta
			installed = true
		}
	}
	if installed {
		// A suffix is reusable after InstallSnapshot only when its entry X has
		// the snapshot term. Durably invalidate X+1 and later before publishing
		// SNAPSHOT, so neither side of a crash can resurrect a conflicting tail.
		batch, err := appendFrame(nil, &raftpb.LogRecord{Body: &raftpb.LogRecord_Truncate{Truncate: &raftpb.TruncateRecord{FromIndex: meta.Index + 1}}})
		if err != nil {
			return err
		}
		if err := l.rotateIfNeeded(int64(len(batch))); err != nil {
			l.failed = err
			return err
		}
		if err := l.appendActiveLocked(batch); err != nil {
			l.failed = err
			return err
		}
		l.truncateEntries(meta.Index + 1)
	}
	if err := l.persistSnapshotStateLocked(meta, compacted, l.segmentFloor); err != nil {
		if installed {
			l.failed = err
		}
		return err
	}
	l.snapshot = meta
	l.compacted = compacted
	if installed {
		// X and below are covered by the snapshot; the standalone truncate
		// record above already invalidated the old suffix.
		clear(l.entries)
		l.entries = nil
	} else if compacted.Index == meta.Index {
		l.dropCompactedEntriesLocked()
	}
	return nil
}

func (l *DiskLog) persistSnapshotStateLocked(snapshot, compacted raft.SnapshotMeta, segmentFloor uint64) error {
	data := encodeSnapshotMeta(snapshot, compacted, segmentFloor)
	tempPath := filepath.Join(l.dirPath, snapshotTempName)
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("disklog: remove stale snapshot metadata temp: %w", err)
	}
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("disklog: create snapshot metadata temp: %w", err)
	}
	failed := func(cause error) error {
		l.failed = cause
		return cause
	}
	if n, writeErr := file.Write(data); writeErr != nil {
		_ = file.Close()
		return failed(fmt.Errorf("disklog: write snapshot metadata: %w", writeErr))
	} else if n != len(data) {
		_ = file.Close()
		return failed(fmt.Errorf("disklog: write snapshot metadata: wrote %d of %d bytes: %w", n, len(data), io.ErrShortWrite))
	}
	if err := l.syncFile(file); err != nil {
		_ = file.Close()
		return failed(fmt.Errorf("disklog: sync snapshot metadata: %w", err))
	}
	if err := file.Close(); err != nil {
		return failed(fmt.Errorf("disklog: close snapshot metadata: %w", err))
	}
	if err := os.Rename(tempPath, filepath.Join(l.dirPath, snapshotMetaName)); err != nil {
		return failed(fmt.Errorf("disklog: publish snapshot metadata: %w", err))
	}
	if err := l.syncDir(l.dir); err != nil {
		return failed(fmt.Errorf("disklog: sync dir after snapshot metadata: %w", err))
	}
	return nil
}

// HardState returns the last saved (or recovered) hard state.
func (l *DiskLog) HardState() (raft.HardState, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.hard, nil
}

// Snapshot returns the last durably published snapshot position.
func (l *DiskLog) Snapshot() (raft.SnapshotMeta, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.snapshot, nil
}

// Compacted returns the durable predecessor of the retained Raft log. It is
// intentionally distinct from Snapshot so the overlap (Compacted, Snapshot]
// remains available to replication after restart.
func (l *DiskLog) Compacted() (raft.SnapshotMeta, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.compacted, nil
}

// Entries returns deep copies of the stored entries in [lo, hi).
func (l *DiskLog) Entries(lo, hi uint64) ([]raftpb.Entry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	first, last := l.firstLastLocked()
	if lo > hi || lo < first || hi > last+1 {
		return nil, storage.ErrOutOfBounds
	}
	return cloneEntries(l.entries[lo-first : hi-first]), nil
}

// FirstIndex returns the first available index, or 1 for an empty log.
func (l *DiskLog) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	first, _ := l.firstLastLocked()
	return first
}

// LastIndex returns the last available index, or 0 for an empty log.
func (l *DiskLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked()
}

// Compact removes only complete, closed segment files whose every physical
// EntriesRecord ends at or before uptoIndex. A fresh active segment carrying
// a durable HardState checkpoint is created first, so prefix deletion cannot
// discard the latest term/vote state. A segment straddling the boundary is
// retained whole.
func (l *DiskLog) Compact(uptoIndex uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	if l.failed != nil {
		return fmt.Errorf("disklog: poisoned by earlier failure: %w", l.failed)
	}
	if uptoIndex > l.snapshot.Index {
		return fmt.Errorf("%w: compact through %d, snapshot index %d", storage.ErrCompactionBeyondSnapshot, uptoIndex, l.snapshot.Index)
	}
	if uptoIndex == 0 {
		return nil
	}
	if uptoIndex < l.compacted.Index {
		return nil
	}
	base := l.compacted
	if uptoIndex > base.Index {
		term, ok := l.termLocked(uptoIndex)
		if !ok {
			return fmt.Errorf("disklog: compact index %d has no retained term", uptoIndex)
		}
		base = raft.SnapshotMeta{Index: uptoIndex, Term: term}
	}
	// First create a durable HardState checkpoint in a fresh survivor. If a
	// crash precedes metadata publication, the old segment floor still causes
	// recovery to replay the full prefix plus this harmless checkpoint.
	if err := l.rotateForCompactionLocked(); err != nil {
		l.failed = err
		return err
	}
	checkpoint, err := encodeBatch(&l.hard, nil, l.lastIndexLocked())
	if err != nil {
		return err
	}
	if err := l.appendActiveLocked(checkpoint); err != nil {
		l.failed = err
		return err
	}

	seqs, err := listSegments(l.dirPath)
	if err != nil {
		return err
	}
	deleteCount := 0
	discardEveryClosed := uptoIndex == l.snapshot.Index && l.compacted.Index == l.snapshot.Index && len(l.entries) == 0
	for _, seq := range seqs {
		if seq == l.activeSeq {
			break
		}
		if discardEveryClosed {
			// InstallSnapshot with a missing/conflicting boundary durably
			// invalidated its entire old suffix before setting B=X. With no
			// subsequently appended live tail, every pre-checkpoint segment is
			// obsolete even if its physical history once contained indexes >X.
			deleteCount++
			continue
		}
		maxIndex, hasEntries, scanErr := l.segmentEntryMaximum(seq)
		if scanErr != nil {
			return scanErr
		}
		if hasEntries && maxIndex > uptoIndex {
			break
		}
		deleteCount++
	}
	if deleteCount >= len(seqs) {
		l.failed = ErrCorrupt
		return fmt.Errorf("%w: compaction selected every segment including active %s", ErrCorrupt, segmentName(l.activeSeq))
	}
	newFloor := seqs[deleteCount]
	// Publish B and the exact first surviving segment before the first unlink.
	// Recovery can ignore a leftover physical prefix, but it rejects a missing
	// declared survivor instead of mistaking data loss for prior compaction.
	if base != l.compacted || newFloor != l.segmentFloor {
		if err := l.persistSnapshotStateLocked(l.snapshot, base, newFloor); err != nil {
			return err
		}
		l.compacted = base
		l.segmentFloor = newFloor
	}
	if deleteCount == 0 {
		l.dropCompactedEntriesLocked()
		return nil
	}
	for _, seq := range seqs[:deleteCount] {
		if err := os.Remove(l.segmentPath(seq)); err != nil {
			l.failed = err
			return fmt.Errorf("disklog: remove compacted segment %s: %w", segmentName(seq), err)
		}
		// Sync every oldest-prefix deletion before attempting the next. Thus
		// every crash-visible namespace is a contiguous suffix, never a
		// persisted middle gap whose recovery meaning would be ambiguous.
		if err := l.syncDir(l.dir); err != nil {
			l.failed = err
			return fmt.Errorf("disklog: sync dir after removing %s: %w", segmentName(seq), err)
		}
	}
	remaining := seqs[deleteCount:]
	if err := l.rebuildMirrorLocked(remaining); err != nil {
		l.failed = err
		return err
	}
	return nil
}

func (l *DiskLog) termLocked(index uint64) (uint64, bool) {
	if index == l.snapshot.Index {
		return l.snapshot.Term, true
	}
	if index == l.compacted.Index {
		return l.compacted.Term, true
	}
	return l.entryTermLocked(index)
}

func (l *DiskLog) entryTermLocked(index uint64) (uint64, bool) {
	for i := range l.entries {
		if l.entries[i].Index == index {
			return l.entries[i].Term, true
		}
	}
	return 0, false
}

// Close releases the directory and active-segment handles. Every successful
// Save is already durable, so Close syncs nothing. Reads keep serving from
// the mirror; further Saves fail.
func (l *DiskLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var errFile error
	if l.active != nil {
		errFile = l.active.Close()
	}
	errDir := l.dir.Close()
	if errFile != nil {
		return errFile
	}
	return errDir
}

// rotateIfNeeded moves to a fresh segment when the batch would push the
// active one past rotateSize. The check runs before the write and an empty
// segment always accepts its batch whole.
func (l *DiskLog) rotateIfNeeded(batchSize int64) error {
	if batchSize < 0 {
		return fmt.Errorf("disklog: negative batch size %d", batchSize)
	}
	if l.activeSize == 0 || (l.activeSize <= l.rotateSize && batchSize <= l.rotateSize-l.activeSize) {
		return nil
	}
	if err := l.active.Close(); err != nil {
		return fmt.Errorf("disklog: close %s: %w", segmentName(l.activeSeq), err)
	}
	l.active = nil // no active segment until the next one exists; a failure here poisons the log
	if l.activeSeq == math.MaxUint64 {
		return errors.New("disklog: segment sequence overflow")
	}
	next, err := l.createSegment(l.activeSeq + 1)
	if err != nil {
		return err
	}
	l.active = next
	l.activeSeq++
	l.activeSize = 0
	return nil
}

// createSegment creates the numbered segment file and syncs the directory so
// the new name survives a crash, per the phase spec.
func (l *DiskLog) createSegment(seq uint64) (*os.File, error) {
	f, err := os.OpenFile(l.segmentPath(seq), os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("disklog: create segment: %w", err)
	}
	if err := l.syncDir(l.dir); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("disklog: sync dir after creating %s: %w", segmentName(seq), err)
	}
	return f, nil
}

func (l *DiskLog) rotateForCompactionLocked() error {
	if l.active == nil {
		return errors.New("disklog: no active segment")
	}
	if l.activeSeq == math.MaxUint64 {
		return errors.New("disklog: segment sequence overflow")
	}
	if err := l.active.Close(); err != nil {
		return fmt.Errorf("disklog: close %s for compaction: %w", segmentName(l.activeSeq), err)
	}
	l.active = nil
	next, err := l.createSegment(l.activeSeq + 1)
	if err != nil {
		return err
	}
	l.active = next
	l.activeSeq++
	l.activeSize = 0
	return nil
}

func (l *DiskLog) appendActiveLocked(batch []byte) error {
	if len(batch) == 0 {
		return nil
	}
	if n, err := l.active.Write(batch); err != nil {
		return fmt.Errorf("disklog: write %s: %w", segmentName(l.activeSeq), err)
	} else if n != len(batch) {
		return fmt.Errorf("disklog: write %s: wrote %d of %d bytes: %w", segmentName(l.activeSeq), n, len(batch), io.ErrShortWrite)
	}
	if err := l.syncFile(l.active); err != nil {
		return fmt.Errorf("disklog: sync %s: %w", segmentName(l.activeSeq), err)
	}
	l.activeSize += int64(len(batch))
	return nil
}

func validateSnapshotAdvance(current, next raft.SnapshotMeta) error {
	if next.Index == math.MaxUint64 {
		return errors.New("disklog: snapshot index overflows first index")
	}
	if next.Index == 0 {
		if next.Term != 0 {
			return errors.New("disklog: zero snapshot index has nonzero term")
		}
		if current != (raft.SnapshotMeta{}) {
			return fmt.Errorf("disklog: snapshot index regresses from %d to 0", current.Index)
		}
		return nil
	}
	if next.Term == 0 {
		return fmt.Errorf("disklog: snapshot index %d has zero term", next.Index)
	}
	if next.Index < current.Index {
		return fmt.Errorf("disklog: snapshot index regresses from %d to %d", current.Index, next.Index)
	}
	if next.Index == current.Index && current != (raft.SnapshotMeta{}) && next.Term != current.Term {
		return fmt.Errorf("disklog: snapshot term changed at index %d from %d to %d", next.Index, current.Term, next.Term)
	}
	return nil
}

func encodeSnapshotMeta(snapshot, compacted raft.SnapshotMeta, segmentFloor uint64) []byte {
	data := make([]byte, snapshotMetaSize)
	copy(data[:8], snapshotMetaMagic[:])
	binary.LittleEndian.PutUint64(data[8:16], snapshot.Index)
	binary.LittleEndian.PutUint64(data[16:24], snapshot.Term)
	binary.LittleEndian.PutUint64(data[24:32], compacted.Index)
	binary.LittleEndian.PutUint64(data[32:40], compacted.Term)
	binary.LittleEndian.PutUint64(data[40:48], segmentFloor)
	binary.LittleEndian.PutUint32(data[48:52], crc32.Checksum(data[:48], castagnoli))
	return data
}

func decodeSnapshotMeta(data []byte) (raft.SnapshotMeta, raft.SnapshotMeta, uint64, error) {
	if len(data) != snapshotMetaSize {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("%w: snapshot metadata is %d bytes, want %d", ErrCorrupt, len(data), snapshotMetaSize)
	}
	if string(data[:8]) != string(snapshotMetaMagic[:]) {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("%w: snapshot metadata magic mismatch", ErrCorrupt)
	}
	if stored, computed := binary.LittleEndian.Uint32(data[48:52]), crc32.Checksum(data[:48], castagnoli); stored != computed {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("%w: snapshot metadata crc32c mismatch: stored %08x, computed %08x", ErrCorrupt, stored, computed)
	}
	snapshot := raft.SnapshotMeta{Index: binary.LittleEndian.Uint64(data[8:16]), Term: binary.LittleEndian.Uint64(data[16:24])}
	compacted := raft.SnapshotMeta{Index: binary.LittleEndian.Uint64(data[24:32]), Term: binary.LittleEndian.Uint64(data[32:40])}
	segmentFloor := binary.LittleEndian.Uint64(data[40:48])
	if err := validateSnapshotAdvance(raft.SnapshotMeta{}, snapshot); err != nil {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("%w: invalid state snapshot metadata: %v", ErrCorrupt, err)
	}
	if err := validateSnapshotAdvance(raft.SnapshotMeta{}, compacted); err != nil {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("%w: invalid compacted metadata: %v", ErrCorrupt, err)
	}
	if compacted.Index > snapshot.Index {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("%w: compacted index %d exceeds state snapshot index %d", ErrCorrupt, compacted.Index, snapshot.Index)
	}
	if segmentFloor == 0 {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("%w: snapshot metadata has zero segment floor", ErrCorrupt)
	}
	return snapshot, compacted, segmentFloor, nil
}

func (l *DiskLog) removeSnapshotTemp() error {
	path := filepath.Join(l.dirPath, snapshotTempName)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("disklog: remove orphan snapshot metadata temp: %w", err)
	}
	if err := l.syncDir(l.dir); err != nil {
		return fmt.Errorf("disklog: sync dir after orphan snapshot metadata cleanup: %w", err)
	}
	return nil
}

func (l *DiskLog) loadSnapshotMeta() (raft.SnapshotMeta, raft.SnapshotMeta, uint64, error) {
	path := filepath.Join(l.dirPath, snapshotMetaName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 1, nil
	}
	if err != nil {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("disklog: read snapshot metadata: %w", err)
	}
	snapshot, compacted, segmentFloor, err := decodeSnapshotMeta(data)
	if err != nil {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("disklog: open snapshot metadata for recovery sync: %w", err)
	}
	if err := l.syncFile(file); err != nil {
		_ = file.Close()
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("disklog: recovery sync snapshot metadata: %w", err)
	}
	if err := file.Close(); err != nil {
		return raft.SnapshotMeta{}, raft.SnapshotMeta{}, 0, fmt.Errorf("disklog: close snapshot metadata after recovery sync: %w", err)
	}
	return snapshot, compacted, segmentFloor, nil
}

// discardRetiredSegmentPrefix completes any crash-interrupted deletion that
// followed atomic publication of segmentFloor. Files below the floor are
// known obsolete; the floor itself must exist exactly, or startup reports
// data loss instead of treating a missing leading segment as compaction.
func (l *DiskLog) discardRetiredSegmentPrefix(seqs []uint64) ([]uint64, error) {
	position := sort.Search(len(seqs), func(i int) bool { return seqs[i] >= l.segmentFloor })
	if position == len(seqs) || seqs[position] != l.segmentFloor {
		first := seqs[0]
		return nil, fmt.Errorf("%w: segment sequence gap: declared first segment %s is missing (first present %s)", ErrCorrupt, segmentName(l.segmentFloor), segmentName(first))
	}
	for _, seq := range seqs[:position] {
		if err := os.Remove(l.segmentPath(seq)); err != nil {
			return nil, fmt.Errorf("disklog: remove retired segment %s during recovery: %w", segmentName(seq), err)
		}
		if err := l.syncDir(l.dir); err != nil {
			return nil, fmt.Errorf("disklog: sync dir after removing retired segment %s: %w", segmentName(seq), err)
		}
	}
	return seqs[position:], nil
}

// recover replays every segment into the mirror, reopens the last one for
// appending, and then — before the log serves anything — makes everything it
// recovered durable: it syncs every segment file and then the directory,
// exactly as if it had just written each byte and name itself.
//
// The recovery syncs are load-bearing. Bytes and names that read back are not
// evidence of durability: a previous process may have written a batch and
// fail-stopped on a failed sync (its Save error and poison die with it), or
// crashed between write and sync, leaving complete frames only in the page
// cache; a previous rotation may have created a segment and failed the
// directory sync, leaving a name no one ever made durable. Recovered state
// drives responses without any further Save — a follower re-answers a
// repeated RequestVote from its recovered VotedFor with no new HardState, and
// Save(nil, nil) persists nothing — so Open is the last point where
// durability can be established before something depends on it. If a sync
// fails here, Open fails; serving unverified state is exactly the bug.
func (l *DiskLog) recover(seqs []uint64) error {
	for i, seq := range seqs {
		if err := l.replaySegment(seq, i == len(seqs)-1); err != nil {
			return err
		}
	}
	if err := l.validateRecoveredSnapshotTailLocked(); err != nil {
		return err
	}
	l.dropCompactedEntriesLocked()
	last := seqs[len(seqs)-1]
	f, err := os.OpenFile(l.segmentPath(last), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("disklog: reopen %s: %w", segmentName(last), err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("disklog: stat %s: %w", segmentName(last), err)
	}
	l.active, l.activeSeq, l.activeSize = f, last, info.Size()
	for _, seq := range seqs[:len(seqs)-1] {
		if err := l.syncSegment(seq); err != nil {
			return err
		}
	}
	if err := l.syncFile(l.active); err != nil {
		return fmt.Errorf("disklog: recovery sync %s: %w", segmentName(last), err)
	}
	if err := l.syncDir(l.dir); err != nil {
		return fmt.Errorf("disklog: recovery sync dir: %w", err)
	}
	return nil
}

func (l *DiskLog) validateRecoveredSnapshotTailLocked() error {
	if l.compacted.Index > l.snapshot.Index {
		return fmt.Errorf("%w: compacted index %d exceeds snapshot index %d", ErrCorrupt, l.compacted.Index, l.snapshot.Index)
	}
	if l.compacted.Index == l.snapshot.Index {
		if l.compacted.Term != l.snapshot.Term {
			return fmt.Errorf("%w: compacted/snapshot term mismatch at index %d: %d != %d", ErrCorrupt, l.snapshot.Index, l.compacted.Term, l.snapshot.Term)
		}
		return nil
	}
	want := l.compacted.Index + 1
	position := -1
	for i := range l.entries {
		if l.entries[i].Index == want {
			position = i
			break
		}
	}
	if position < 0 {
		return fmt.Errorf("%w: retained overlap is missing first index %d", ErrCorrupt, want)
	}
	for index := want; index <= l.snapshot.Index; index++ {
		offset := position + int(index-want)
		if offset >= len(l.entries) || l.entries[offset].Index != index {
			return fmt.Errorf("%w: retained overlap is missing index %d", ErrCorrupt, index)
		}
		if index == l.snapshot.Index && l.entries[offset].Term != l.snapshot.Term {
			return fmt.Errorf("%w: snapshot term %d at index %d does not match retained log term %d", ErrCorrupt, l.snapshot.Term, index, l.entries[offset].Term)
		}
	}
	return nil
}

// syncSegment opens one closed segment just long enough to sync it.
func (l *DiskLog) syncSegment(seq uint64) error {
	f, err := os.OpenFile(l.segmentPath(seq), os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("disklog: open %s for recovery sync: %w", segmentName(seq), err)
	}
	if err := l.syncFile(f); err != nil {
		_ = f.Close()
		return fmt.Errorf("disklog: recovery sync %s: %w", segmentName(seq), err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("disklog: close %s after recovery sync: %w", segmentName(seq), err)
	}
	return nil
}

// replaySegment applies each physical frame in one segment to the mirror.
// At a record boundary EOF is success and every other byte must be 0xFA.
// Once a frame starts, only encoded-nibble symbols may precede 0xFB. EOF
// before 0xFB is removable only for the final record of the final segment;
// every observed terminator commits recovery to validating the complete
// frame, even when it is the last frame in the log.
func (l *DiskLog) replaySegment(seq uint64, final bool) error {
	path := l.segmentPath(seq)
	buf, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("disklog: read %s: %w", segmentName(seq), err)
	}
	offset := 0
	for offset < len(buf) {
		if buf[offset] != frameStart {
			return fmt.Errorf("%w: %s offset %d: byte 0x%02x at record boundary, want start marker 0x%02x", ErrCorrupt, segmentName(seq), offset, buf[offset], frameStart)
		}

		terminator := -1
		for p := offset + 1; p < len(buf); p++ {
			b := buf[p]
			switch {
			case b == frameEnd:
				terminator = p
			case b == frameStart:
				return fmt.Errorf("%w: %s offset %d: start marker 0x%02x before frame terminator", ErrCorrupt, segmentName(seq), p, b)
			case !isFrameSymbol(b):
				return fmt.Errorf("%w: %s offset %d: invalid interior byte 0x%02x", ErrCorrupt, segmentName(seq), p, b)
			}
			if terminator >= 0 {
				break
			}
		}
		if terminator < 0 {
			if !final {
				return fmt.Errorf("%w: %s offset %d: EOF before frame terminator in non-final segment", ErrCorrupt, segmentName(seq), offset)
			}
			return l.truncateTail(path, seq, int64(offset))
		}

		record, err := decodeTerminatedFrame(buf[offset+1 : terminator])
		if err != nil {
			return fmt.Errorf("%w: %s offset %d: %v", ErrCorrupt, segmentName(seq), offset, err)
		}
		if err := l.applyRecord(record); err != nil {
			return fmt.Errorf("%w: %s offset %d: %v", ErrCorrupt, segmentName(seq), offset, err)
		}
		offset = terminator + 1
	}
	return nil
}

// segmentEntryMaximum scans a closed segment without mutating storage and
// reports the greatest physically encoded entry index. Compact deliberately
// uses physical coverage: if a segment ever contained an entry beyond the
// boundary, retaining that whole segment is conservative and crash-safe.
func (l *DiskLog) segmentEntryMaximum(seq uint64) (uint64, bool, error) {
	buf, err := os.ReadFile(l.segmentPath(seq))
	if err != nil {
		return 0, false, fmt.Errorf("disklog: read %s for compaction: %w", segmentName(seq), err)
	}
	var maximum uint64
	hasEntries := false
	offset := 0
	for offset < len(buf) {
		if buf[offset] != frameStart {
			return 0, false, fmt.Errorf("%w: %s offset %d: invalid record boundary during compaction", ErrCorrupt, segmentName(seq), offset)
		}
		terminator := -1
		for position := offset + 1; position < len(buf); position++ {
			symbol := buf[position]
			if symbol == frameEnd {
				terminator = position
				break
			}
			if symbol == frameStart || !isFrameSymbol(symbol) {
				return 0, false, fmt.Errorf("%w: %s offset %d: malformed frame during compaction", ErrCorrupt, segmentName(seq), offset)
			}
		}
		if terminator < 0 {
			return 0, false, fmt.Errorf("%w: %s offset %d: unterminated closed segment during compaction", ErrCorrupt, segmentName(seq), offset)
		}
		record, err := decodeTerminatedFrame(buf[offset+1 : terminator])
		if err != nil {
			return 0, false, fmt.Errorf("%w: %s offset %d: %v", ErrCorrupt, segmentName(seq), offset, err)
		}
		if body := record.GetEntries(); body != nil {
			for _, entry := range body.GetEntries() {
				if entry.GetIndex() > maximum {
					maximum = entry.GetIndex()
				}
				hasEntries = true
			}
		}
		offset = terminator + 1
	}
	return maximum, hasEntries, nil
}

func (l *DiskLog) rebuildMirrorLocked(seqs []uint64) error {
	scratch := &DiskLog{
		dirPath:   l.dirPath,
		snapshot:  l.snapshot,
		compacted: l.compacted,
		syncFile:  l.syncFile,
	}
	for index, seq := range seqs {
		if err := scratch.replaySegment(seq, index == len(seqs)-1); err != nil {
			return fmt.Errorf("disklog: rebuild mirror after compaction: %w", err)
		}
	}
	l.hard = scratch.hard
	l.entries = scratch.entries
	l.dropCompactedEntriesLocked()
	return nil
}

func (l *DiskLog) dropCompactedEntriesLocked() {
	cut := 0
	for cut < len(l.entries) && l.entries[cut].Index <= l.compacted.Index {
		cut++
	}
	if cut == 0 {
		return
	}
	clear(l.entries[:cut])
	l.entries = append([]raftpb.Entry(nil), l.entries[cut:]...)
}

func isFrameSymbol(b byte) bool {
	return b >= frameSymbolBase && b <= frameSymbolLimit
}

// decodeTerminatedFrame validates a frame whose 0xFB has already been
// observed. It decodes the fixed header first, checks all size arithmetic in
// uint64, and requires the physical symbol count to agree exactly with the
// declared payload length before allocating the payload. A corrupt length
// therefore cannot drive an allocation or an int conversion.
func decodeTerminatedFrame(symbols []byte) (*raftpb.LogRecord, error) {
	symbolCount := uint64(len(symbols))
	if symbolCount%encodedByteWidth != 0 {
		return nil, fmt.Errorf("odd interior symbol count %d", symbolCount)
	}
	bodySize := symbolCount / encodedByteWidth
	if bodySize < frameHeaderSize {
		return nil, fmt.Errorf("short decoded body: %d bytes, want at least %d", bodySize, frameHeaderSize)
	}

	headerSymbolCount, ok := checkedMulUint64(frameHeaderSize, encodedByteWidth)
	if !ok {
		return nil, errors.New("frame header symbol count overflow")
	}
	headerSymbolCountInt, err := checkedInt(headerSymbolCount)
	if err != nil {
		return nil, err
	}
	var header [8]byte
	if err := decodeSymbols(header[:], symbols[:headerSymbolCountInt]); err != nil {
		return nil, err
	}

	payloadSize := uint64(binary.LittleEndian.Uint32(header[:4]))
	wantBodySize, ok := checkedAddUint64(frameHeaderSize, payloadSize)
	if !ok {
		return nil, errors.New("decoded body size overflow")
	}
	if bodySize != wantBodySize {
		return nil, fmt.Errorf("decoded body size %d does not match header size %d + payload length %d", bodySize, frameHeaderSize, payloadSize)
	}
	payloadSizeInt, err := checkedInt(payloadSize)
	if err != nil {
		return nil, fmt.Errorf("payload length: %w", err)
	}
	payload := make([]byte, payloadSizeInt)
	if err := decodeSymbols(payload, symbols[headerSymbolCountInt:]); err != nil {
		return nil, err
	}

	storedCRC := binary.LittleEndian.Uint32(header[4:])
	if computed := crc32.Checksum(payload, castagnoli); computed != storedCRC {
		return nil, fmt.Errorf("crc32c mismatch: stored %08x, computed %08x", storedCRC, computed)
	}
	record := &raftpb.LogRecord{}
	if err := proto.Unmarshal(payload, record); err != nil {
		return nil, fmt.Errorf("unmarshal LogRecord: %w", err)
	}
	if !hasSupportedRecordKind(record) {
		return nil, errors.New("unknown record type: unsupported or empty LogRecord oneof")
	}
	return record, nil
}

func hasSupportedRecordKind(record *raftpb.LogRecord) bool {
	switch body := record.GetBody().(type) {
	case *raftpb.LogRecord_HardState:
		return body != nil && body.HardState != nil
	case *raftpb.LogRecord_Entries:
		return body != nil && body.Entries != nil
	case *raftpb.LogRecord_Truncate:
		return body != nil && body.Truncate != nil
	default:
		return false
	}
}

// applyRecord replays one record into the mirror, enforcing the same
// invariants Save writes under.
func (l *DiskLog) applyRecord(record *raftpb.LogRecord) error {
	switch body := record.GetBody().(type) {
	case *raftpb.LogRecord_HardState:
		l.hard = raft.HardState{
			Term:     body.HardState.GetTerm(),
			VotedFor: raft.NodeID(body.HardState.GetVotedFor()),
		}
	case *raftpb.LogRecord_Truncate:
		from := body.Truncate.GetFromIndex()
		if from == 0 {
			return errors.New("truncate record from index 0")
		}
		l.truncateEntries(from)
	case *raftpb.LogRecord_Entries:
		incoming := body.Entries.GetEntries()
		if len(incoming) == 0 {
			return errors.New("empty entries record")
		}
		last := l.lastIndexLocked()
		first := incoming[0].GetIndex()
		if first == 0 || first > last+1 {
			return fmt.Errorf("entries record first index %d does not extend last index %d", first, last)
		}
		for i := 1; i < len(incoming); i++ {
			if incoming[i].GetIndex() != incoming[i-1].GetIndex()+1 {
				return fmt.Errorf("non-contiguous entries record: index %d follows %d", incoming[i].GetIndex(), incoming[i-1].GetIndex())
			}
		}
		// An overlapping first index truncates implicitly — the same rule
		// Save applies to the mirror — before the replacements append.
		l.truncateEntries(incoming[0].GetIndex())
		for _, e := range incoming {
			l.entries = append(l.entries, raftpb.Entry{
				Index: e.GetIndex(),
				Term:  e.GetTerm(),
				Type:  e.GetType(),
				Data:  append([]byte(nil), e.GetData()...),
			})
		}
	default:
		return errors.New("unknown record type")
	}
	return nil
}

// truncateTail physically discards the sole recoverable tail shape: an
// unterminated final frame in the final segment. Complete invalid frames are
// corruption and never reach this function. The shrunken file is synced so
// discarded bytes cannot resurface after appends resume.
func (l *DiskLog) truncateTail(path string, seq uint64, offset int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("disklog: open %s to drop torn tail: %w", segmentName(seq), err)
	}
	if err := f.Truncate(offset); err != nil {
		_ = f.Close()
		return fmt.Errorf("disklog: truncate torn tail of %s at %d: %w", segmentName(seq), offset, err)
	}
	if err := l.syncFile(f); err != nil {
		_ = f.Close()
		return fmt.Errorf("disklog: sync %s after tail truncate: %w", segmentName(seq), err)
	}
	return f.Close()
}

// truncateEntries drops the mirrored suffix at index and above.
func (l *DiskLog) truncateEntries(index uint64) {
	cut := len(l.entries)
	for i := range l.entries {
		if l.entries[i].Index >= index {
			cut = i
			break
		}
	}
	tail := l.entries[cut:]
	for i := range tail {
		tail[i] = raftpb.Entry{}
	}
	l.entries = l.entries[:cut]
}

// checkContiguous rejects a batch whose entries do not overwrite or extend
// the stored log as one gapless ascending run; persisting a gap would make
// the log unrecoverable.
func checkContiguous(entries []raftpb.Entry, last uint64) error {
	if len(entries) == 0 {
		return nil
	}
	if first := entries[0].Index; first == 0 || first > last+1 {
		return fmt.Errorf("disklog: non-contiguous append: first index %d does not extend last index %d", first, last)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("disklog: non-contiguous batch: index %d follows %d", entries[i].Index, entries[i-1].Index)
		}
	}
	return nil
}

// encodeBatch frames the records of one Save in replay order: hard state,
// truncate (only when the entries overwrite an existing suffix), entries.
// Tearing can persist any record prefix of an unsynced batch; every prefix
// is a state Raft already tolerates, because Save had not returned and so
// nothing depending on the batch was sent.
func encodeBatch(hs *raft.HardState, entries []raftpb.Entry, last uint64) ([]byte, error) {
	var batch []byte
	var err error
	if hs != nil {
		record := &raftpb.HardStateRecord{Term: hs.Term, VotedFor: uint64(hs.VotedFor)}
		batch, err = appendFrame(batch, &raftpb.LogRecord{Body: &raftpb.LogRecord_HardState{HardState: record}})
		if err != nil {
			return nil, err
		}
	}
	if len(entries) == 0 {
		return batch, nil
	}
	if first := entries[0].Index; first <= last {
		record := &raftpb.TruncateRecord{FromIndex: first}
		batch, err = appendFrame(batch, &raftpb.LogRecord{Body: &raftpb.LogRecord_Truncate{Truncate: record}})
		if err != nil {
			return nil, err
		}
	}
	record := &raftpb.EntriesRecord{Entries: entryPointers(entries)}
	return appendFrame(batch, &raftpb.LogRecord{Body: &raftpb.LogRecord_Entries{Entries: record}})
}

// appendFrame appends 0xFA || E(B) || 0xFB, where B is the little-endian
// length and CRC32-C header followed by the protobuf payload. All sizing is
// checked in uint64 before converting to int or growing the batch.
func appendFrame(batch []byte, record *raftpb.LogRecord) ([]byte, error) {
	payload, err := proto.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("disklog: marshal record: %w", err)
	}
	if uint64(len(payload)) > math.MaxUint32 {
		return nil, fmt.Errorf("disklog: record payload %d bytes overflows frame length", len(payload))
	}
	physicalSize, ok := checkedFrameSize(uint64(len(payload)))
	if !ok {
		return nil, fmt.Errorf("disklog: record payload %d bytes overflows physical frame size", len(payload))
	}
	totalSize, ok := checkedAddUint64(uint64(len(batch)), physicalSize)
	if !ok {
		return nil, errors.New("disklog: batch size overflows uint64")
	}
	physicalSizeInt, err := checkedInt(physicalSize)
	if err != nil {
		return nil, fmt.Errorf("disklog: physical frame size: %w", err)
	}
	totalSizeInt, err := checkedInt(totalSize)
	if err != nil {
		return nil, fmt.Errorf("disklog: batch size: %w", err)
	}

	oldLen := len(batch)
	batch = slices.Grow(batch, physicalSizeInt)
	batch = batch[:totalSizeInt]
	frame := batch[oldLen:]
	frame[0] = frameStart
	frame[len(frame)-1] = frameEnd

	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:], crc32.Checksum(payload, castagnoli))
	bodySymbols := frame[1 : len(frame)-1]
	headerSymbols := bodySymbols[:len(header)*2]
	if err := encodeSymbols(headerSymbols, header[:]); err != nil {
		return nil, fmt.Errorf("disklog: encode frame header: %w", err)
	}
	if err := encodeSymbols(bodySymbols[len(headerSymbols):], payload); err != nil {
		return nil, fmt.Errorf("disklog: encode frame payload: %w", err)
	}
	return batch, nil
}

func encodeSymbols(dst, decoded []byte) error {
	want, ok := checkedMulUint64(uint64(len(decoded)), encodedByteWidth)
	if !ok || want != uint64(len(dst)) {
		return fmt.Errorf("encoded symbol buffer has %d bytes, want %d", len(dst), want)
	}
	for i, b := range decoded {
		dst[2*i] = frameSymbolBase + b>>4
		dst[2*i+1] = frameSymbolBase + b&0x0f
	}
	return nil
}

func decodeSymbols(dst, symbols []byte) error {
	want, ok := checkedMulUint64(uint64(len(dst)), encodedByteWidth)
	if !ok || want != uint64(len(symbols)) {
		return fmt.Errorf("interior has %d symbols, want %d", len(symbols), want)
	}
	for i := range dst {
		hi, lo := symbols[2*i], symbols[2*i+1]
		if !isFrameSymbol(hi) {
			return fmt.Errorf("invalid interior byte 0x%02x at symbol %d", hi, 2*i)
		}
		if !isFrameSymbol(lo) {
			return fmt.Errorf("invalid interior byte 0x%02x at symbol %d", lo, 2*i+1)
		}
		dst[i] = (hi-frameSymbolBase)<<4 | (lo - frameSymbolBase)
	}
	return nil
}

func checkedFrameSize(payloadSize uint64) (uint64, bool) {
	bodySize, ok := checkedAddUint64(frameHeaderSize, payloadSize)
	if !ok {
		return 0, false
	}
	encodedSize, ok := checkedMulUint64(bodySize, encodedByteWidth)
	if !ok {
		return 0, false
	}
	return checkedAddUint64(frameMarkerBytes, encodedSize)
}

func checkedAddUint64(a, b uint64) (uint64, bool) {
	if math.MaxUint64-a < b {
		return 0, false
	}
	return a + b, true
}

func checkedMulUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func checkedInt(n uint64) (int, error) {
	if !fitsIntBits(n, strconv.IntSize) {
		return 0, fmt.Errorf("size %d exceeds %d-bit int", n, strconv.IntSize)
	}
	return int(n), nil
}

func fitsIntBits(n uint64, bits int) bool {
	switch bits {
	case 32:
		return n <= math.MaxInt32
	case 64:
		return n <= math.MaxInt64
	default:
		return false
	}
}

// entryPointers builds the record's entries without copying generated
// messages by value; Data is aliased only for the marshal that immediately
// follows, never retained.
func entryPointers(entries []raftpb.Entry) []*raftpb.Entry {
	pointers := make([]*raftpb.Entry, 0, len(entries))
	for i := range entries {
		pointers = append(pointers, &raftpb.Entry{
			Index: entries[i].Index,
			Term:  entries[i].Term,
			Type:  entries[i].Type,
			Data:  entries[i].Data,
		})
	}
	return pointers
}

// listSegments returns the segment sequence numbers in dir. Compaction may
// delete a whole prefix, so the first sequence is arbitrary; every retained
// successor must still be contiguous. Foreign file names are ignored.
func listSegments(dir string) ([]uint64, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("disklog: read dir: %w", err)
	}
	var seqs []uint64
	for _, ent := range dirEntries {
		if seq, ok := parseSegmentName(ent.Name()); ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return nil, fmt.Errorf("%w: segment sequence gap: want %s, have %s", ErrCorrupt, segmentName(seqs[i-1]+1), segmentName(seqs[i]))
		}
	}
	return seqs, nil
}

func parseSegmentName(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, segmentSuffix)
	if !ok || len(digits) < 6 {
		return 0, false
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || seq == 0 {
		return 0, false
	}
	return seq, true
}

func segmentName(seq uint64) string {
	return fmt.Sprintf("%06d%s", seq, segmentSuffix)
}

func (l *DiskLog) segmentPath(seq uint64) string {
	return filepath.Join(l.dirPath, segmentName(seq))
}

func (l *DiskLog) firstLastLocked() (uint64, uint64) {
	entries := l.entries
	if len(entries) == 0 {
		return l.compacted.Index + 1, l.snapshot.Index
	}
	return entries[0].Index, max(entries[len(entries)-1].Index, l.snapshot.Index)
}

func (l *DiskLog) lastIndexLocked() uint64 {
	_, last := l.firstLastLocked()
	return last
}

func cloneEntries(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]raftpb.Entry, len(entries))
	for i := range entries {
		cloned[i] = raftpb.Entry{
			Index: entries[i].Index,
			Term:  entries[i].Term,
			Type:  entries[i].Type,
			Data:  append([]byte(nil), entries[i].Data...),
		}
	}
	return cloned
}
