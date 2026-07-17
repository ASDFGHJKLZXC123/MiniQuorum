package sim

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

// CrashStorage is the Phase 3 sim storage crash model: an in-memory
// storage.Storage that models the disklog's sync boundary at byte
// granularity. Every Save encodes its batch as framed records in the
// disklog's exact replay order — a hard-state record, a logical truncate
// record when the entries overwrite an existing suffix, then an entries
// record — into an unsynced (dirty) buffer, then moves them to the durable
// byte log at the sync barrier. A crash keeps durable bytes exactly as
// written (synced records are never torn) and keeps only the scheduled
// prefix of the dirty buffer, so a cut can land mid-record: a torn tail.
//
// Recovery scans the surviving bytes front to back, folds each complete
// record into a fresh view in record order (a truncate record drops the
// suffix from its index; an entries record logically truncates the suffix
// from its first index, exactly like Storage.Save), and drops a torn final
// record — the disklog torn-tail policy. A surviving complete truncate
// record whose entries record tore away therefore recovers to a durably
// shortened old log: the intermediate crash state the disklog's record
// order makes reachable. Everything is single-threaded and derives from
// plain data: no wall clock, no goroutines, no map iteration, no ambient
// randomness anywhere in the model.
//
// While crashed, the read side keeps serving the surviving platter image so
// the sim's omniscient invariants can inspect a down node's disk; Save fails
// with ErrCrashed until Recover.
type CrashStorage struct {
	directives []CrashDirective // this node's directives, sorted by Save ordinal
	next       int              // index of the next unconsumed directive
	saves      uint64           // accepted Save calls so far
	durable    []byte           // synced record bytes; the platter
	unsynced   []byte           // dirty record bytes, non-empty only inside Save
	view       *storage.MemStorage
	crashed    bool
	armed      bool   // a CrashAfterSend directive fired at this save...
	armedSave  uint64 // ...and this is its ordinal
	lastCrash  CrashInfo
	hasCrashed bool
}

var _ storage.Storage = (*CrashStorage)(nil)

var errNotCrashed = errors.New("sim: Recover called on a storage that is not crashed")

// NewCrashStorage builds the crash-model storage for node id, keeping the
// directives schedule.For(id) sorted by Save ordinal. It rejects ordinal 0,
// non-schedulable points, retentions below RetainAllUnsynced, and duplicate
// ordinals for the node. The caller's schedule is never mutated.
func NewCrashStorage(id raft.NodeID, schedule FaultSchedule) (*CrashStorage, error) {
	directives := schedule.For(id)
	sort.Slice(directives, func(i, j int) bool { return directives[i].Save < directives[j].Save })
	for i := range directives {
		d := directives[i]
		if d.Save == 0 {
			return nil, fmt.Errorf("sim: crash directive for node %d has Save 0; ordinals are 1-based", id)
		}
		if d.Point < CrashBeforeSync || d.Point > CrashAfterSend {
			return nil, fmt.Errorf("sim: crash directive for node %d save %d has non-schedulable point %s", id, d.Save, d.Point)
		}
		if d.RetainUnsynced < RetainAllUnsynced {
			return nil, fmt.Errorf("sim: crash directive for node %d save %d retains %d bytes; minimum is RetainAllUnsynced (-1)", id, d.Save, d.RetainUnsynced)
		}
		if i > 0 && directives[i-1].Save == d.Save {
			return nil, fmt.Errorf("sim: duplicate crash directives for node %d at save %d", id, d.Save)
		}
	}
	return &CrashStorage{directives: directives, view: storage.NewMemStorage()}, nil
}

// Save implements storage.Storage with the crash model spliced into the
// batch's lifecycle: stage dirty records, hit a scheduled CrashBeforeSync,
// cross the sync barrier, hit a scheduled CrashAfterSyncBeforeSend, or
// return success (arming a CrashAfterSend directive for the host to fire
// after it sends). A crashed storage rejects Save with ErrCrashed without
// consuming an ordinal.
func (cs *CrashStorage) Save(hs *raft.HardState, entries []raftpb.Entry) error {
	if cs.crashed {
		return ErrCrashed
	}
	if cs.armed {
		// The host was contracted to fire the armed after-send crash before
		// any further storage use; fire it now as a deterministic backstop.
		// Survivors are identical (the armed batch was fully durable) and
		// this batch dies before staging, so fail-stop still holds.
		save, point := cs.armedSave, CrashAfterSend
		cs.crash(save, point, 0)
		return fmt.Errorf("sim: crash at save %d, %s: %w", save, point, ErrCrashed)
	}
	cs.saves++
	if hs != nil {
		cs.unsynced = appendHardStateRecord(cs.unsynced, *hs)
	}
	if len(entries) > 0 {
		// An overlapping batch stages a logical truncate record before its
		// entries, the disklog's exact order, so a tear inside the entries
		// record leaves the old suffix durably truncated with nothing in its
		// place. Index 0 never stages one: the disklog rejects it before
		// encoding, and here the entries record's own implicit cut covers it.
		if first := entries[0].Index; first != 0 && first <= cs.view.LastIndex() {
			cs.unsynced = appendTruncateRecord(cs.unsynced, first)
		}
		cs.unsynced = appendEntriesRecord(cs.unsynced, entries)
	}
	directive, ok := cs.pendingDirective(cs.saves)
	if ok && directive.Point == CrashBeforeSync {
		cs.crash(cs.saves, CrashBeforeSync, directive.RetainUnsynced)
		return fmt.Errorf("sim: crash at save %d, %s: %w", cs.saves, CrashBeforeSync, ErrCrashed)
	}
	// The sync barrier: every staged byte becomes durable in one step, and
	// the live view materializes the batch. MemStorage.Save never fails.
	cs.durable = append(cs.durable, cs.unsynced...)
	cs.unsynced = nil
	_ = cs.view.Save(hs, entries)
	if ok {
		switch directive.Point {
		case CrashAfterSyncBeforeSend:
			cs.crash(cs.saves, CrashAfterSyncBeforeSend, 0)
			return fmt.Errorf("sim: crash at save %d, %s: %w", cs.saves, CrashAfterSyncBeforeSend, ErrCrashed)
		case CrashAfterSend:
			cs.armed = true
			cs.armedSave = cs.saves
		}
	}
	return nil
}

// HardState implements storage.Storage; while crashed it reports the
// surviving platter image.
func (cs *CrashStorage) HardState() (raft.HardState, error) { return cs.view.HardState() }

// Entries implements storage.Storage with MemStorage's exact bounds
// semantics; while crashed it reports the surviving platter image.
func (cs *CrashStorage) Entries(lo, hi uint64) ([]raftpb.Entry, error) { return cs.view.Entries(lo, hi) }

// FirstIndex implements storage.Storage.
func (cs *CrashStorage) FirstIndex() uint64 { return cs.view.FirstIndex() }

// LastIndex implements storage.Storage.
func (cs *CrashStorage) LastIndex() uint64 { return cs.view.LastIndex() }

// Crash kills the storage between batches, as a virtual-time-scheduled kill
// would: the dirty buffer is empty, so exactly the durable bytes survive.
// It reports ErrCrashed if the storage is already down.
func (cs *CrashStorage) Crash() error {
	if cs.crashed {
		return ErrCrashed
	}
	cs.crash(cs.saves, CrashHostInitiated, 0)
	return nil
}

// CrashIfArmed fires the pending CrashAfterSend crash, if one is armed, and
// reports whether it fired. Hosts call it right after sending an armed
// batch's messages, before applying anything.
func (cs *CrashStorage) CrashIfArmed() bool {
	if cs.crashed || !cs.armed {
		return false
	}
	cs.crash(cs.armedSave, CrashAfterSend, 0)
	return true
}

// CrashArmed reports whether a CrashAfterSend directive is waiting for the
// host to fire it.
func (cs *CrashStorage) CrashArmed() bool { return cs.armed }

// Crashed reports whether the storage is down.
func (cs *CrashStorage) Crashed() bool { return cs.crashed }

// SaveCount returns the number of accepted Save calls; the next accepted
// Save gets ordinal SaveCount()+1.
func (cs *CrashStorage) SaveCount() uint64 { return cs.saves }

// LastCrash returns what the most recent crash did, if any has happened.
func (cs *CrashStorage) LastCrash() (CrashInfo, bool) { return cs.lastCrash, cs.hasCrashed }

// DurableBytes returns a copy of the platter: while crashed it includes any
// torn tail the retention cut left behind; after Recover the tail is
// trimmed to complete records, mirroring the disklog torn-tail policy.
func (cs *CrashStorage) DurableBytes() []byte { return append([]byte(nil), cs.durable...) }

// Recover brings a crashed storage back: it rebuilds the read view by
// folding the surviving complete records in byte order and physically trims
// a torn final record so later Saves append after a clean tail. Recovery
// consults nothing but the surviving bytes.
func (cs *CrashStorage) Recover() error {
	if !cs.crashed {
		return errNotCrashed
	}
	view, complete, err := parseRecords(cs.durable)
	if err != nil {
		return err
	}
	cs.durable = cs.durable[:complete]
	cs.view = view
	cs.crashed = false
	return nil
}

// pendingDirective consumes and returns the directive scheduled for the
// given Save ordinal, if any. Directives are sorted and ordinals visit every
// integer once, so only the next unconsumed directive can match.
func (cs *CrashStorage) pendingDirective(save uint64) (CrashDirective, bool) {
	if cs.next >= len(cs.directives) || cs.directives[cs.next].Save != save {
		return CrashDirective{}, false
	}
	d := cs.directives[cs.next]
	cs.next++
	return d, true
}

// crash applies retention to the dirty buffer, fixes the surviving platter
// bytes, and swaps the read view to their parse. retain follows
// CrashDirective.RetainUnsynced semantics.
func (cs *CrashStorage) crash(save uint64, point CrashPoint, retain int) {
	keep := len(cs.unsynced)
	if retain != RetainAllUnsynced && retain < keep {
		keep = retain
	}
	cs.lastCrash = CrashInfo{Save: save, Point: point, UnsyncedBytes: len(cs.unsynced), RetainedBytes: keep}
	cs.hasCrashed = true
	cs.durable = append(cs.durable, cs.unsynced[:keep]...)
	cs.unsynced = nil
	cs.crashed = true
	cs.armed = false
	view, _, err := parseRecords(cs.durable)
	if err != nil {
		// Unreachable by construction: survivors are a byte prefix of
		// records this storage encoded, and any prefix parses.
		panic(fmt.Sprintf("sim: crash storage survivor parse failed: %v", err))
	}
	cs.view = view
}

// Record framing mirrors the disklog's [len][crc32c][payload] shape with a
// sim-local deterministic payload encoding (fixed-width little-endian; no
// protobuf, so this packet does not touch proto/). One Save stages at most
// three records, in the disklog's replay order: hard state, truncate (only
// when the batch overwrites an existing suffix), then entries.
const recordHeaderLen = 8

const (
	recordKindHardState byte = 1
	recordKindEntries   byte = 2
	recordKindTruncate  byte = 3
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func appendRecord(dst, payload []byte) []byte {
	var header [recordHeaderLen]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.Checksum(payload, castagnoli))
	return append(append(dst, header[:]...), payload...)
}

func appendHardStateRecord(dst []byte, hs raft.HardState) []byte {
	payload := make([]byte, 17)
	payload[0] = recordKindHardState
	binary.LittleEndian.PutUint64(payload[1:9], hs.Term)
	binary.LittleEndian.PutUint64(payload[9:17], uint64(hs.VotedFor))
	return appendRecord(dst, payload)
}

func appendTruncateRecord(dst []byte, from uint64) []byte {
	payload := make([]byte, 9)
	payload[0] = recordKindTruncate
	binary.LittleEndian.PutUint64(payload[1:9], from)
	return appendRecord(dst, payload)
}

func appendEntriesRecord(dst []byte, entries []raftpb.Entry) []byte {
	payload := []byte{recordKindEntries, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(payload[1:5], uint32(len(entries)))
	for i := range entries {
		var fixed [24]byte
		binary.LittleEndian.PutUint64(fixed[0:8], entries[i].Index)
		binary.LittleEndian.PutUint64(fixed[8:16], entries[i].Term)
		binary.LittleEndian.PutUint32(fixed[16:20], uint32(entries[i].Type))
		binary.LittleEndian.PutUint32(fixed[20:24], uint32(len(entries[i].Data)))
		payload = append(append(payload, fixed[:]...), entries[i].Data...)
	}
	return appendRecord(dst, payload)
}

// parseRecords folds every complete record in data, in byte order, into a
// fresh view, and returns the byte length of that complete prefix. Trailing
// bytes that do not form a whole record are a torn tail: ignored here,
// trimmed by Recover. The fold applies hard-state and entries records
// exactly as Storage.Save would (an entries record truncates the suffix from
// its first index) and honors a complete truncate record by itself — even
// when the entries record that followed it tore away.
func parseRecords(data []byte) (*storage.MemStorage, int, error) {
	var state recoveredState
	offset := 0
	for {
		rest := data[offset:]
		if len(rest) < recordHeaderLen {
			return state.view(), offset, nil
		}
		payloadLen := int(binary.LittleEndian.Uint32(rest[0:4]))
		if len(rest) < recordHeaderLen+payloadLen {
			return state.view(), offset, nil
		}
		payload := rest[recordHeaderLen : recordHeaderLen+payloadLen]
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(rest[4:8]) {
			return nil, 0, fmt.Errorf("sim: record at byte %d fails crc32c; synced bytes are never torn, so this is a codec bug", offset)
		}
		if err := state.apply(payload); err != nil {
			return nil, 0, err
		}
		offset += recordHeaderLen + payloadLen
	}
}

// recoveredState is the record fold target. It holds plain hard state and
// entries because a truncate record is a pure suffix drop, which
// Storage.Save cannot express; the other record kinds fold with Save's exact
// cut-and-append semantics, so the materialized view is identical to folding
// them through a MemStorage.
type recoveredState struct {
	hard    raft.HardState
	entries []raftpb.Entry
}

func (s *recoveredState) apply(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("sim: empty record payload")
	}
	switch payload[0] {
	case recordKindHardState:
		if len(payload) != 17 {
			return fmt.Errorf("sim: hard-state record payload is %d bytes, want 17", len(payload))
		}
		s.hard = raft.HardState{
			Term:     binary.LittleEndian.Uint64(payload[1:9]),
			VotedFor: raft.NodeID(binary.LittleEndian.Uint64(payload[9:17])),
		}
		return nil
	case recordKindTruncate:
		if len(payload) != 9 {
			return fmt.Errorf("sim: truncate record payload is %d bytes, want 9", len(payload))
		}
		from := binary.LittleEndian.Uint64(payload[1:9])
		if from == 0 {
			return errors.New("sim: truncate record from index 0")
		}
		s.entries = truncateFromIndex(s.entries, from)
		return nil
	case recordKindEntries:
		entries, err := decodeEntriesBody(payload[1:])
		if err != nil {
			return err
		}
		s.entries = append(truncateFromIndex(s.entries, entries[0].Index), entries...)
		return nil
	default:
		return fmt.Errorf("sim: unknown record kind %d", payload[0])
	}
}

// view materializes the folded state as a MemStorage so reads keep the
// frozen bounds semantics. MemStorage.Save never fails.
func (s *recoveredState) view() *storage.MemStorage {
	view := storage.NewMemStorage()
	_ = view.Save(&s.hard, nil)
	if len(s.entries) > 0 {
		_ = view.Save(nil, s.entries)
	}
	return view
}

// truncateFromIndex drops the suffix at index and above: the cut
// Storage.Save applies before appending an overlapping batch, and the whole
// effect of a truncate record.
func truncateFromIndex(entries []raftpb.Entry, index uint64) []raftpb.Entry {
	cut := len(entries)
	for i := range entries {
		if entries[i].Index >= index {
			cut = i
			break
		}
	}
	return entries[:cut]
}

func decodeEntriesBody(body []byte) ([]raftpb.Entry, error) {
	if len(body) < 4 {
		return nil, errors.New("sim: entries record shorter than its count")
	}
	count := int(binary.LittleEndian.Uint32(body[0:4]))
	body = body[4:]
	if count == 0 {
		return nil, errors.New("sim: entries record with zero entries")
	}
	entries := make([]raftpb.Entry, 0, count)
	for i := 0; i < count; i++ {
		if len(body) < 24 {
			return nil, fmt.Errorf("sim: entries record truncated at entry %d", i)
		}
		dataLen := int(binary.LittleEndian.Uint32(body[20:24]))
		if len(body) < 24+dataLen {
			return nil, fmt.Errorf("sim: entries record truncated inside entry %d data", i)
		}
		var data []byte
		if dataLen > 0 {
			data = append([]byte(nil), body[24:24+dataLen]...)
		}
		entries = append(entries, raftpb.Entry{
			Index: binary.LittleEndian.Uint64(body[0:8]),
			Term:  binary.LittleEndian.Uint64(body[8:16]),
			Type:  raftpb.EntryType(int32(binary.LittleEndian.Uint32(body[16:20]))),
			Data:  data,
		})
		body = body[24+dataLen:]
	}
	if len(body) != 0 {
		return nil, fmt.Errorf("sim: entries record has %d trailing bytes", len(body))
	}
	return entries, nil
}
