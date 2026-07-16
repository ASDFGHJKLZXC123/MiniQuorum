package raft

import raftpb "miniquorum/proto"

// raftLog is the in-memory log owned by the deterministic core. Storage is
// deliberately absent: the host persists changed entries through Ready.
type raftLog struct {
	snapshot SnapshotMeta
	entries  []logEntry
}

// logEntry keeps generated protobuf runtime state out of the mutable in-memory
// log. raftpb.Entry values are constructed only at the Ready/message boundary;
// generated messages must not be copied after use.
type logEntry struct {
	index uint64
	term  uint64
	typ   raftpb.EntryType
	data  []byte
}

func newRaftLog(snapshot SnapshotMeta, entries []raftpb.Entry) raftLog {
	log := raftLog{snapshot: snapshot, entries: make([]logEntry, 0, len(entries))}
	for i := range entries {
		log.entries = append(log.entries, logEntry{
			index: entries[i].Index,
			term:  entries[i].Term,
			typ:   entries[i].Type,
			data:  append([]byte(nil), entries[i].Data...),
		})
	}
	return log
}

func (l *raftLog) firstIndex() uint64 { return l.snapshot.Index + 1 }

func (l *raftLog) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.snapshot.Index
	}
	return l.entries[len(l.entries)-1].index
}

func (l *raftLog) lastTerm() uint64 {
	term, _ := l.term(l.lastIndex())
	return term
}

func (l *raftLog) term(index uint64) (uint64, bool) {
	if index == l.snapshot.Index {
		return l.snapshot.Term, true
	}
	entry := l.entry(index)
	if entry == nil {
		return 0, false
	}
	return entry.term, true
}

func (l *raftLog) matches(index, term uint64) bool {
	localTerm, ok := l.term(index)
	return ok && localTerm == term
}

func (l *raftLog) entry(index uint64) *logEntry {
	if len(l.entries) == 0 || index < l.entries[0].index {
		return nil
	}
	offset := index - l.entries[0].index
	if offset >= uint64(len(l.entries)) || l.entries[offset].index != index {
		return nil
	}
	return &l.entries[offset]
}

// rangeEntries returns deep copies from the half-open interval [lo, hi).
func (l *raftLog) rangeEntries(lo, hi uint64) []raftpb.Entry {
	if lo >= hi {
		return nil
	}
	entries := make([]raftpb.Entry, 0, hi-lo)
	for index := lo; index < hi; index++ {
		entry := l.entry(index)
		if entry == nil {
			break
		}
		entries = append(entries, raftpb.Entry{
			Index: entry.index,
			Term:  entry.term,
			Type:  entry.typ,
			Data:  append([]byte(nil), entry.data...),
		})
	}
	return entries
}

func (l *raftLog) appendLocal(term uint64, typ raftpb.EntryType, data []byte) uint64 {
	index := l.lastIndex() + 1
	l.entries = append(l.entries, logEntry{
		index: index,
		term:  term,
		typ:   typ,
		data:  append([]byte(nil), data...),
	})
	return index
}

// appendFromLeader applies the Raft conflict rule. Matching entries are kept;
// the first different term truncates the local suffix and replaces it with the
// leader's suffix. It returns the first changed index, or zero when the local
// log was already identical for every supplied entry.
func (l *raftLog) appendFromLeader(entries []*raftpb.Entry) uint64 {
	for i, incoming := range entries {
		if incoming == nil || incoming.Index <= l.snapshot.Index {
			continue
		}
		localTerm, exists := l.term(incoming.Index)
		if exists && localTerm == incoming.Term {
			continue
		}

		l.truncateFrom(incoming.Index)
		firstChanged := incoming.Index
		for _, remaining := range entries[i:] {
			if remaining == nil || remaining.Index <= l.snapshot.Index {
				continue
			}
			l.entries = append(l.entries, logEntry{
				index: remaining.Index,
				term:  remaining.Term,
				typ:   remaining.Type,
				data:  append([]byte(nil), remaining.Data...),
			})
		}
		return firstChanged
	}
	return 0
}

func (l *raftLog) truncateFrom(index uint64) {
	cut := len(l.entries)
	for i := range l.entries {
		if l.entries[i].index >= index {
			cut = i
			break
		}
	}
	for i := cut; i < len(l.entries); i++ {
		l.entries[i] = logEntry{}
	}
	l.entries = l.entries[:cut]
}

func entryPointers(entries []raftpb.Entry) []*raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	pointers := make([]*raftpb.Entry, 0, len(entries))
	for i := range entries {
		pointers = append(pointers, &raftpb.Entry{
			Index: entries[i].Index,
			Term:  entries[i].Term,
			Type:  entries[i].Type,
			Data:  append([]byte(nil), entries[i].Data...),
		})
	}
	return pointers
}
