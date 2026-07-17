package sim

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

// crashAt builds the one-node, one-directive schedule most tests use.
func crashAt(save uint64, point CrashPoint, retain int) FaultSchedule {
	return FaultSchedule{Crashes: []CrashDirective{{Node: 1, Save: save, Point: point, RetainUnsynced: retain}}}
}

func mustCrashStorage(t *testing.T, id raft.NodeID, schedule FaultSchedule) *CrashStorage {
	t.Helper()
	cs, err := NewCrashStorage(id, schedule)
	if err != nil {
		t.Fatalf("NewCrashStorage() error = %v", err)
	}
	return cs
}

func crashTestEntries(firstIndex, term uint64, payloads ...string) []raftpb.Entry {
	entries := make([]raftpb.Entry, len(payloads))
	for i := range payloads {
		entries[i] = raftpb.Entry{Index: firstIndex + uint64(i), Term: term, Type: raftpb.EntryType_NORMAL, Data: []byte(payloads[i])}
	}
	return entries
}

func mustSaveOK(t *testing.T, cs *CrashStorage, hs *raft.HardState, entries []raftpb.Entry) {
	t.Helper()
	if err := cs.Save(hs, entries); err != nil {
		t.Fatalf("Save() error = %v, want nil", err)
	}
}

// storedLog reads the full retained log back through the frozen interface.
func storedLog(t *testing.T, cs *CrashStorage) []raftpb.Entry {
	t.Helper()
	first, last := cs.FirstIndex(), cs.LastIndex()
	if last < first {
		return nil
	}
	entries, err := cs.Entries(first, last+1)
	if err != nil {
		t.Fatalf("Entries(%d, %d) error = %v", first, last+1, err)
	}
	return entries
}

func crashLogsEqual(a, b []raftpb.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameEntry(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

func crashLogString(entries []raftpb.Entry) string {
	parts := make([]string, len(entries))
	for i := range entries {
		parts[i] = formatEntry(&entries[i])
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// recordBytesFor measures the framed byte length a batch contributes by
// saving it alone on a scratch storage, so no framing constants leak into
// test expectations.
func recordBytesFor(t *testing.T, hs *raft.HardState, entries []raftpb.Entry) int {
	t.Helper()
	scratch := mustCrashStorage(t, 1, FaultSchedule{})
	mustSaveOK(t, scratch, hs, entries)
	return len(scratch.DurableBytes())
}

// TestCrashStorageSaveReadRoundTripMatchesStorageContract pins CrashStorage
// to the frozen storage.Storage semantics: empty-log index conventions,
// hard-state and entries round-trips, suffix truncation from the first
// supplied index, and MemStorage's exact Entries bounds behavior.
func TestCrashStorageSaveReadRoundTripMatchesStorageContract(t *testing.T) {
	cs := mustCrashStorage(t, 1, FaultSchedule{})
	if got := cs.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex() = %d, want 1 for an empty log", got)
	}
	if got := cs.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() = %d, want 0 for an empty log", got)
	}
	mustSaveOK(t, cs, &raft.HardState{Term: 3, VotedFor: 2}, crashTestEntries(1, 3, "a", "b", "c"))
	hs, err := cs.HardState()
	if err != nil || hs != (raft.HardState{Term: 3, VotedFor: 2}) {
		t.Fatalf("HardState() = %+v, %v, want ({Term:3 VotedFor:2}, nil)", hs, err)
	}
	if got, want := storedLog(t, cs), crashTestEntries(1, 3, "a", "b", "c"); !crashLogsEqual(got, want) {
		t.Fatalf("stored log = %s, want %s", crashLogString(got), crashLogString(want))
	}
	// A batch overlapping the log truncates the suffix from its first index.
	mustSaveOK(t, cs, nil, crashTestEntries(2, 4, "x"))
	want := []raftpb.Entry{
		{Index: 1, Term: 3, Type: raftpb.EntryType_NORMAL, Data: []byte("a")},
		{Index: 2, Term: 4, Type: raftpb.EntryType_NORMAL, Data: []byte("x")},
	}
	if got := storedLog(t, cs); !crashLogsEqual(got, want) {
		t.Fatalf("truncated log = %s, want %s", crashLogString(got), crashLogString(want))
	}
	if _, err := cs.Entries(0, 1); !errors.Is(err, storage.ErrOutOfBounds) {
		t.Fatalf("Entries(0, 1) error = %v, want ErrOutOfBounds", err)
	}
	if _, err := cs.Entries(1, 4); !errors.Is(err, storage.ErrOutOfBounds) {
		t.Fatalf("Entries(1, 4) error = %v, want ErrOutOfBounds", err)
	}
}

// TestCrashStorageDirtyBatchDurableOnlyAtSyncBarrier is the dirty-versus-
// durable core: a batch staged before the sync barrier can vanish entirely
// (retain none) or survive entirely without ever being synced (retain all),
// while the previously synced batch is untouchable either way.
func TestCrashStorageDirtyBatchDurableOnlyAtSyncBarrier(t *testing.T) {
	base := crashTestEntries(1, 1, "alpha", "bravo")
	cases := []struct {
		name     string
		retain   int
		wantHS   raft.HardState
		wantLast uint64
	}{
		{"retain none loses the whole dirty batch", 0, raft.HardState{Term: 1}, 2},
		{"retain all keeps the whole dirty batch", RetainAllUnsynced, raft.HardState{Term: 2, VotedFor: 3}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := mustCrashStorage(t, 1, crashAt(2, CrashBeforeSync, tc.retain))
			mustSaveOK(t, cs, &raft.HardState{Term: 1}, base)
			err := cs.Save(&raft.HardState{Term: 2, VotedFor: 3}, crashTestEntries(3, 2, "charlie"))
			if !errors.Is(err, ErrCrashed) {
				t.Fatalf("Save() error = %v, want ErrCrashed", err)
			}
			info, ok := cs.LastCrash()
			if !ok || info.Save != 2 || info.Point != CrashBeforeSync || info.UnsyncedBytes == 0 {
				t.Fatalf("LastCrash() = %+v, %t, want save 2 before-sync with dirty bytes", info, ok)
			}
			wantRetained := 0
			if tc.retain == RetainAllUnsynced {
				wantRetained = info.UnsyncedBytes
			}
			if info.RetainedBytes != wantRetained {
				t.Fatalf("RetainedBytes = %d, want %d", info.RetainedBytes, wantRetained)
			}
			if err := cs.Recover(); err != nil {
				t.Fatalf("Recover() error = %v", err)
			}
			hs, _ := cs.HardState()
			if hs != tc.wantHS || cs.LastIndex() != tc.wantLast {
				t.Fatalf("recovered (HardState, LastIndex) = (%+v, %d), want (%+v, %d)", hs, cs.LastIndex(), tc.wantHS, tc.wantLast)
			}
			if got := storedLog(t, cs); len(got) < 2 || !sameEntry(&got[0], &base[0]) || !sameEntry(&got[1], &base[1]) {
				t.Fatalf("recovered log %s lost the synced batch %s", crashLogString(got), crashLogString(base))
			}
		})
	}
}

// TestCrashStorageEveryCrashPointIsSchedulable drives one identical Save
// sequence into each of the three schedulable crash points and asserts each
// point's distinct observable contract: whether Save errors, whether the
// batch is durable after recovery, and how an after-send crash arms and fires.
func TestCrashStorageEveryCrashPointIsSchedulable(t *testing.T) {
	cases := []struct {
		name             string
		point            CrashPoint
		wantSaveErr      bool
		wantBatchDurable bool
	}{
		{"before-sync", CrashBeforeSync, true, false},
		{"after-sync-before-send", CrashAfterSyncBeforeSend, true, true},
		{"after-send", CrashAfterSend, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := mustCrashStorage(t, 1, crashAt(2, tc.point, 0))
			mustSaveOK(t, cs, &raft.HardState{Term: 1, VotedFor: 1}, crashTestEntries(1, 1, "alpha"))
			err := cs.Save(&raft.HardState{Term: 2, VotedFor: 2}, crashTestEntries(2, 2, "bravo"))
			if tc.wantSaveErr {
				if !errors.Is(err, ErrCrashed) {
					t.Fatalf("Save() error = %v, want ErrCrashed", err)
				}
				if !cs.Crashed() {
					t.Fatal("Crashed() = false after a scheduled crash, want true")
				}
				if cs.CrashIfArmed() {
					t.Fatal("CrashIfArmed() = true on an unarmed storage, want false")
				}
			} else {
				if err != nil {
					t.Fatalf("Save() error = %v, want nil (after-send crashes fire only after the host sends)", err)
				}
				if cs.Crashed() || !cs.CrashArmed() {
					t.Fatalf("(Crashed, CrashArmed) = (%t, %t), want (false, true) before the host fires", cs.Crashed(), cs.CrashArmed())
				}
				// The host sends the batch's messages, then fires the crash.
				if !cs.CrashIfArmed() {
					t.Fatal("CrashIfArmed() = false, want true")
				}
				if !cs.Crashed() || cs.CrashArmed() {
					t.Fatalf("(Crashed, CrashArmed) = (%t, %t) after firing, want (true, false)", cs.Crashed(), cs.CrashArmed())
				}
			}
			info, ok := cs.LastCrash()
			if !ok || info.Save != 2 || info.Point != tc.point {
				t.Fatalf("LastCrash() = %+v, %t, want save 2 at %s", info, ok, tc.point)
			}
			// A dead storage accepts no writes and consumes no ordinals.
			if err := cs.Save(nil, nil); !errors.Is(err, ErrCrashed) {
				t.Fatalf("Save() on a crashed storage error = %v, want ErrCrashed", err)
			}
			if got := cs.SaveCount(); got != 2 {
				t.Fatalf("SaveCount() = %d, want 2", got)
			}
			if err := cs.Recover(); err != nil {
				t.Fatalf("Recover() error = %v", err)
			}
			wantHS, wantLast := raft.HardState{Term: 1, VotedFor: 1}, uint64(1)
			if tc.wantBatchDurable {
				wantHS, wantLast = raft.HardState{Term: 2, VotedFor: 2}, 2
			}
			hs, _ := cs.HardState()
			if hs != wantHS || cs.LastIndex() != wantLast {
				t.Fatalf("recovered (HardState, LastIndex) = (%+v, %d), want (%+v, %d)", hs, cs.LastIndex(), wantHS, wantLast)
			}
		})
	}
}

// TestCrashStorageTornTailEveryPrefixCutOfUnsyncedBytes cuts the dirty
// buffer at every byte offset of a two-record batch (hard state, then
// entries). Survivors must be exactly the complete records under the cut:
// nothing new below the hard-state record boundary, hard state without
// entries in between (a mid-record torn tail), the full batch only at the
// full length — and the synced prefix must stay byte-identical throughout.
func TestCrashStorageTornTailEveryPrefixCutOfUnsyncedBytes(t *testing.T) {
	hs2 := raft.HardState{Term: 2, VotedFor: 3}
	batch2 := crashTestEntries(2, 2, "delta", "echo")
	hsRecordLen := recordBytesFor(t, &hs2, nil)
	entriesRecordLen := recordBytesFor(t, nil, batch2)
	total := hsRecordLen + entriesRecordLen

	for cut := 0; cut <= total; cut++ {
		cs := mustCrashStorage(t, 1, crashAt(2, CrashBeforeSync, cut))
		mustSaveOK(t, cs, &raft.HardState{Term: 1, VotedFor: 1}, crashTestEntries(1, 1, "alpha"))
		syncedImage := cs.DurableBytes()
		if err := cs.Save(&hs2, batch2); !errors.Is(err, ErrCrashed) {
			t.Fatalf("cut %d: Save() error = %v, want ErrCrashed", cut, err)
		}
		info, _ := cs.LastCrash()
		if info.UnsyncedBytes != total || info.RetainedBytes != cut {
			t.Fatalf("cut %d: LastCrash() = %+v, want %d dirty bytes with %d retained", cut, info, total, cut)
		}
		platter := cs.DurableBytes()
		if len(platter) != len(syncedImage)+cut {
			t.Fatalf("cut %d: platter = %d bytes, want %d", cut, len(platter), len(syncedImage)+cut)
		}
		if !bytes.Equal(platter[:len(syncedImage)], syncedImage) {
			t.Fatalf("cut %d: synced prefix changed on the platter, want byte-identical", cut)
		}
		if err := cs.Recover(); err != nil {
			t.Fatalf("cut %d: Recover() error = %v", cut, err)
		}
		wantHS, wantLast, wantDurable := raft.HardState{Term: 1, VotedFor: 1}, uint64(1), len(syncedImage)
		if cut >= hsRecordLen {
			wantHS = hs2 // the hard-state record survived complete
			wantDurable += hsRecordLen
		}
		if cut == total {
			wantLast = 3 // the entries record survived complete
			wantDurable += entriesRecordLen
		}
		hs, _ := cs.HardState()
		if hs != wantHS || cs.LastIndex() != wantLast {
			t.Fatalf("cut %d: recovered (HardState, LastIndex) = (%+v, %d), want (%+v, %d)", cut, hs, cs.LastIndex(), wantHS, wantLast)
		}
		if got := len(cs.DurableBytes()); got != wantDurable {
			t.Fatalf("cut %d: durable after recovery = %d bytes, want %d (torn tail trimmed)", cut, got, wantDurable)
		}
	}
}

// TestCrashStorageRetentionClampAndRetainAllEquivalence pins the pure-data
// retention semantics seeded harnesses rely on: a retention beyond the dirty
// buffer clamps to it, surviving byte-identically to RetainAllUnsynced.
func TestCrashStorageRetentionClampAndRetainAllEquivalence(t *testing.T) {
	run := func(retain int) []byte {
		cs := mustCrashStorage(t, 1, crashAt(1, CrashBeforeSync, retain))
		if err := cs.Save(&raft.HardState{Term: 1, VotedFor: 1}, crashTestEntries(1, 1, "alpha")); !errors.Is(err, ErrCrashed) {
			t.Fatalf("Save() error = %v, want ErrCrashed", err)
		}
		if err := cs.Recover(); err != nil {
			t.Fatalf("Recover() error = %v", err)
		}
		return cs.DurableBytes()
	}
	all := run(RetainAllUnsynced)
	if len(all) == 0 {
		t.Fatal("retain-all survivors are empty, want the full batch")
	}
	if clamped := run(len(all) + 4096); !bytes.Equal(clamped, all) {
		t.Fatal("retention beyond the dirty buffer did not clamp to retain-all")
	}
}

// TestCrashStorageSyncedRecordsSurviveMidRecordCutExactly is the synced-
// immunity gate: after several synced batches, a crash whose retention cut
// tears the new batch must leave every previously synced byte, the hard
// state, and the whole log exactly as they were — durable means durable.
func TestCrashStorageSyncedRecordsSurviveMidRecordCutExactly(t *testing.T) {
	cs := mustCrashStorage(t, 1, crashAt(4, CrashBeforeSync, 3))
	mustSaveOK(t, cs, &raft.HardState{Term: 1, VotedFor: 2}, crashTestEntries(1, 1, "alpha"))
	mustSaveOK(t, cs, nil, crashTestEntries(2, 1, "bravo", "charlie"))
	mustSaveOK(t, cs, &raft.HardState{Term: 2}, nil)
	syncedImage := cs.DurableBytes()
	wantHS := raft.HardState{Term: 2}
	wantLog := storedLog(t, cs)

	if err := cs.Save(nil, crashTestEntries(4, 2, "delta")); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Save() error = %v, want ErrCrashed", err)
	}
	if err := cs.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	// Three retained bytes cannot even hold a record header, so recovery
	// trims them and the platter is byte-identical to the synced image.
	if !bytes.Equal(cs.DurableBytes(), syncedImage) {
		t.Fatal("synced platter bytes changed across crash and recovery, want byte-identical")
	}
	hs, _ := cs.HardState()
	if hs != wantHS {
		t.Fatalf("recovered HardState = %+v, want %+v", hs, wantHS)
	}
	if got := storedLog(t, cs); !crashLogsEqual(got, wantLog) {
		t.Fatalf("recovered log = %s, want %s", crashLogString(got), crashLogString(wantLog))
	}
	// Post-recovery Saves append cleanly after the trimmed tail.
	mustSaveOK(t, cs, nil, crashTestEntries(4, 3, "echo"))
	if got := cs.LastIndex(); got != 4 {
		t.Fatalf("LastIndex() after a post-recovery Save = %d, want 4", got)
	}
}

// TestCrashStorageSuffixTruncationDurableOnlyWhenSynced feeds the model a
// conflicting batch that logically truncates the log's suffix: lost before
// the sync barrier, recovery must show the original untruncated log; synced,
// recovery must honor the truncation.
func TestCrashStorageSuffixTruncationDurableOnlyWhenSynced(t *testing.T) {
	original := crashTestEntries(1, 1, "alpha", "bravo", "charlie")
	overwrite := crashTestEntries(2, 2, "prime")

	dirty := mustCrashStorage(t, 1, crashAt(2, CrashBeforeSync, 0))
	mustSaveOK(t, dirty, &raft.HardState{Term: 1}, original)
	if err := dirty.Save(&raft.HardState{Term: 2}, overwrite); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Save() error = %v, want ErrCrashed", err)
	}
	if err := dirty.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if got := storedLog(t, dirty); !crashLogsEqual(got, original) {
		t.Fatalf("unsynced truncation leaked into recovery: log = %s, want the original %s", crashLogString(got), crashLogString(original))
	}

	synced := mustCrashStorage(t, 1, crashAt(2, CrashAfterSyncBeforeSend, 0))
	mustSaveOK(t, synced, &raft.HardState{Term: 1}, original)
	if err := synced.Save(&raft.HardState{Term: 2}, overwrite); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Save() error = %v, want ErrCrashed", err)
	}
	if err := synced.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	want := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("alpha")},
		{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("prime")},
	}
	if got := storedLog(t, synced); !crashLogsEqual(got, want) {
		t.Fatalf("synced truncation not honored: log = %s, want %s", crashLogString(got), crashLogString(want))
	}
}

// crashScriptDigest drives one storage through a fixed script of saves,
// crashes, and recoveries — spanning a torn before-sync crash and an armed
// after-send crash — and records every observable after every step.
func crashScriptDigest(t *testing.T, cs *CrashStorage) []string {
	t.Helper()
	var digest []string
	note := func(step string, err error) {
		hs, hsErr := cs.HardState()
		digest = append(digest, fmt.Sprintf("%s err=%v crashed=%t armed=%t saves=%d hs=%+v hsErr=%v first=%d last=%d durable=%x",
			step, err, cs.Crashed(), cs.CrashArmed(), cs.SaveCount(), hs, hsErr, cs.FirstIndex(), cs.LastIndex(), cs.DurableBytes()))
	}
	note("save1", cs.Save(&raft.HardState{Term: 1, VotedFor: 1}, crashTestEntries(1, 1, "alpha")))
	note("save2", cs.Save(&raft.HardState{Term: 2, VotedFor: 2}, crashTestEntries(2, 2, "bravo", "charlie")))
	if info, ok := cs.LastCrash(); ok {
		digest = append(digest, fmt.Sprintf("crash %+v", info))
	}
	note("recover1", cs.Recover())
	note("save3", cs.Save(nil, crashTestEntries(2, 3, "delta")))
	note("save4", cs.Save(&raft.HardState{Term: 3, VotedFor: 1}, nil))
	digest = append(digest, fmt.Sprintf("fired=%t", cs.CrashIfArmed()))
	if info, ok := cs.LastCrash(); ok {
		digest = append(digest, fmt.Sprintf("crash %+v", info))
	}
	note("recover2", cs.Recover())
	note("save5", cs.Save(nil, crashTestEntries(3, 3, "echo")))
	return digest
}

// TestCrashStorageSameScheduleIsByteIdenticallyRepeatable is the
// same-schedule replay gate: two storages fed the identical schedule and
// Save script must match on every observable at every step, durable bytes
// included, across multiple crash/recover cycles.
func TestCrashStorageSameScheduleIsByteIdenticallyRepeatable(t *testing.T) {
	schedule := FaultSchedule{Crashes: []CrashDirective{
		{Node: 1, Save: 2, Point: CrashBeforeSync, RetainUnsynced: 7},
		{Node: 1, Save: 4, Point: CrashAfterSend},
	}}
	first := crashScriptDigest(t, mustCrashStorage(t, 1, schedule))
	second := crashScriptDigest(t, mustCrashStorage(t, 1, schedule))
	if len(first) == 0 {
		t.Fatal("script digest is empty, want a non-trivial run")
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same-schedule runs diverged:\nfirst:  %v\nsecond: %v", first, second)
	}
}

// TestCrashStorageScheduleDivergenceIsDeterministic changes exactly one
// retention byte across the hard-state record boundary: the two schedules
// must land in two different, exactly predicted survivor states, and each
// must replay to itself.
func TestCrashStorageScheduleDivergenceIsDeterministic(t *testing.T) {
	hsRecordLen := recordBytesFor(t, &raft.HardState{Term: 2, VotedFor: 3}, nil)
	run := func(retain int) (raft.HardState, uint64, string) {
		cs := mustCrashStorage(t, 1, crashAt(2, CrashBeforeSync, retain))
		mustSaveOK(t, cs, &raft.HardState{Term: 1, VotedFor: 1}, crashTestEntries(1, 1, "alpha"))
		if err := cs.Save(&raft.HardState{Term: 2, VotedFor: 3}, crashTestEntries(2, 2, "bravo")); !errors.Is(err, ErrCrashed) {
			t.Fatalf("Save() error = %v, want ErrCrashed", err)
		}
		if err := cs.Recover(); err != nil {
			t.Fatalf("Recover() error = %v", err)
		}
		hs, _ := cs.HardState()
		return hs, cs.LastIndex(), fmt.Sprintf("%x", cs.DurableBytes())
	}

	hsShort, lastShort, bytesShort := run(hsRecordLen - 1)
	hsFull, lastFull, bytesFull := run(hsRecordLen)
	if hsShort != (raft.HardState{Term: 1, VotedFor: 1}) || lastShort != 1 {
		t.Fatalf("cut below the record boundary recovered (%+v, %d), want the pre-crash state ({Term:1 VotedFor:1}, 1)", hsShort, lastShort)
	}
	if hsFull != (raft.HardState{Term: 2, VotedFor: 3}) || lastFull != 1 {
		t.Fatalf("cut at the record boundary recovered (%+v, %d), want ({Term:2 VotedFor:3}, 1)", hsFull, lastFull)
	}
	if bytesShort == bytesFull {
		t.Fatal("one-byte retention difference produced identical survivors, want deterministic divergence")
	}
	if hs2, last2, bytes2 := run(hsRecordLen - 1); hs2 != hsShort || last2 != lastShort || bytes2 != bytesShort {
		t.Fatal("replaying the same schedule diverged from its first run")
	}
}

// TestCrashStorageHostCrashRecoverAndMisuse covers the host-driven crash
// used for between-batch kills plus the misuse surface: recovering a running
// storage, crashing a crashed one, and saving while down.
func TestCrashStorageHostCrashRecoverAndMisuse(t *testing.T) {
	cs := mustCrashStorage(t, 1, FaultSchedule{})
	if err := cs.Recover(); err == nil {
		t.Fatal("Recover() on a running storage = nil, want an error")
	}
	mustSaveOK(t, cs, &raft.HardState{Term: 1, VotedFor: 1}, crashTestEntries(1, 1, "alpha"))
	image := cs.DurableBytes()
	if err := cs.Crash(); err != nil {
		t.Fatalf("Crash() error = %v, want nil", err)
	}
	info, ok := cs.LastCrash()
	if !ok || info.Point != CrashHostInitiated || info.Save != 1 || info.UnsyncedBytes != 0 || info.RetainedBytes != 0 {
		t.Fatalf("LastCrash() = %+v, %t, want host-initiated after save 1 with an empty dirty buffer", info, ok)
	}
	if err := cs.Crash(); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Crash() on a crashed storage error = %v, want ErrCrashed", err)
	}
	if err := cs.Save(nil, nil); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Save() on a crashed storage error = %v, want ErrCrashed", err)
	}
	if got := cs.SaveCount(); got != 1 {
		t.Fatalf("SaveCount() = %d, want 1 (rejected Saves consume no ordinal)", got)
	}
	if !bytes.Equal(cs.DurableBytes(), image) {
		t.Fatal("host-initiated crash changed durable bytes, want a byte-identical platter")
	}
	if err := cs.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	mustSaveOK(t, cs, nil, crashTestEntries(2, 1, "bravo"))
	if got := cs.LastIndex(); got != 2 {
		t.Fatalf("LastIndex() = %d, want 2 after a post-recovery Save", got)
	}
}

// TestCrashStorageArmedAfterSendCrashFiresAtNextSaveAsBackstop documents the
// host contract for CrashAfterSend: the crash is meant to fire via
// CrashIfArmed right after the send step, and a host that instead reaches
// the next Save gets the crash then — deterministically, with the armed
// ordinal, before the new batch stages anything.
func TestCrashStorageArmedAfterSendCrashFiresAtNextSaveAsBackstop(t *testing.T) {
	cs := mustCrashStorage(t, 1, crashAt(1, CrashAfterSend, 0))
	mustSaveOK(t, cs, &raft.HardState{Term: 1, VotedFor: 1}, crashTestEntries(1, 1, "alpha"))
	if !cs.CrashArmed() {
		t.Fatal("CrashArmed() = false after the after-send Save, want true")
	}
	err := cs.Save(nil, crashTestEntries(2, 1, "bravo"))
	if !errors.Is(err, ErrCrashed) {
		t.Fatalf("Save() while armed error = %v, want ErrCrashed", err)
	}
	info, ok := cs.LastCrash()
	if !ok || info.Save != 1 || info.Point != CrashAfterSend {
		t.Fatalf("LastCrash() = %+v, %t, want the armed save-1 after-send crash", info, ok)
	}
	if got := cs.SaveCount(); got != 1 {
		t.Fatalf("SaveCount() = %d, want 1 (the backstopped Save never began)", got)
	}
	if err := cs.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if got := cs.LastIndex(); got != 1 {
		t.Fatalf("recovered LastIndex() = %d, want 1 (the armed batch is durable, the rejected one is not)", got)
	}
}

// TestCrashStorageEmptySavesConsumeScheduleOrdinals pins the ordinal
// contract: every accepted Save counts, including batches that persist
// nothing, so schedules can name any Ready in a run.
func TestCrashStorageEmptySavesConsumeScheduleOrdinals(t *testing.T) {
	cs := mustCrashStorage(t, 1, crashAt(3, CrashBeforeSync, RetainAllUnsynced))
	mustSaveOK(t, cs, &raft.HardState{Term: 1, VotedFor: 1}, crashTestEntries(1, 1, "alpha"))
	image := cs.DurableBytes()
	mustSaveOK(t, cs, nil, nil)
	if !bytes.Equal(cs.DurableBytes(), image) {
		t.Fatal("an empty Save changed durable bytes, want no records staged")
	}
	if err := cs.Save(nil, nil); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Save() error = %v, want ErrCrashed at ordinal 3", err)
	}
	info, _ := cs.LastCrash()
	if info.Save != 3 || info.UnsyncedBytes != 0 || info.RetainedBytes != 0 {
		t.Fatalf("LastCrash() = %+v, want save 3 with an empty dirty buffer", info)
	}
	if err := cs.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	hs, _ := cs.HardState()
	if hs != (raft.HardState{Term: 1, VotedFor: 1}) || cs.LastIndex() != 1 {
		t.Fatalf("recovered state = (%+v, %d), want the save-1 state", hs, cs.LastIndex())
	}
}

// TestFaultScheduleValidationAndNodeFiltering pins the schedule structure
// 3C and the Phase 4/5 harnesses will reuse: For filters by node in schedule
// order without mutation, and NewCrashStorage rejects malformed directives.
func TestFaultScheduleValidationAndNodeFiltering(t *testing.T) {
	schedule := FaultSchedule{Crashes: []CrashDirective{
		{Node: 2, Save: 5, Point: CrashAfterSend},
		{Node: 1, Save: 4, Point: CrashBeforeSync, RetainUnsynced: 2},
		{Node: 1, Save: 1, Point: CrashAfterSyncBeforeSend},
	}}
	want := []CrashDirective{
		{Node: 1, Save: 4, Point: CrashBeforeSync, RetainUnsynced: 2},
		{Node: 1, Save: 1, Point: CrashAfterSyncBeforeSend},
	}
	if got := schedule.For(1); !reflect.DeepEqual(got, want) {
		t.Fatalf("For(1) = %+v, want %+v in schedule order", got, want)
	}
	if _, err := NewCrashStorage(1, schedule); err != nil {
		t.Fatalf("NewCrashStorage() error = %v, want nil for a valid schedule", err)
	}
	if !reflect.DeepEqual(schedule.For(1), want) {
		t.Fatal("NewCrashStorage() mutated the caller's schedule")
	}

	// Directives for other nodes never fire on this node's storage.
	other := mustCrashStorage(t, 3, schedule)
	for i := 0; i < 6; i++ {
		mustSaveOK(t, other, nil, nil)
	}

	invalid := []struct {
		name      string
		directive CrashDirective
	}{
		{"save ordinal 0", CrashDirective{Node: 1, Point: CrashBeforeSync}},
		{"host-initiated point", CrashDirective{Node: 1, Save: 1, Point: CrashHostInitiated}},
		{"unknown point", CrashDirective{Node: 1, Save: 1, Point: CrashPoint(9)}},
		{"retention below the sentinel", CrashDirective{Node: 1, Save: 1, Point: CrashBeforeSync, RetainUnsynced: -2}},
	}
	for _, tc := range invalid {
		if _, err := NewCrashStorage(1, FaultSchedule{Crashes: []CrashDirective{tc.directive}}); err == nil {
			t.Fatalf("NewCrashStorage(%s) error = nil, want a validation failure", tc.name)
		}
	}
	duplicate := FaultSchedule{Crashes: []CrashDirective{
		{Node: 1, Save: 2, Point: CrashBeforeSync},
		{Node: 1, Save: 2, Point: CrashAfterSend},
	}}
	if _, err := NewCrashStorage(1, duplicate); err == nil {
		t.Fatal("NewCrashStorage(duplicate ordinals) error = nil, want a validation failure")
	}
	// The same ordinal on different nodes is two separate storages' plans.
	if _, err := NewCrashStorage(2, FaultSchedule{Crashes: []CrashDirective{
		{Node: 1, Save: 2, Point: CrashBeforeSync},
		{Node: 2, Save: 2, Point: CrashBeforeSync},
	}}); err != nil {
		t.Fatalf("NewCrashStorage() error = %v, want nil (ordinal uniqueness is per node)", err)
	}
}

// TestCrashStorageReadsWhileCrashedServeSurvivingPlatterImage pins the
// omniscient-observer read contract the sim's invariants depend on: between
// crash and recovery the read side reports exactly the surviving records,
// and recovery then derives the same state from the same bytes.
func TestCrashStorageReadsWhileCrashedServeSurvivingPlatterImage(t *testing.T) {
	cs := mustCrashStorage(t, 1, crashAt(2, CrashBeforeSync, 0))
	mustSaveOK(t, cs, &raft.HardState{Term: 1, VotedFor: 2}, crashTestEntries(1, 1, "alpha", "bravo"))
	wantLog := storedLog(t, cs)
	if err := cs.Save(&raft.HardState{Term: 2, VotedFor: 3}, crashTestEntries(3, 2, "charlie")); !errors.Is(err, ErrCrashed) {
		t.Fatalf("Save() error = %v, want ErrCrashed", err)
	}

	hs, err := cs.HardState()
	if err != nil || hs != (raft.HardState{Term: 1, VotedFor: 2}) {
		t.Fatalf("HardState() while crashed = %+v, %v, want ({Term:1 VotedFor:2}, nil)", hs, err)
	}
	if got := storedLog(t, cs); !crashLogsEqual(got, wantLog) {
		t.Fatalf("log while crashed = %s, want the surviving %s", crashLogString(got), crashLogString(wantLog))
	}
	if err := cs.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if got := storedLog(t, cs); !crashLogsEqual(got, wantLog) {
		t.Fatalf("log after recovery = %s, want %s", crashLogString(got), crashLogString(wantLog))
	}
}
