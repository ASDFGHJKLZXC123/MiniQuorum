package server

import (
	"errors"
	"reflect"
	"strconv"
	"testing"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

func TestProcessReadyOrdersSaveSendApplyAndAdvance(t *testing.T) {
	events := make([]string, 0, 6)
	node := &testNode{events: &events, ready: raft.Ready{
		Messages:         []*raftpb.Message{{To: 2}, {To: 3}},
		CommittedEntries: []raftpb.Entry{{Index: 4}, {Index: 5}},
	}}
	store := &testStorage{events: &events}
	transport := &testTransport{events: &events}
	applier := &testApplier{events: &events}

	if err := processReady(node, store, transport, applier); err != nil {
		t.Fatalf("processReady() error: %v", err)
	}
	if want := []string{"save", "send:2", "send:3", "apply:4", "apply:5", "advance"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestProcessReadySaveFailureHasNoOtherEffects(t *testing.T) {
	events := make([]string, 0, 1)
	wantErr := errors.New("save failed")
	node := &testNode{events: &events, ready: raft.Ready{
		Messages:         []*raftpb.Message{{To: 2}},
		CommittedEntries: []raftpb.Entry{{Index: 4}},
	}}
	store := &testStorage{events: &events, err: wantErr}

	err := processReady(node, store, &testTransport{events: &events}, &testApplier{events: &events})
	if !errors.Is(err, wantErr) {
		t.Fatalf("processReady() error = %v, want wrapped %v", err, wantErr)
	}
	if want := []string{"save"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestProcessReadyApplyFailureIsFailStopAndSkipsAdvance(t *testing.T) {
	events := make([]string, 0, 5)
	wantErr := errors.New("apply failed")
	node := &testNode{events: &events, ready: raft.Ready{
		Messages:         []*raftpb.Message{{To: 2}, {To: 3}},
		CommittedEntries: []raftpb.Entry{{Index: 4}, {Index: 5}, {Index: 6}},
	}}
	applier := &testApplier{events: &events, failIndex: 5, err: wantErr}

	err := processReady(node, &testStorage{events: &events}, &testTransport{events: &events}, applier)
	if !errors.Is(err, wantErr) {
		t.Fatalf("processReady() error = %v, want wrapped %v", err, wantErr)
	}
	if want := []string{"save", "send:2", "send:3", "apply:4", "apply:5"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

type testNode struct {
	events *[]string
	ready  raft.Ready
}

func (n *testNode) Ready() raft.Ready { return n.ready }

func (n *testNode) Advance() {
	*n.events = append(*n.events, "advance")
	n.ready = raft.Ready{}
}

type testStorage struct {
	events *[]string
	err    error
}

func (s *testStorage) Save(*raft.HardState, []raftpb.Entry) error {
	*s.events = append(*s.events, "save")
	return s.err
}

type testTransport struct {
	events *[]string
}

func (t *testTransport) Send(to raft.NodeID, _ *raftpb.Message) {
	*t.events = append(*t.events, "send:"+toString(to))
}

type testApplier struct {
	events    *[]string
	failIndex uint64
	err       error
}

func (a *testApplier) Apply(entry *raftpb.Entry) error {
	*a.events = append(*a.events, "apply:"+strconv.FormatUint(entry.Index, 10))
	if entry.Index == a.failIndex {
		return a.err
	}
	return nil
}

func toString(id raft.NodeID) string { return strconv.FormatUint(uint64(id), 10) }
