package sim

import (
	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

// VirtualTime is simulated time in whole virtual milliseconds. It never
// touches the wall clock; the sim is the sole source of time.
type VirtualTime int64

type eventKind int

const (
	eventTick eventKind = iota
	eventMessage
	eventCrash
	eventRestart
)

// event is one entry in the sim's event queue. seq is assigned at schedule
// time and is the tie-breaker for equal-time events, giving stable insertion
// order without ever consulting a Go map for ordering.
type event struct {
	time VirtualTime
	seq  uint64
	kind eventKind
	node raft.NodeID     // target node for tick/crash/restart
	msg  *raftpb.Message // payload for eventMessage

	// generation is the target node's simNode.generation at schedule time.
	// Only meaningful for eventTick; see simNode.generation and
	// Sim.tickIsStale.
	generation uint64
}

// eventQueue is a deterministic min-heap on (time, seq). It implements
// container/heap.Interface.
type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if q[i].time != q[j].time {
		return q[i].time < q[j].time
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(*event)) }

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return item
}
