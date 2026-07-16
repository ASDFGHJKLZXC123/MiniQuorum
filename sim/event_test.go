package sim

import (
	"container/heap"
	"testing"
)

// TestEventQueueStableOrderOnTies verifies same-time events pop in
// insertion (seq) order, never in map-iteration order — there is no map
// involved in ordering at all.
func TestEventQueueStableOrderOnTies(t *testing.T) {
	q := &eventQueue{}
	heap.Init(q)
	for i := 0; i < 4; i++ {
		heap.Push(q, &event{time: 100, seq: uint64(i)})
	}
	var got []uint64
	for q.Len() > 0 {
		e := heap.Pop(q).(*event)
		got = append(got, e.seq)
	}
	want := []uint64{0, 1, 2, 3}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("pop order = %v, want %v", got, want)
		}
	}
}

// TestEventQueueOrdersByTimeFirst verifies earlier virtual time always pops
// before a later one regardless of seq.
func TestEventQueueOrdersByTimeFirst(t *testing.T) {
	q := &eventQueue{}
	heap.Init(q)
	heap.Push(q, &event{time: 50, seq: 9})
	heap.Push(q, &event{time: 10, seq: 1})
	heap.Push(q, &event{time: 30, seq: 5})

	first := heap.Pop(q).(*event)
	if first.time != 10 {
		t.Fatalf("first popped time = %d, want 10", first.time)
	}
	second := heap.Pop(q).(*event)
	if second.time != 30 {
		t.Fatalf("second popped time = %d, want 30", second.time)
	}
}
