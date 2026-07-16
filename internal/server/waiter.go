package server

import (
	"sync"

	"miniquorum/internal/statemachine"
	raftpb "miniquorum/proto"
)

// waiterKey identifies one outstanding Execute call by the exact log position
// it proposed at. Keying on the pair, not the index alone, is what lets the
// apply loop tell "my entry committed" apart from "a different leader's entry
// committed at the index I was waiting on".
type waiterKey struct {
	index uint64
	term  uint64
}

// waiterOutcome is delivered to a registered waiter exactly once.
type waiterOutcome struct {
	result statemachine.Result
	// stale is true when a different entry committed at this waiter's index,
	// meaning this node's proposal was lost (usually a leadership change).
	// The caller must treat this as retryable and never surface result.
	stale bool
}

// KVApplier bridges the entry-aware StateMachine to the Host's single
// Ready-processing path (via Applier) and to the KV service's per-request
// waiters. It has no goroutines of its own; Apply always runs synchronously
// on whichever goroutine currently holds Host.mu.
type KVApplier struct {
	sm statemachine.StateMachine

	mu      sync.Mutex
	waiters map[waiterKey]chan waiterOutcome
	byIndex map[uint64]waiterKey
}

// NewKVApplier constructs a KVApplier over sm.
func NewKVApplier(sm statemachine.StateMachine) *KVApplier {
	return &KVApplier{
		sm:      sm,
		waiters: make(map[waiterKey]chan waiterOutcome),
		byIndex: make(map[uint64]waiterKey),
	}
}

var _ Applier = (*KVApplier)(nil)

// Apply satisfies Applier: it applies entry to the state machine, then
// fulfills whichever waiter (if any) is registered for entry's log position.
func (a *KVApplier) Apply(entry *raftpb.Entry) error {
	result, err := a.sm.Apply(entry)
	if err != nil {
		return err
	}
	a.fulfill(entry.GetIndex(), entry.GetTerm(), result)
	return nil
}

// register records interest in (index, term) and returns the channel that
// will receive its single outcome. Callers must invoke this before the Ready
// containing that index can be drained, or a fast (e.g. single-node) commit
// can apply and fulfill before anyone is listening.
func (a *KVApplier) register(index, term uint64) <-chan waiterOutcome {
	a.mu.Lock()
	defer a.mu.Unlock()
	ch := make(chan waiterOutcome, 1)
	key := waiterKey{index: index, term: term}
	a.waiters[key] = ch
	a.byIndex[index] = key
	return ch
}

// cancel removes a waiter that will never be read again (e.g. its RPC
// context was cancelled). It is safe to call after the waiter already
// resolved.
func (a *KVApplier) cancel(index, term uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := waiterKey{index: index, term: term}
	if _, ok := a.waiters[key]; ok {
		delete(a.waiters, key)
		if a.byIndex[index] == key {
			delete(a.byIndex, index)
		}
	}
}

func (a *KVApplier) fulfill(index, term uint64, result statemachine.Result) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := waiterKey{index: index, term: term}
	if ch, ok := a.waiters[key]; ok {
		delete(a.waiters, key)
		delete(a.byIndex, index)
		ch <- waiterOutcome{result: result}
		return
	}
	// No waiter for this exact (index, term): if some other waiter is still
	// registered at this index, a different entry has committed there, so
	// that waiter's own proposal was lost and it must never be handed this
	// entry's result.
	if staleKey, ok := a.byIndex[index]; ok {
		if ch, ok := a.waiters[staleKey]; ok {
			delete(a.waiters, staleKey)
			ch <- waiterOutcome{stale: true}
		}
		delete(a.byIndex, index)
	}
}
