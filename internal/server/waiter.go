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
	// err is non-nil when the host permanently fail-stopped before this
	// waiter's entry could commit and apply, so no outcome can ever arrive.
	// The caller must surface it as a retryable failure and never read result.
	err error
}

// KVApplier bridges the entry-aware StateMachine to the Host's single
// Ready-processing path (via Applier) and to the KV service's per-request
// waiters. It has no goroutines of its own; Apply always runs synchronously
// on whichever goroutine currently holds Host.mu.
type KVApplier struct {
	sm statemachine.StateMachine

	mu      sync.Mutex
	waiters map[waiterKey]chan waiterOutcome
	// byIndex tracks every term currently registered at an index, not just
	// the most recent one: two waiters can be outstanding at the same index
	// for different terms (e.g. this node proposed at (5,3), lost and
	// regained leadership, and later proposed again at (5,4) before the
	// first waiter's entry was ever applied or cancelled). fulfill must be
	// able to find and resolve all of them, not just whichever registered
	// last.
	byIndex map[uint64]map[uint64]struct{}
}

// NewKVApplier constructs a KVApplier over sm.
func NewKVApplier(sm statemachine.StateMachine) *KVApplier {
	return &KVApplier{
		sm:      sm,
		waiters: make(map[waiterKey]chan waiterOutcome),
		byIndex: make(map[uint64]map[uint64]struct{}),
	}
}

var (
	_ Applier          = (*KVApplier)(nil)
	_ FailStopNotifier = (*KVApplier)(nil)
)

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
	terms := a.byIndex[index]
	if terms == nil {
		terms = make(map[uint64]struct{})
		a.byIndex[index] = terms
	}
	terms[term] = struct{}{}
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
		a.removeFromIndexLocked(index, term)
	}
}

// removeFromIndexLocked drops term from index's registered-term set. Callers
// must hold a.mu.
func (a *KVApplier) removeFromIndexLocked(index, term uint64) {
	terms, ok := a.byIndex[index]
	if !ok {
		return
	}
	delete(terms, term)
	if len(terms) == 0 {
		delete(a.byIndex, index)
	}
}

// FailStop implements FailStopNotifier: it resolves every registered waiter
// with err and empties both maps. The Host calls it exactly once, holding
// Host.mu, when Ready processing fails and the host permanently fail-stops —
// from that point nothing can ever commit or apply on this node, so every
// waiter still registered (the failing proposal's own and any earlier ones
// still awaiting quorum) would otherwise stay blocked forever. The drain is
// exhaustive and final: waiters register only inside Host.Propose's
// onProposed callback under Host.mu, and every Host entry point
// short-circuits once stopped, so no waiter can appear after this runs.
// A concurrent cancel (context expiry) is safe: whichever side removes the
// waiter from the maps first delivers (or, for cancel, suppresses) its single
// outcome, and the other finds nothing. Calling FailStop again is a no-op.
func (a *KVApplier) FailStop(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for key, ch := range a.waiters {
		delete(a.waiters, key)
		a.removeFromIndexLocked(key.index, key.term)
		ch <- waiterOutcome{err: err}
	}
}

// fulfill resolves every waiter registered at index, not just one: the
// waiter whose term matches the committed entry receives its result, and
// every other term registered at that index is failed stale (its own
// proposal was lost, usually to a leadership change) so it can never remain
// stranded waiting on a result that will never come.
func (a *KVApplier) fulfill(index, term uint64, result statemachine.Result) {
	a.mu.Lock()
	defer a.mu.Unlock()

	terms := a.byIndex[index]
	for t := range terms {
		key := waiterKey{index: index, term: t}
		ch, ok := a.waiters[key]
		if !ok {
			continue
		}
		delete(a.waiters, key)
		if t == term {
			ch <- waiterOutcome{result: result}
		} else {
			ch <- waiterOutcome{stale: true}
		}
	}
	delete(a.byIndex, index)
}
