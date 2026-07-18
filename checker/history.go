package checker

import (
	"errors"
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"

	raftpb "miniquorum/proto"
)

// Input is the invocation of one logical KV operation.
type Input struct {
	Op    raftpb.Op
	Key   []byte
	Value []byte
}

// Output is the successful response to one logical KV operation.
type Output struct {
	OK    bool
	Value []byte
	Found bool

	// Unknown is used only by the Porcupine adapter for a recorded open
	// operation. Recorder.Complete rejects it: an outcome-unknown operation
	// must never become a completed logical history record.
	Unknown bool
}

// Operation is one logical client operation. A nil ReturnTime means the
// recorded operation is unfinished: its outcome is unknown. Porcupine v1.3's
// paired-event adapter represents that open record with a synthetic unknown
// return; it is not an observed client completion.
type Operation struct {
	ClientID   uint64
	Seq        uint64
	InvokeTime int64
	ReturnTime *int64
	Input      Input
	Output     Output
}

// History is an invocation-ordered logical operation history.
type History []Operation

// Metadata identifies an operation in Porcupine visualizations without
// affecting linearizability checking.
type Metadata struct {
	ClientID uint64
	Seq      uint64
}

var (
	// ErrOutstandingOperation means a client tried to start a second logical
	// operation before resolving its current one.
	ErrOutstandingOperation = errors.New("checker: client already has an outstanding operation")
	// ErrNoOutstandingOperation means a completion, timeout, or retry did not
	// match the client's current logical operation.
	ErrNoOutstandingOperation = errors.New("checker: no matching outstanding operation")
	// ErrSequenceNotIncreasing means a new logical operation did not advance
	// the stable session's sequence number.
	ErrSequenceNotIncreasing = errors.New("checker: sequence is not monotonically increasing")
)

// Recorder builds a history while enforcing one outstanding logical
// operation per stable client session. Retries are deliberately not appended:
// they validate the same sequence and leave the first invocation unchanged.
type Recorder struct {
	history History
	active  map[uint64]int
	lastSeq map[uint64]uint64
}

// NewRecorder constructs an empty history recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		active:  make(map[uint64]int),
		lastSeq: make(map[uint64]uint64),
	}
}

// Invoke starts a new logical operation for clientID.
func (r *Recorder) Invoke(clientID, seq uint64, at int64, input Input) error {
	if _, ok := r.active[clientID]; ok {
		return ErrOutstandingOperation
	}
	if last, ok := r.lastSeq[clientID]; ok && seq <= last {
		return fmt.Errorf("%w: client %d seq %d after %d", ErrSequenceNotIncreasing, clientID, seq, last)
	}
	r.history = append(r.history, Operation{
		ClientID:   clientID,
		Seq:        seq,
		InvokeTime: at,
		Input:      cloneInput(input),
	})
	r.active[clientID] = len(r.history) - 1
	r.lastSeq[clientID] = seq
	return nil
}

// Retry validates that an attempt belongs to the client's current logical
// operation. It does not add a history record or change the first invoke time.
func (r *Recorder) Retry(clientID, seq uint64) error {
	_, err := r.activeIndex(clientID, seq)
	return err
}

// Complete records the successful response to the client's current logical
// operation.
func (r *Recorder) Complete(clientID, seq uint64, at int64, output Output) error {
	index, err := r.activeIndex(clientID, seq)
	if err != nil {
		return err
	}
	if at < r.history[index].InvokeTime {
		return fmt.Errorf("checker: return time %d precedes invoke time %d", at, r.history[index].InvokeTime)
	}
	if output.Unknown {
		return errors.New("checker: an unknown outcome cannot complete an operation")
	}
	returnTime := at
	r.history[index].ReturnTime = &returnTime
	r.history[index].Output = cloneOutput(output)
	delete(r.active, clientID)
	return nil
}

// Timeout marks that the client's wait reached a virtual deadline without
// recording a response. The logical operation remains open in the history and
// remains the session's outstanding operation because it may already have
// applied even though the client did not learn its outcome. The session may
// retry the same sequence, but it may not invoke a new one.
func (r *Recorder) Timeout(clientID, seq uint64, at int64) error {
	index, err := r.activeIndex(clientID, seq)
	if err != nil {
		return err
	}
	if at < r.history[index].InvokeTime {
		return fmt.Errorf("checker: timeout time %d precedes invoke time %d", at, r.history[index].InvokeTime)
	}
	return nil
}

// HasOutstanding reports whether clientID currently has a logical operation
// awaiting completion, retry, or timeout.
func (r *Recorder) HasOutstanding(clientID uint64) bool {
	_, ok := r.active[clientID]
	return ok
}

// History returns a deep copy of every logical operation in invocation order.
func (r *Recorder) History() History {
	cloned := make(History, len(r.history))
	for i := range r.history {
		cloned[i] = cloneOperation(r.history[i])
	}
	return cloned
}

func (r *Recorder) activeIndex(clientID, seq uint64) (int, error) {
	index, ok := r.active[clientID]
	if !ok || r.history[index].Seq != seq {
		return 0, fmt.Errorf("%w: client %d seq %d", ErrNoOutstandingOperation, clientID, seq)
	}
	return index, nil
}

// PorcupineEvents converts a logical history into ordered call/return events.
// Calls sort before returns at the same virtual time, matching Porcupine's
// closed-interval interpretation.
//
// Porcupine v1.3 represents an unfinished invocation with an unknown synthetic
// return at the end of the observed history. The logical record remains open;
// the synthetic output permits every total KV operation to linearize anywhere
// after invocation, including after all completed operations, without claiming
// that the client observed a response.
func (h History) PorcupineEvents() []porcupine.Event {
	type timedEvent struct {
		time  int64
		call  bool
		order int
		event porcupine.Event
	}

	clientIDs := make([]uint64, 0, len(h))
	seenClients := make(map[uint64]struct{})
	for _, operation := range h {
		if _, ok := seenClients[operation.ClientID]; !ok {
			seenClients[operation.ClientID] = struct{}{}
			clientIDs = append(clientIDs, operation.ClientID)
		}
	}
	sort.Slice(clientIDs, func(i, j int) bool { return clientIDs[i] < clientIDs[j] })
	clientIndex := make(map[uint64]int, len(clientIDs))
	for i, clientID := range clientIDs {
		clientIndex[clientID] = i
	}

	timed := make([]timedEvent, 0, 2*len(h))
	unfinished := make([]porcupine.Event, 0)
	for id, operation := range h {
		metadata := Metadata{ClientID: operation.ClientID, Seq: operation.Seq}
		timed = append(timed, timedEvent{
			time:  operation.InvokeTime,
			call:  true,
			order: id,
			event: porcupine.Event{
				ClientId: clientIndex[operation.ClientID],
				Kind:     porcupine.CallEvent,
				Value:    cloneInput(operation.Input),
				Id:       id,
				Metadata: metadata,
			},
		})
		if operation.ReturnTime != nil {
			timed = append(timed, timedEvent{
				time:  *operation.ReturnTime,
				order: id,
				event: porcupine.Event{
					ClientId: clientIndex[operation.ClientID],
					Kind:     porcupine.ReturnEvent,
					Value:    cloneOutput(operation.Output),
					Id:       id,
					Metadata: metadata,
				},
			})
		} else {
			unfinished = append(unfinished, porcupine.Event{
				ClientId: clientIndex[operation.ClientID],
				Kind:     porcupine.ReturnEvent,
				Value:    Output{Unknown: true},
				Id:       id,
				Metadata: metadata,
			})
		}
	}
	sort.SliceStable(timed, func(i, j int) bool {
		if timed[i].time != timed[j].time {
			return timed[i].time < timed[j].time
		}
		if timed[i].call != timed[j].call {
			return timed[i].call
		}
		return timed[i].order < timed[j].order
	})

	events := make([]porcupine.Event, 0, len(timed)+len(unfinished))
	for i := range timed {
		events = append(events, timed[i].event)
	}
	events = append(events, unfinished...)
	return events
}

func cloneOperation(operation Operation) Operation {
	cloned := operation
	cloned.Input = cloneInput(operation.Input)
	cloned.Output = cloneOutput(operation.Output)
	if operation.ReturnTime != nil {
		returnTime := *operation.ReturnTime
		cloned.ReturnTime = &returnTime
	}
	return cloned
}

func cloneInput(input Input) Input {
	input.Key = append([]byte(nil), input.Key...)
	input.Value = append([]byte(nil), input.Value...)
	return input
}

func cloneOutput(output Output) Output {
	output.Value = append([]byte(nil), output.Value...)
	return output
}
