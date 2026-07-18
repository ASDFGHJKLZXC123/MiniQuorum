package checker

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"

	raftpb "miniquorum/proto"
)

type kvState map[string]string

// KVModel is the partitioned Porcupine model for MiniQuorum's KV map. Each
// partition contains the operations for exactly one key, while the pure state
// representation remains a map so the sequential specification is explicit.
var KVModel = porcupine.Model{
	Partition:      partitionOperations,
	PartitionEvent: partitionEvents,
	Init: func() interface{} {
		return kvState{}
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		current, stateOK := state.(kvState)
		invocation, inputOK := input.(Input)
		response, outputOK := output.(Output)
		if !stateOK || !inputOK || !outputOK || (!response.OK && !response.Unknown) {
			return false, state
		}

		key := string(invocation.Key)
		switch invocation.Op {
		case raftpb.Op_PUT:
			next := cloneState(current)
			next[key] = string(invocation.Value)
			return true, next
		case raftpb.Op_DELETE:
			next := cloneState(current)
			delete(next, key)
			return true, next
		case raftpb.Op_GET:
			if response.Unknown {
				return true, state
			}
			value, found := current[key]
			return response.Found == found && bytes.Equal(response.Value, []byte(value)), state
		default:
			return false, state
		}
	},
	Equal: func(state1, state2 interface{}) bool {
		left, leftOK := state1.(kvState)
		right, rightOK := state2.(kvState)
		if !leftOK || !rightOK || len(left) != len(right) {
			return false
		}
		for key, value := range left {
			rightValue, ok := right[key]
			if !ok || rightValue != value {
				return false
			}
		}
		return true
	},
	Hash: func(state interface{}) uint64 {
		current, ok := state.(kvState)
		if !ok {
			return 0
		}
		keys := make([]string, 0, len(current))
		for key := range current {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		h := fnv.New64a()
		for _, key := range keys {
			_, _ = h.Write([]byte(key))
			_, _ = h.Write([]byte{0})
			_, _ = h.Write([]byte(current[key]))
			_, _ = h.Write([]byte{0})
		}
		return h.Sum64()
	},
	DescribeOperation: func(input, output interface{}) string {
		invocation, inputOK := input.(Input)
		response, outputOK := output.(Output)
		if !inputOK || !outputOK {
			return fmt.Sprintf("%v -> %v", input, output)
		}
		switch invocation.Op {
		case raftpb.Op_PUT:
			if response.Unknown {
				return fmt.Sprintf("PUT(%q, %q) -> unknown", invocation.Key, invocation.Value)
			}
			return fmt.Sprintf("PUT(%q, %q) -> ok=%t", invocation.Key, invocation.Value, response.OK)
		case raftpb.Op_DELETE:
			if response.Unknown {
				return fmt.Sprintf("DELETE(%q) -> unknown", invocation.Key)
			}
			return fmt.Sprintf("DELETE(%q) -> ok=%t", invocation.Key, response.OK)
		case raftpb.Op_GET:
			if response.Unknown {
				return fmt.Sprintf("GET(%q) -> unknown", invocation.Key)
			}
			return fmt.Sprintf("GET(%q) -> value=%q found=%t ok=%t", invocation.Key, response.Value, response.Found, response.OK)
		default:
			return fmt.Sprintf("OP(%d, %q) -> %v", invocation.Op, invocation.Key, response)
		}
	},
	DescribeState: func(state interface{}) string {
		current, ok := state.(kvState)
		if !ok {
			return fmt.Sprint(state)
		}
		keys := make([]string, 0, len(current))
		for key := range current {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, fmt.Sprintf("%q:%q", key, current[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	},
}

// Check reports whether history is linearizable. Logical open records remain
// open; PorcupineEvents supplies v1.3's paired synthetic-unknown adapter event,
// allowing the operation to linearize after invocation without claiming that
// the client observed a completion.
func Check(history History) bool {
	return porcupine.CheckEvents(KVModel, history.PorcupineEvents())
}

func partitionOperations(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := make(map[string][]porcupine.Operation)
	for _, operation := range history {
		input, ok := operation.Input.(Input)
		if !ok {
			byKey[""] = append(byKey[""], operation)
			continue
		}
		key := string(input.Key)
		byKey[key] = append(byKey[key], operation)
	}
	return sortedOperationPartitions(byKey)
}

func partitionEvents(history []porcupine.Event) [][]porcupine.Event {
	byKey := make(map[string][]porcupine.Event)
	keysByID := make(map[int]string)
	for _, event := range history {
		if event.Kind == porcupine.CallEvent {
			input, ok := event.Value.(Input)
			key := ""
			if ok {
				key = string(input.Key)
			}
			keysByID[event.Id] = key
			byKey[key] = append(byKey[key], event)
			continue
		}
		key := keysByID[event.Id]
		byKey[key] = append(byKey[key], event)
	}

	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	partitions := make([][]porcupine.Event, 0, len(keys))
	for _, key := range keys {
		partitions = append(partitions, byKey[key])
	}
	return partitions
}

func sortedOperationPartitions(byKey map[string][]porcupine.Operation) [][]porcupine.Operation {
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	partitions := make([][]porcupine.Operation, 0, len(keys))
	for _, key := range keys {
		partitions = append(partitions, byKey[key])
	}
	return partitions
}

func cloneState(state kvState) kvState {
	cloned := make(kvState, len(state)+1)
	for key, value := range state {
		cloned[key] = value
	}
	return cloned
}
