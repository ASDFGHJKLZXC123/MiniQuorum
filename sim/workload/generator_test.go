package workload

import (
	"reflect"
	"strconv"
	"testing"

	raftpb "miniquorum/proto"
)

func TestGeneratorIsDeterministicAndUniformOverOperationsAndEightKeys(t *testing.T) {
	first, err := NewGenerator(20260716, 3, DefaultKeyCount)
	if err != nil {
		t.Fatalf("NewGenerator(first): %v", err)
	}
	second, err := NewGenerator(20260716, 3, DefaultKeyCount)
	if err != nil {
		t.Fatalf("NewGenerator(second): %v", err)
	}
	for seq := uint64(1); seq <= 100; seq++ {
		left, right := first.Next(seq), second.Next(seq)
		if !reflect.DeepEqual(left, right) {
			t.Fatalf("same seed diverged at seq %d: %#v != %#v", seq, left, right)
		}
	}

	generator, err := NewGenerator(77, 3, DefaultKeyCount)
	if err != nil {
		t.Fatalf("NewGenerator(distribution): %v", err)
	}
	const samples = 30000
	opCounts := make(map[raftpb.Op]int)
	keyCounts := make(map[string]int)
	for seq := uint64(1); seq <= samples; seq++ {
		input := generator.Next(seq)
		opCounts[input.Op]++
		keyCounts[string(input.Key)]++
		switch input.Op {
		case raftpb.Op_PUT:
			if len(input.Value) == 0 || len(input.Value) > 12 {
				t.Fatalf("PUT value length = %d, want deterministic short value", len(input.Value))
			}
		case raftpb.Op_DELETE, raftpb.Op_GET:
			if len(input.Value) != 0 {
				t.Fatalf("%s generated value %q, want empty", input.Op, input.Value)
			}
		default:
			t.Fatalf("generated unsupported op %s", input.Op)
		}
	}
	assertNearUniform(t, "PUT", opCounts[raftpb.Op_PUT], samples/3, samples/20)
	assertNearUniform(t, "DELETE", opCounts[raftpb.Op_DELETE], samples/3, samples/20)
	assertNearUniform(t, "GET", opCounts[raftpb.Op_GET], samples/3, samples/20)
	for key := 0; key < DefaultKeyCount; key++ {
		assertNearUniform(t, "k"+strconv.Itoa(key), keyCounts["k"+strconv.Itoa(key)], samples/DefaultKeyCount, samples/40)
	}
}

func assertNearUniform(t *testing.T, label string, got, want, tolerance int) {
	t.Helper()
	if got < want-tolerance || got > want+tolerance {
		t.Fatalf("%s count = %d, want %d +/- %d", label, got, want, tolerance)
	}
}
