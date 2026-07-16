package main

import (
	"reflect"
	"testing"

	"miniquorum/internal/raft"
)

func TestPeerIDsAreSorted(t *testing.T) {
	peers := map[raft.NodeID]string{3: "three", 1: "one", 2: "two"}
	if got, want := peerIDs(peers), []raft.NodeID{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("peerIDs() = %v, want %v", got, want)
	}
}
