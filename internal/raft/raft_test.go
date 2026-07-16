package raft

import "testing"

func TestNewNodeAppliesElectionTickMinDefault(t *testing.T) {
	node := NewNode(Config{}, InitialState{}, nil)
	if node.config.ElectionTickMin != 10 {
		t.Fatalf("ElectionTickMin = %d, want 10", node.config.ElectionTickMin)
	}
}

func TestNewNodeAppliesElectionTickMaxDefault(t *testing.T) {
	node := NewNode(Config{}, InitialState{}, nil)
	if node.config.ElectionTickMax != 20 {
		t.Fatalf("ElectionTickMax = %d, want 20", node.config.ElectionTickMax)
	}
}

func TestNewNodeAppliesHeartbeatTicksDefault(t *testing.T) {
	node := NewNode(Config{}, InitialState{}, nil)
	if node.config.HeartbeatTicks != 2 {
		t.Fatalf("HeartbeatTicks = %d, want 2", node.config.HeartbeatTicks)
	}
}
