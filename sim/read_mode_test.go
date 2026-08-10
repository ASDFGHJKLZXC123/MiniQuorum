package sim

import (
	"strings"
	"testing"

	"miniquorum/internal/raft"
)

func TestParseReadModeAcceptsOnlyLogAndReadIndex(t *testing.T) {
	for _, test := range []struct {
		value   string
		want    ReadMode
		wantErr bool
	}{
		{value: "log", want: ReadModeLog},
		{value: "readindex", want: ReadModeReadIndex},
		{value: "", wantErr: true},
		{value: "lease", wantErr: true},
		{value: "ReadIndex", wantErr: true},
	} {
		t.Run(test.value, func(t *testing.T) {
			got, err := ParseReadMode(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("ParseReadMode(%q) error = %v, wantErr=%t", test.value, err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("ParseReadMode(%q) = %q, want %q", test.value, got, test.want)
			}
		})
	}
}

func TestSimReadModeSelectionIsExplicitAndZeroValueCompatible(t *testing.T) {
	base := Config{Seed: 17, NodeIDs: []raft.NodeID{1}}
	defaultSim, err := NewSim(base)
	if err != nil {
		t.Fatalf("NewSim(default reads) error = %v", err)
	}
	if defaultSim.reads != ReadModeLog {
		t.Fatalf("direct Sim zero-value reads = %q, want compatibility mode log", defaultSim.reads)
	}

	for _, mode := range []ReadMode{ReadModeLog, ReadModeReadIndex} {
		config := base
		config.Reads = mode
		s, err := NewSim(config)
		if err != nil {
			t.Fatalf("NewSim(reads=%s) error = %v", mode, err)
		}
		if s.reads != mode {
			t.Fatalf("NewSim(reads=%s) selected %q", mode, s.reads)
		}
	}

	invalid := base
	invalid.Reads = ReadMode("lease")
	if _, err := NewSim(invalid); err == nil || !strings.Contains(err.Error(), "unsupported read mode") {
		t.Fatalf("NewSim(invalid reads) error = %v, want explicit validation", err)
	}
}
