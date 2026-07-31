package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestCLIRejectsUnknownAndIncompleteArguments(t *testing.T) {
	cases := [][]string{
		nil,
		{"unknown"},
		{"collect", "-profile", "wrong", "-out", "result.json"},
		{"collect", "-profile", "smoke-v1"},
		{"validate"},
		{"validate", "-in", "missing.json", "trailing"},
		{"graph", "-in", "raw.json"},
		{"graph", "-in", "raw.json", "-out-dir", "graphs", "trailing"},
	}
	for _, arguments := range cases {
		t.Run(strings.Join(arguments, " "), func(t *testing.T) {
			command := exec.Command("go", append([]string{"run", "."}, arguments...)...)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("lsmbench %v succeeded unexpectedly: %s", arguments, output)
			}
			if len(output) == 0 {
				t.Fatalf("lsmbench %v failed without a diagnostic", arguments)
			}
		})
	}
}
