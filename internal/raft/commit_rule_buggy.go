//go:build buggy

package raft

// commitTermEligible is the deliberately unsafe Phase 4 negative control.
// It recreates only the section 5.4.2 commit-by-count bug. This file is
// unreachable in ordinary builds because commit_rule.go is selected instead.
func commitTermEligible(_, _ uint64) bool {
	return true
}
