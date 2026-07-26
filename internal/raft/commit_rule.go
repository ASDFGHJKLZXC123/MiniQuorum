//go:build !buggy

package raft

// commitTermEligible enforces Raft section 5.4.2: a leader may advance its
// commit index by replica count only for an entry from its current term.
func commitTermEligible(entryTerm, currentTerm uint64) bool {
	return entryTerm == currentTerm
}
