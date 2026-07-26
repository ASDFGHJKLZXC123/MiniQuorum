//go:build !race

package simharness

// Ordinary builds retain testing's normal parallel admission.
const raceCorpusReplayLimit = 0
