//go:build race

package simharness

// Four concurrent full replays gave the best race-mode throughput on the
// Packet 4C correction host. The test still starts one subtest for every
// committed artifact; this limits only how many instrumented simulations run
// at once.
const raceCorpusReplayLimit = 4
