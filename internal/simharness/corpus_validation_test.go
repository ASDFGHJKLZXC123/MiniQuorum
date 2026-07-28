//go:build !buggy

package simharness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"miniquorum/sim"
)

// TestLoadCorpusRejectsStaleOrIncompatibleEvidence pins the corpus loader's
// version discipline: a manifest with an unknown artifact version, a stale
// manifest generator version, a pinned schedule carrying a stale *nonzero*
// generator version, a generated file whose version disagrees with the
// manifest, a malformed generated filename, and a scripted file that is not
// version 0 must all fail to load rather than silently supplying stale evidence
// to the gates. The pin itself is deliberately admitted at version 0 (a
// hand-authored scripted schedule — the shape the section 5.4.2 catch needed)
// or the current generator version; TestLoadCorpusAcceptsVersionZeroPin asserts
// that positive half so this rejection set can never be tightened into
// rejecting the genuine version-0 pin.
func TestLoadCorpusRejectsStaleOrIncompatibleEvidence(t *testing.T) {
	valid := func() corpusFixture { return newCorpusFixture(t) }

	cases := []struct {
		name    string
		mutate  func(*corpusFixture)
		wantErr string
	}{
		{
			name:    "manifest artifact version unknown",
			mutate:  func(f *corpusFixture) { f.manifest.ArtifactVersion = CorpusArtifactVersion + 1 },
			wantErr: "artifact version",
		},
		{
			name:    "manifest generator version stale",
			mutate:  func(f *corpusFixture) { f.manifest.GeneratorVersion = sim.FaultScheduleGeneratorVersion - 1 },
			wantErr: "generator version",
		},
		{
			name: "pinned schedule has a stale nonzero generator version",
			mutate: func(f *corpusFixture) {
				// A hand-authored pin is version 0 and the current generator's
				// output is the current version; any *other* nonzero version is
				// stale evidence this build can no longer reproduce and must not
				// load. (Version 0 acceptance is asserted by
				// TestLoadCorpusAcceptsVersionZeroPin, not rejected here.)
				f.writeSchedule("pinned/buggy.json", sim.FaultSchedule{Version: sim.FaultScheduleGeneratorVersion - 1})
			},
			wantErr: "unsupported",
		},
		{
			name: "generated file version mismatch",
			mutate: func(f *corpusFixture) {
				f.writeSchedule("generated/seed-0001.json", sim.FaultSchedule{Version: 0})
			},
			wantErr: "want manifest generator version",
		},
		{
			name: "scripted file is not version 0",
			mutate: func(f *corpusFixture) {
				f.writeSchedule("scripted/scenario.json", sim.FaultSchedule{Version: sim.FaultScheduleGeneratorVersion})
			},
			wantErr: "want 0",
		},
		{
			name: "generated filename without a seed",
			mutate: func(f *corpusFixture) {
				f.writeSchedule("generated/latest.json", sim.FaultSchedule{Version: sim.FaultScheduleGeneratorVersion})
			},
			wantErr: "does not match seed-<N>.json",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := valid()
			tc.mutate(&fixture)
			fixture.writeManifest()
			if _, err := LoadCorpus(fixture.dir); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("LoadCorpus error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}

	fixture := valid()
	fixture.writeManifest()
	corpus, err := LoadCorpus(fixture.dir)
	if err != nil {
		t.Fatalf("LoadCorpus on the valid fixture: %v", err)
	}
	if len(corpus.Generated) != 1 || corpus.Generated[0].Seed != 1 {
		t.Fatalf("valid fixture generated entries = %#v, want exactly seed 1", corpus.Generated)
	}
}

// TestLoadCorpusAcceptsVersionZeroPin is the positive half of the loader's
// pinned-version contract, matching corpus.go's intentional design: the genuine
// section 5.4.2 negative control is a hand-authored scripted schedule, which by
// the FaultSchedule contract carries version 0, so the loader must accept a
// version-0 pin. (The rejection set above still rejects a stale *nonzero* pin
// version — the two together admit exactly {0, current generator version}.)
func TestLoadCorpusAcceptsVersionZeroPin(t *testing.T) {
	fixture := newCorpusFixture(t)
	// Replace the fixture's current-generator pin with a hand-authored version-0
	// schedule, exactly the shape corpus/pinned/buggy-5.4.2.json uses.
	fixture.writeSchedule("pinned/buggy.json", sim.FaultSchedule{
		Version: 0,
		Events: []sim.FaultEvent{
			{Time: 100, Kind: sim.FaultPause, Node: 1},
			{Time: 200, Kind: sim.FaultResume, Node: 1},
		},
	})
	fixture.writeManifest()
	if _, err := LoadCorpus(fixture.dir); err != nil {
		t.Fatalf("LoadCorpus rejected a version-0 pin: %v", err)
	}
}

type corpusFixture struct {
	t        *testing.T
	dir      string
	manifest CorpusManifest
}

// newCorpusFixture builds a minimal valid corpus in a temp directory: one
// scripted entry, one pinned entry, and one generated schedule for seed 1.
func newCorpusFixture(t *testing.T) corpusFixture {
	t.Helper()
	fixture := corpusFixture{
		t:   t,
		dir: t.TempDir(),
		manifest: CorpusManifest{
			ArtifactVersion:  CorpusArtifactVersion,
			GeneratorVersion: sim.FaultScheduleGeneratorVersion,
			Scripted: []CorpusEntry{
				{Name: "scenario", Seed: 42, Schedule: "scripted/scenario.json"},
			},
			Pinned: CorpusEntry{Name: "pinned", Seed: 7, Schedule: "pinned/buggy.json"},
		},
	}
	fixture.writeSchedule("scripted/scenario.json", sim.FaultSchedule{
		Events: []sim.FaultEvent{{Time: 10, Kind: sim.FaultPause, Node: 1}, {Time: 20, Kind: sim.FaultResume, Node: 1}},
	})
	generated, err := sim.GenerateFaultSchedule(7, DefaultNodeIDs(), DefaultFaultHorizon)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule: %v", err)
	}
	fixture.writeSchedule("pinned/buggy.json", generated)
	seedOne, err := sim.GenerateFaultSchedule(1, DefaultNodeIDs(), DefaultFaultHorizon)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule: %v", err)
	}
	fixture.writeSchedule("generated/seed-0001.json", seedOne)
	return fixture
}

func (f *corpusFixture) writeSchedule(relative string, schedule sim.FaultSchedule) {
	f.t.Helper()
	encoded, err := sim.EncodeFaultSchedule(schedule)
	if err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(f.dir, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *corpusFixture) writeManifest() {
	f.t.Helper()
	encoded, err := json.MarshalIndent(f.manifest, "", "  ")
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, CorpusManifestFile), append(encoded, '\n'), 0o644); err != nil {
		f.t.Fatal(err)
	}
}
