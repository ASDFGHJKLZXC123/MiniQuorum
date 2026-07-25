package simharness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"miniquorum/sim"
)

const (
	// CorpusArtifactVersion is the manifest layout version this loader
	// understands. Bump it only with a deliberate manifest format change so
	// stale or future corpus evidence can never silently load.
	CorpusArtifactVersion = 1
	// CorpusManifestFile names the hand-maintained half of the corpus: the
	// scripted scenario entries and the pinned negative-control entry.
	CorpusManifestFile = "manifest.json"
	// CorpusGeneratedDir holds generated schedules as seed-<N>.json. The
	// filename carries the seed and the body carries the generator version,
	// so generated entries need no manifest row that could drift.
	CorpusGeneratedDir = "generated"
)

// CorpusEntry pairs one committed schedule file with the seed that drives the
// rest of the run's determinism (message delays, election jitter, workload
// draws). A schedule file alone does not identify a replay; (seed, schedule)
// does.
type CorpusEntry struct {
	Name     string `json:"name"`
	Seed     int64  `json:"seed"`
	Schedule string `json:"schedule"`
}

// CorpusManifest is corpus/manifest.json. Pinned is the committed schedule
// that must pass the ordinary build and must fail Porcupine under the buggy
// build tag (the section 5.4.2 negative control).
type CorpusManifest struct {
	ArtifactVersion  int           `json:"artifact_version"`
	GeneratorVersion int           `json:"generator_version"`
	Scripted         []CorpusEntry `json:"scripted"`
	Pinned           CorpusEntry   `json:"pinned"`
}

// Corpus is the loaded and structurally validated committed corpus.
type Corpus struct {
	Manifest  CorpusManifest
	Generated []CorpusEntry
}

var generatedSeedPattern = regexp.MustCompile(`^seed-(\d+)\.json$`)

// LoadCorpus reads the manifest, discovers every generated schedule, and
// validates versions: generated files must carry the manifest's generator
// version (which must be the current one, or a stale corpus would silently
// stop matching seed replays), scripted files must carry version 0, and every
// referenced file must decode. Generated entries are returned in ascending
// seed order.
func LoadCorpus(dir string) (Corpus, error) {
	manifestBytes, err := os.ReadFile(filepath.Join(dir, CorpusManifestFile))
	if err != nil {
		return Corpus{}, fmt.Errorf("corpus manifest: %w", err)
	}
	var manifest CorpusManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return Corpus{}, fmt.Errorf("corpus manifest: %w", err)
	}
	if manifest.ArtifactVersion != CorpusArtifactVersion {
		return Corpus{}, fmt.Errorf("corpus manifest artifact version %d, want %d", manifest.ArtifactVersion, CorpusArtifactVersion)
	}
	if manifest.GeneratorVersion != sim.FaultScheduleGeneratorVersion {
		return Corpus{}, fmt.Errorf("corpus manifest generator version %d, want current %d (regenerate with cmd/schedgen)",
			manifest.GeneratorVersion, sim.FaultScheduleGeneratorVersion)
	}
	for _, entry := range manifest.Scripted {
		schedule, err := ReadCorpusSchedule(dir, entry)
		if err != nil {
			return Corpus{}, err
		}
		if schedule.Version != 0 {
			return Corpus{}, fmt.Errorf("corpus scripted entry %s: version %d, want 0 (hand-written scripted schedule)", entry.Name, schedule.Version)
		}
	}
	if manifest.Pinned.Schedule != "" {
		pinned, err := ReadCorpusSchedule(dir, manifest.Pinned)
		if err != nil {
			return Corpus{}, err
		}
		// The pinned negative-control schedule is replayed verbatim, never
		// regenerated, so it may be either a hand-authored scripted schedule
		// (version 0, the shape the section 5.4.2 catch actually needed) or
		// output of the *current* generator. What must stay impossible is a
		// stale generator version: that would mean the committed evidence no
		// longer corresponds to anything this build can reproduce. Because
		// manifest.GeneratorVersion is already pinned to the current one just
		// above, this admits exactly {0, current} and rejects every other
		// version explicitly rather than leaning on the decoder to do it.
		if pinned.Version != 0 && pinned.Version != manifest.GeneratorVersion {
			return Corpus{}, fmt.Errorf("corpus pinned entry %s: schedule version %d, want 0 (scripted) or manifest generator version %d",
				manifest.Pinned.Name, pinned.Version, manifest.GeneratorVersion)
		}
		if manifest.Pinned.Seed == 0 {
			return Corpus{}, fmt.Errorf("corpus pinned entry %s: seed 0; a schedule file alone does not identify a replay", manifest.Pinned.Name)
		}
	}

	generatedDir := filepath.Join(dir, CorpusGeneratedDir)
	names, err := os.ReadDir(generatedDir)
	if err != nil {
		return Corpus{}, fmt.Errorf("corpus generated dir: %w", err)
	}
	var generated []CorpusEntry
	for _, name := range names {
		if name.IsDir() {
			continue
		}
		match := generatedSeedPattern.FindStringSubmatch(name.Name())
		if match == nil {
			return Corpus{}, fmt.Errorf("corpus generated file %s does not match seed-<N>.json", name.Name())
		}
		seed, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return Corpus{}, fmt.Errorf("corpus generated file %s: %w", name.Name(), err)
		}
		entry := CorpusEntry{
			Name:     "generated-" + name.Name(),
			Seed:     seed,
			Schedule: filepath.Join(CorpusGeneratedDir, name.Name()),
		}
		schedule, err := ReadCorpusSchedule(dir, entry)
		if err != nil {
			return Corpus{}, err
		}
		if schedule.Version != manifest.GeneratorVersion {
			return Corpus{}, fmt.Errorf("corpus generated file %s: version %d, want manifest generator version %d",
				name.Name(), schedule.Version, manifest.GeneratorVersion)
		}
		generated = append(generated, entry)
	}
	sort.Slice(generated, func(i, j int) bool { return generated[i].Seed < generated[j].Seed })
	return Corpus{Manifest: manifest, Generated: generated}, nil
}

// ReadCorpusSchedule reads and decodes one committed schedule file.
func ReadCorpusSchedule(dir string, entry CorpusEntry) (sim.FaultSchedule, error) {
	data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(entry.Schedule)))
	if err != nil {
		return sim.FaultSchedule{}, fmt.Errorf("corpus entry %s: %w", entry.Name, err)
	}
	schedule, err := sim.DecodeFaultSchedule(data)
	if err != nil {
		return sim.FaultSchedule{}, fmt.Errorf("corpus entry %s: %w", entry.Name, err)
	}
	return schedule, nil
}
