package bench

import (
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"miniquorum/internal/lsm"
)

func TestFrozenProfilesAndExpectedRows(t *testing.T) {
	report, ok := BuiltinProfile(ProfileReport)
	if !ok {
		t.Fatal("report profile missing")
	}
	if got, want := ExpectedRows(ProfileReport), 130; got != want {
		t.Fatalf("report rows = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(report.Trials, []int{1, 2, 3, 4, 5}) || !reflect.DeepEqual(report.Seeds, []int{1, 2, 3, 4, 5}) {
		t.Fatalf("report trial/seed mapping = %v/%v", report.Trials, report.Seeds)
	}
	if !reflect.DeepEqual(report.WriteThroughput.FlushThresholdBytes, []int{65536, 262144, 1048576, 4194304}) || report.WriteThroughput.Operations != 100000 {
		t.Fatalf("report write matrix changed: %+v", report.WriteThroughput)
	}
	if !reflect.DeepEqual(report.PointRead.SSTableCounts, []int{1, 4, 16, 64}) || report.PointRead.EntriesPerSSTable != 4096 || report.PointRead.Operations != 25000 {
		t.Fatalf("report point-read matrix changed: %+v", report.PointRead)
	}

	smoke, ok := BuiltinProfile(ProfileSmoke)
	if !ok {
		t.Fatal("smoke profile missing")
	}
	if got, want := ExpectedRows(ProfileSmoke), 14; got != want {
		t.Fatalf("smoke rows = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(smoke.Trials, []int{1}) || !reflect.DeepEqual(smoke.Seeds, []int{1}) || !reflect.DeepEqual(smoke.WriteThroughput.FlushThresholdBytes, []int{65536, 4194304}) || smoke.WriteThroughput.Operations != 2000 {
		t.Fatalf("smoke matrix changed: %+v", smoke)
	}
}

func TestFrozenMatricesAreFullyPinned(t *testing.T) {
	for _, expected := range []Profile{
		{Name: ProfileReport, Trials: []int{1, 2, 3, 4, 5}, Seeds: []int{1, 2, 3, 4, 5}, KeySize: 16, ValueSize: 128,
			WriteThroughput: WriteThroughputSpec{FlushThresholdBytes: []int{65536, 262144, 1048576, 4194304}, Operations: 100000},
			PointRead:       PointReadSpec{SSTableCounts: []int{1, 4, 16, 64}, BloomModes: []bool{true, false}, LookupKinds: []string{"hit", "miss"}, EntriesPerSSTable: 4096, Operations: 25000},
			BloomFP:         BloomFalsePositiveSpec{InsertedKeyCounts: []int{1000, 10000, 100000}, ProbeKeys: 100000},
			Amplification:   AmplificationSpec{Operations: 250000, Keyspace: 25000, PutsPercent: 90, DeletesPercent: 10, FlushThreshold: 65536, Reads: 50000, ReadHitPercent: 50},
			CompactionPause: CompactionPauseSpec{Operations: 100000, Keyspace: 100000, FlushThreshold: 65536}, SkiplistHeight: SkiplistHeightSpec{Samples: 1000000}},
		{Name: ProfileSmoke, Trials: []int{1}, Seeds: []int{1}, KeySize: 16, ValueSize: 128,
			WriteThroughput: WriteThroughputSpec{FlushThresholdBytes: []int{65536, 4194304}, Operations: 2000},
			PointRead:       PointReadSpec{SSTableCounts: []int{1, 4}, BloomModes: []bool{true, false}, LookupKinds: []string{"hit", "miss"}, EntriesPerSSTable: 256, Operations: 1000},
			BloomFP:         BloomFalsePositiveSpec{InsertedKeyCounts: []int{1000}, ProbeKeys: 5000},
			Amplification:   AmplificationSpec{Operations: 5000, Keyspace: 500, PutsPercent: 90, DeletesPercent: 10, FlushThreshold: 16384, Reads: 1000, ReadHitPercent: 50},
			CompactionPause: CompactionPauseSpec{Operations: 5000, Keyspace: 5000, FlushThreshold: 16384}, SkiplistHeight: SkiplistHeightSpec{Samples: 10000}},
	} {
		actual, ok := BuiltinProfile(expected.Name)
		if !ok || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("%s matrix changed:\n got: %#v\nwant: %#v", expected.Name, actual, expected)
		}
	}
}

func TestBuiltinProfileMutationIsolationAndBloomTrialKeys(t *testing.T) {
	first, _ := BuiltinProfile(ProfileReport)
	first.Trials[0], first.Seeds[0], first.WriteThroughput.FlushThresholdBytes[0], first.PointRead.SSTableCounts[0], first.PointRead.BloomModes[0], first.PointRead.LookupKinds[0], first.BloomFP.InsertedKeyCounts[0] = 99, 99, 99, 99, false, "mutated", 99
	second, _ := BuiltinProfile(ProfileReport)
	if second.Trials[0] != 1 || second.Seeds[0] != 1 || second.WriteThroughput.FlushThresholdBytes[0] != 65536 || second.PointRead.SSTableCounts[0] != 1 || !second.PointRead.BloomModes[0] || second.PointRead.LookupKinds[0] != "hit" || second.BloomFP.InsertedKeyCounts[0] != 1000 {
		t.Fatal("BuiltinProfile returned mutable canonical slices")
	}
	a, p := bloomTrialKeys(1, 20, 30, 16)
	b, q := bloomTrialKeys(1, 20, 30, 16)
	c, _ := bloomTrialKeys(2, 20, 30, 16)
	seen := map[string]bool{}
	for i := range a {
		if string(a[i]) != string(b[i]) || seen[string(a[i])] {
			t.Fatal("Bloom inserted keys not reproducible/unique")
		}
		seen[string(a[i])] = true
	}
	for i := range p {
		if string(p[i]) != string(q[i]) || seen[string(p[i])] {
			t.Fatal("Bloom probes not reproducible/disjoint")
		}
		seen[string(p[i])] = true
	}
	if string(a[0]) == string(c[0]) {
		t.Fatal("Bloom seed does not affect keys")
	}
}

func TestStrictValidationRejectsMalformedResults(t *testing.T) {
	base := completeFixture(t, ProfileSmoke)
	if err := ValidateResult(base); err != nil {
		t.Fatalf("complete fixture rejected: %v", err)
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRawResult([]byte(`{"unknown":true,` + string(encoded[1:]))); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := DecodeRawResult(append(encoded, []byte(` {}`)...)); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	var histogram HeightCounts
	if err := json.Unmarshal([]byte(`[1]`), &histogram); err == nil {
		t.Fatal("short height histogram accepted")
	}

	cases := []struct {
		name string
		edit func(*RawResult)
	}{
		{"schema", func(result *RawResult) { result.Schema = "wrong" }},
		{"version", func(result *RawResult) { result.SchemaVersion++ }},
		{"profile", func(result *RawResult) { result.Profile = "wrong" }},
		{"timestamp offset", func(result *RawResult) { result.CollectedAtUTC = "2026-01-02T03:04:05+00:00" }},
		{"missing cell", func(result *RawResult) { result.WriteThroughput = result.WriteThroughput[1:] }},
		{"duplicate cell", func(result *RawResult) { result.WriteThroughput[1] = result.WriteThroughput[0] }},
		{"unexpected dimension", func(result *RawResult) { result.PointReadLatency[0].SSTableCount = 3 }},
		{"wrong seed", func(result *RawResult) { result.PointReadLatency[0].Seed++ }},
		{"invalid duration", func(result *RawResult) { result.WriteThroughput[0].ElapsedNS = 0 }},
		{"inconsistent point count", func(result *RawResult) { result.PointReadLatency[0].Found-- }},
		{"unordered quantiles", func(result *RawResult) { result.PointReadLatency[0].P95NS = result.PointReadLatency[0].P50NS - 1 }},
		{"bloom false negative", func(result *RawResult) { result.BloomFalsePositive[0].FalseNegatives = 1 }},
		{"amplification sum", func(result *RawResult) { result.Amplification[0].Puts++ }},
		{"compaction sum", func(result *RawResult) { result.CompactionPause[0].OrdinaryOperations-- }},
		{"height sum", func(result *RawResult) { result.SkiplistHeight[0].HeightCounts[0]-- }},
		{"height overflow", func(result *RawResult) {
			result.SkiplistHeight[0].HeightCounts[0], result.SkiplistHeight[0].HeightCounts[1] = ^uint64(0), 10001
		}},
		{"elapsed below max", func(result *RawResult) { result.PointReadLatency[0].ElapsedNS = result.PointReadLatency[0].MaxNS - 1 }},
		{"hit without reads", func(result *RawResult) {
			result.PointReadLatency[0].SSTableReadCalls, result.PointReadLatency[0].SSTableReadBytes = 0, 0
		}},
		{"bloom-off miss without reads", func(result *RawResult) {
			for i := range result.PointReadLatency {
				if !result.PointReadLatency[i].BloomEnabled && result.PointReadLatency[i].LookupKind == "miss" {
					result.PointReadLatency[i].SSTableReadCalls, result.PointReadLatency[i].SSTableReadBytes = 0, 0
					return
				}
			}
		}},
		{"logical live impossible", func(result *RawResult) { result.Amplification[0].LogicalLiveBytes = 1 }},
		{"write bytes overflow", func(result *RawResult) {
			result.Amplification[0].SSTableWriteBytes, result.Amplification[0].ManifestWriteBytes = ^uint64(0), 1
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := cloneResult(t, base)
			testCase.edit(&candidate)
			if err := ValidateResult(candidate); err == nil {
				t.Fatal("malformed result accepted")
			}
		})
	}
}

func TestGraphsAreGoldenAndPermutationInvariant(t *testing.T) {
	result := completeFixture(t, ProfileReport)
	first := t.TempDir()
	if err := GenerateGraphs(result, first); err != nil {
		t.Fatal(err)
	}
	permuted := cloneResult(t, result)
	reverse(permuted.WriteThroughput)
	reverse(permuted.PointReadLatency)
	reverse(permuted.BloomFalsePositive)
	reverse(permuted.Amplification)
	reverse(permuted.CompactionPause)
	reverse(permuted.SkiplistHeight)
	second := t.TempDir()
	if err := GenerateGraphs(permuted, second); err != nil {
		t.Fatal(err)
	}
	for _, name := range graphNames() {
		firstBytes, err := os.ReadFile(filepath.Join(first, name))
		if err != nil {
			t.Fatal(err)
		}
		secondBytes, err := os.ReadFile(filepath.Join(second, name))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(firstBytes, secondBytes) {
			t.Fatalf("%s changes when raw rows are permuted", name)
		}
		decoder := xml.NewDecoder(strings.NewReader(string(firstBytes)))
		for {
			_, err = decoder.Token()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("%s is not valid XML: %v", name, err)
			}
		}
		got := fmt.Sprintf("%x", sha256.Sum256(firstBytes))
		if got != expectedGraphSHA256[name] {
			t.Fatalf("%s golden bytes changed: got %s, want %s", name, got, expectedGraphSHA256[name])
		}
	}
}

func TestPointReadFixtureHasExactTablesAndBloomBypassBehavior(t *testing.T) {
	profile, _ := BuiltinProfile(ProfileReport)
	profile.PointRead.EntriesPerSSTable = 8 // preserve every frozen count without a 262k-write unit test.
	profile.PointRead.Operations = 16
	for _, count := range []int{1, 4, 16, 64} {
		fixture, err := buildPointReadFixture(profile, 1, count)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(fixture.dir) })
		engine, err := openBenchmarkEngine(fixture.dir, 1, 1<<30, true, false)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(engine.ReferencedSSTables()); got != count {
			_ = engine.Close()
			t.Fatalf("fixture has %d tables, want %d", got, count)
		}
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}

		var selected []byte
		for _, candidate := range fixture.queries["miss"] {
			counter := lsm.NewCountingFS(lsm.RealFS{})
			reader, err := openBenchmarkEngineWithFS(fixture.dir, counter, 1, 1<<30, true, false)
			if err != nil {
				t.Fatal(err)
			}
			counter.Reset()
			_, found, err := reader.Read(candidate)
			stats := counter.Snapshot()
			_ = reader.Close()
			if err != nil || found {
				t.Fatalf("bloom-on miss read = found:%t err:%v", found, err)
			}
			if stats.SSTableReadCalls == 0 {
				selected = candidate
				break
			}
		}
		if selected == nil {
			t.Fatal("fixture did not provide a Bloom-rejected in-range miss")
		}
		counter := lsm.NewCountingFS(lsm.RealFS{})
		reader, err := openBenchmarkEngineWithFS(fixture.dir, counter, 1, 1<<30, true, true)
		if err != nil {
			t.Fatal(err)
		}
		counter.Reset()
		_, found, err := reader.Read(selected)
		stats := counter.Snapshot()
		_ = reader.Close()
		if err != nil || found || stats.SSTableReadCalls == 0 || stats.SSTableReadBytes == 0 {
			t.Fatalf("bloom-off in-range miss did not read a candidate block: found=%t stats=%+v err=%v", found, stats, err)
		}
	}
}

func TestSmokeCollectionValidationAndGraphs(t *testing.T) {
	dir := t.TempDir()
	raw := filepath.Join(dir, "raw.json")
	result, err := Collect(ProfileSmoke, raw)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(result.WriteThroughput)+len(result.PointReadLatency)+len(result.BloomFalsePositive)+len(result.Amplification)+len(result.CompactionPause)+len(result.SkiplistHeight), 14; got != want {
		t.Fatalf("smoke rows = %d, want %d", got, want)
	}
	if _, err := ReadRawResult(raw); err != nil {
		t.Fatal(err)
	}
	if err := GenerateGraphsFromFile(raw, filepath.Join(dir, "graphs")); err != nil {
		t.Fatal(err)
	}
	for _, name := range graphNames() {
		info, err := os.Stat(filepath.Join(dir, "graphs", name))
		if err != nil || info.Size() == 0 {
			t.Fatalf("missing graph %s: %v", name, err)
		}
	}
}

func TestAmplificationAccountingAndReferencedSpace(t *testing.T) {
	profile, _ := BuiltinProfile(ProfileSmoke)
	row, err := collectAmplificationRow(profile, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if row.Puts != 4500 || row.Deletes != 500 || row.ReadOperations != 1000 || row.SSTableWriteBytes == 0 || row.ManifestWriteBytes == 0 || row.Compactions == 0 {
		t.Fatalf("amplification row does not include workload/flush/compaction accounting: %+v", row)
	}
	live := map[uint64]struct{}{1: {}, 3: {}}
	reads, err := collectAmplificationReads(profile, 1, live)
	if err != nil {
		t.Fatal(err)
	}
	hits := 0
	for _, read := range reads {
		if read.wantFound {
			hits++
		}
	}
	if len(reads) != 1000 || hits != 500 {
		t.Fatalf("read split = %d reads, %d hits; want 1000/500", len(reads), hits)
	}

	dir := t.TempDir()
	engine, err := openBenchmarkEngine(dir, 1, 1<<30, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(fixedKey(1, profile.KeySize), fixedValue(1, 1, profile.ValueSize), 1); err != nil {
		t.Fatal(err)
	}
	if err := engine.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	referenced, err := referencedSSTableBytes(dir, engine)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orphan.sst"), make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	withOrphan, err := referencedSSTableBytes(dir, engine)
	if err != nil {
		t.Fatal(err)
	}
	if referenced == 0 || withOrphan != referenced {
		t.Fatalf("referenced-space accounting included an orphan: before=%d after=%d", referenced, withOrphan)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionPauseClassificationUsesCounterTransition(t *testing.T) {
	dir := t.TempDir()
	const delay = time.Millisecond
	engine, err := openBenchmarkEngineWithFS(dir, delayedFS{FS: lsm.RealFS{}, delay: delay}, 1, 1, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = engine.Close() }()
	before := engine.SnapshotBenchmarkCounters()
	started := time.Now()
	if err := engine.Put(fixedKey(1, 16), fixedValue(1, 1, 128), 1); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < delay {
		t.Fatalf("non-compaction Put finished in %s despite %s delayed SSTable write", elapsed, delay)
	}
	after := engine.SnapshotBenchmarkCounters()
	if after.CompletedCompactions != before.CompletedCompactions {
		t.Fatalf("delayed non-compaction Put advanced compaction counter: before=%+v after=%+v", before, after)
	}
}

type delayedFS struct {
	lsm.FS
	delay time.Duration
}

func (fs delayedFS) Create(name string) (lsm.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return delayedFile{File: f, delay: fs.delay}, nil
}

type delayedFile struct {
	lsm.File
	delay time.Duration
}

func (file delayedFile) Write(data []byte) (int, error) {
	time.Sleep(file.delay)
	return file.File.Write(data)
}

func TestCompactionClassificationAndAccountingMechanisms(t *testing.T) {
	dir := t.TempDir()
	counted := lsm.NewCountingFS(lsm.RealFS{})
	engine, err := openBenchmarkEngineWithFS(dir, counted, 1, 1, false, false)
	if err != nil {
		t.Fatal(err)
	}
	counted.Reset()
	before := engine.SnapshotBenchmarkCounters()
	causing := -1
	for i := 0; i < 4; i++ {
		if err := engine.Put(fixedKey(uint64(i), 16), fixedValue(1, i, 128), uint64(i+1)); err != nil {
			t.Fatal(err)
		}
		after := engine.SnapshotBenchmarkCounters()
		if after.CompletedCompactions > before.CompletedCompactions {
			causing = i
		}
		before = after
	}
	if causing != 3 {
		t.Fatalf("compaction causing operation = %d, want 3", causing)
	}
	withCompaction := counted.Snapshot().SSTableWriteBytes
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	withoutDir := t.TempDir()
	without := lsm.NewCountingFS(lsm.RealFS{})
	engine, err = openBenchmarkEngineWithFS(withoutDir, without, 1, 1<<30, true, false)
	if err != nil {
		t.Fatal(err)
	}
	without.Reset()
	for i := 0; i < 4; i++ {
		if err := engine.Put(fixedKey(uint64(i), 16), fixedValue(1, i, 128), uint64(i+1)); err != nil {
			t.Fatal(err)
		}
		if err := engine.ForceFlush(); err != nil {
			t.Fatal(err)
		}
	}
	flushOnly := without.Snapshot().SSTableWriteBytes
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if withCompaction <= flushOnly {
		t.Fatalf("SSTable accounting omitted compaction output: with=%d flush-only=%d", withCompaction, flushOnly)
	}
}

func TestGraphSemanticsUseMedianWhiskersAndAllAmplificationPanels(t *testing.T) {
	result := completeFixture(t, ProfileReport)
	for i := range result.WriteThroughput {
		result.WriteThroughput[i].ElapsedNS = int64(i/4+1) * 1000
	}
	medianValue, minValue, maxValue := aggregateMedianMinMax([]float64{1, 2, 3, 4, 5})
	if medianValue != 3 || minValue != 1 || maxValue != 5 {
		t.Fatalf("aggregate = %v/%v/%v, want 3/1/5", medianValue, minValue, maxValue)
	}
	write := renderWriteThroughput(result)
	if !strings.Contains(write, `stroke="#111827"`) || !strings.Contains(write, `median ops/s`) {
		t.Fatal("write graph lacks median whisker rendering")
	}
	result.Amplification[0].SSTableWriteBytes = 999999
	for i := range result.Amplification {
		result.Amplification[i].SSTableReadBytes = uint64(100 * (i + 1))
		result.Amplification[i].ReadOperations = 10
	}
	amp := renderAmplification(result)
	for _, panel := range []string{"Write amplification", "SSTable bytes per read", "SSTable reads per read", "Space amplification"} {
		if !strings.Contains(amp, panel) {
			t.Fatalf("missing amplification panel %q", panel)
		}
	}
	if got := amplificationSSTableBytesPerRead(result.Amplification[2]); got != 30 {
		t.Fatalf("bytes/read = %v, want 30", got)
	}
	row := result.Amplification[2]
	if float64(row.SSTableWriteBytes+row.ManifestWriteBytes)/float64(row.LogicalUserWriteBytes) <= 0 || float64(row.SSTableReadCalls)/float64(row.ReadOperations) <= 0 || float64(row.ReferencedSSTableBytes)/float64(row.LogicalLiveBytes) <= 0 {
		t.Fatal("amplification panel transform invalid")
	}
}

func completeFixture(t *testing.T, profileName string) RawResult {
	t.Helper()
	profile, ok := BuiltinProfile(profileName)
	if !ok {
		t.Fatalf("profile %q missing", profileName)
	}
	result := RawResult{
		Schema:         SchemaName,
		SchemaVersion:  SchemaVersionV1,
		Profile:        profileName,
		CollectedAtUTC: "2026-01-02T03:04:05Z",
		Environment:    Environment{GoVersion: "go-test", Goos: "test", Goarch: "test", NumCPU: 1, Gomaxprocs: 1},
		Matrix:         profile,
	}
	for index, trial := range profile.Trials {
		seed := profile.Seeds[index]
		for _, threshold := range profile.WriteThroughput.FlushThresholdBytes {
			result.WriteThroughput = append(result.WriteThroughput, WriteThroughputRow{Trial: trial, Seed: seed, FlushThresholdBytes: threshold, Operations: profile.WriteThroughput.Operations, ElapsedNS: int64(1000 + trial + threshold%13)})
		}
		for _, count := range profile.PointRead.SSTableCounts {
			for _, bloom := range profile.PointRead.BloomModes {
				for _, kind := range profile.PointRead.LookupKinds {
					found, missing := int64(0), int64(profile.PointRead.Operations)
					if kind == "hit" {
						found, missing = int64(profile.PointRead.Operations), 0
					}
					result.PointReadLatency = append(result.PointReadLatency, PointReadLatencyRow{Trial: trial, Seed: seed, SSTableCount: count, BloomEnabled: bloom, LookupKind: kind, Operations: profile.PointRead.Operations, ElapsedNS: int64(10000 + trial), P50NS: 2 + int64(trial), P95NS: 4 + int64(trial), P99NS: 5 + int64(trial), MaxNS: 6 + int64(trial), Found: found, NotFound: missing, SSTableReadCalls: 1, SSTableReadBytes: 1})
				}
			}
		}
		for _, inserted := range profile.BloomFP.InsertedKeyCounts {
			result.BloomFalsePositive = append(result.BloomFalsePositive, BloomFalsePositiveRow{Trial: trial, Seed: seed, InsertedKeys: inserted, ProbeKeys: profile.BloomFP.ProbeKeys, MBits: uint64(inserted * 10), K: 7, FalsePositives: uint64(trial), FalseNegatives: 0})
		}
		puts := profile.Amplification.Operations * profile.Amplification.PutsPercent / 100
		deletes := profile.Amplification.Operations - puts
		logicalWrites := uint64(puts*(profile.KeySize+profile.ValueSize) + deletes*profile.KeySize)
		result.Amplification = append(result.Amplification, AmplificationRow{Trial: trial, Seed: seed, Operations: profile.Amplification.Operations, Puts: puts, Deletes: deletes, LogicalUserWriteBytes: logicalWrites, SSTableWriteBytes: 20 + uint64(trial), ManifestWriteBytes: 10 + uint64(trial), ReadOperations: profile.Amplification.Reads, SSTableReadCalls: 2 + uint64(trial), SSTableReadBytes: 30 + uint64(trial), LogicalLiveBytes: 144, ReferencedSSTableBytes: 200 + uint64(trial), Compactions: trial})
		result.CompactionPause = append(result.CompactionPause, CompactionPauseRow{Trial: trial, Seed: seed, Operations: profile.CompactionPause.Operations, CompactionOperations: 1, OrdinaryOperations: profile.CompactionPause.Operations - 1, CompactionP50NS: 3 + int64(trial), CompactionP95NS: 4 + int64(trial), CompactionP99NS: 5 + int64(trial), CompactionMaxNS: 6 + int64(trial), OrdinaryP50NS: 1 + int64(trial), OrdinaryP95NS: 2 + int64(trial), OrdinaryP99NS: 3 + int64(trial), OrdinaryMaxNS: 4 + int64(trial)})
		var heights HeightCounts
		heights[0] = uint64(profile.SkiplistHeight.Samples)
		result.SkiplistHeight = append(result.SkiplistHeight, SkiplistHeightRow{Trial: trial, Seed: seed, Samples: profile.SkiplistHeight.Samples, HeightCounts: heights})
	}
	return result
}

func cloneResult(t *testing.T, result RawResult) RawResult {
	t.Helper()
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var clone RawResult
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func graphNames() []string {
	return []string{writeThroughputSVG, pointReadLatencySVG, bloomFalsePositiveSVG, amplificationSVG, compactionPauseSVG, skiplistHeightSVG}
}

var expectedGraphSHA256 = map[string]string{
	writeThroughputSVG:    "2dd4c6c02bdab691027fa43a53c27302b8cba08d984e1df82ed9aca4529e06ca",
	pointReadLatencySVG:   "a0b45f383db238684f1f945721de458ab9cf9cfea80eb230f64268eef45a5247",
	bloomFalsePositiveSVG: "807fcadfe76d4c37c7d86dd07dc0e929e28717020f51cceaa15dccf95bcaa404",
	amplificationSVG:      "3f32e536dfa7e5b230d5cd5a7dea56381cdd415c8a7b62a875427f1f65c27276",
	compactionPauseSVG:    "7f674efe37903d71afd6665ef2d2bbd269a74bc0a68b0fe5a1cc4c0903ea1215",
	skiplistHeightSVG:     "51f0913b243e582399d5cf939bba6f498cc74a1a103ebf90c49b03a23e33f553",
}

func reverse[T any](values []T) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
