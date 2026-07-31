package bench

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"miniquorum/internal/lsm"
)

// benchmarkRand adapts math/rand into the internal/lsm Rand seam.
type benchmarkRand struct {
	r *rand.Rand
}

func (rnd *benchmarkRand) IntN(n int) int { return rnd.r.Intn(n) }

func newBenchmarkRand(seed int64) *benchmarkRand {
	return &benchmarkRand{r: rand.New(rand.NewSource(seed))}
}

// Collect runs one frozen profile and writes one versioned raw file.
func Collect(profileName, outPath string) (*RawResult, error) {
	profile, ok := BuiltinProfile(profileName)
	if !ok {
		return nil, fmt.Errorf("unknown profile %q", profileName)
	}
	if len(profile.Trials) == 0 || len(profile.Seeds) == 0 || len(profile.Trials) != len(profile.Seeds) {
		return nil, fmt.Errorf("invalid profile %q trial/seed mapping", profileName)
	}

	result := RawResult{
		Schema:        SchemaName,
		SchemaVersion: SchemaVersionV1,
		Profile:       profileName,
		Environment:   currentEnvironment(),
		Matrix:        profile,
	}

	for index, trial := range profile.Trials {
		seed := profile.Seeds[index]

		for _, threshold := range profile.WriteThroughput.FlushThresholdBytes {
			row, err := collectWriteThroughputRow(profile, trial, seed, threshold)
			if err != nil {
				return nil, err
			}
			result.WriteThroughput = append(result.WriteThroughput, row)
		}

		for _, tableCount := range profile.PointRead.SSTableCounts {
			fixture, err := buildPointReadFixture(profile, seed, tableCount)
			if err != nil {
				return nil, err
			}
			for _, bloomEnabled := range profile.PointRead.BloomModes {
				for _, lookupKind := range profile.PointRead.LookupKinds {
					row, err := collectPointReadRow(profile, trial, seed, fixture, tableCount, bloomEnabled, lookupKind)
					if err != nil {
						_ = os.RemoveAll(fixture.dir)
						return nil, err
					}
					result.PointReadLatency = append(result.PointReadLatency, row)
				}
			}
			_ = os.RemoveAll(fixture.dir)
		}

		for _, insertedKeys := range profile.BloomFP.InsertedKeyCounts {
			row, err := collectBloomFalsePositiveRow(profile, trial, seed, insertedKeys)
			if err != nil {
				return nil, err
			}
			result.BloomFalsePositive = append(result.BloomFalsePositive, row)
		}

		amplification, err := collectAmplificationRow(profile, trial, seed)
		if err != nil {
			return nil, err
		}
		result.Amplification = append(result.Amplification, amplification)

		compactionPause, err := collectCompactionPauseRow(profile, trial, seed)
		if err != nil {
			return nil, err
		}
		result.CompactionPause = append(result.CompactionPause, compactionPause)

		skiplist, err := collectSkiplistRow(profile, trial, seed)
		if err != nil {
			return nil, err
		}
		result.SkiplistHeight = append(result.SkiplistHeight, skiplist)
	}

	sortWriteThroughputRows(result.WriteThroughput)
	sortPointReadRows(result.PointReadLatency)
	sortBloomRows(result.BloomFalsePositive)
	sortAmplificationRows(result.Amplification)
	sortCompactionRows(result.CompactionPause)
	sortSkiplistRows(result.SkiplistHeight)

	result.CollectedAtUTC = CollectedAtUTC(time.Now())
	if err := ValidateResult(result); err != nil {
		return nil, err
	}
	if err := WriteRawResult(outPath, result); err != nil {
		return nil, err
	}
	return &result, nil
}

type pointReadFixture struct {
	dir     string
	queries map[string][][]byte
}

func collectWriteThroughputRow(profile Profile, trial, seed, threshold int) (WriteThroughputRow, error) {
	if profile.WriteThroughput.Operations <= 0 {
		return WriteThroughputRow{}, fmt.Errorf("invalid write operations")
	}
	if threshold <= 0 {
		return WriteThroughputRow{}, fmt.Errorf("invalid threshold %d", threshold)
	}

	dir, err := os.MkdirTemp("", "miniquorum-lsmbench-write-")
	if err != nil {
		return WriteThroughputRow{}, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	keys, values := makePrecomputedPairs(profile.WriteThroughput.Operations, seed, profile.KeySize, profile.ValueSize)

	engine, err := openBenchmarkEngine(dir, seed, threshold, false, false)
	if err != nil {
		return WriteThroughputRow{}, err
	}

	start := time.Now()
	for i := 0; i < profile.WriteThroughput.Operations; i++ {
		if err := engine.Put(keys[i], values[i], uint64(i+1)); err != nil {
			_ = engine.Close()
			return WriteThroughputRow{}, err
		}
	}
	if err := engine.ForceFlush(); err != nil {
		_ = engine.Close()
		return WriteThroughputRow{}, err
	}
	elapsed := time.Since(start)
	if err := engine.Close(); err != nil {
		return WriteThroughputRow{}, err
	}

	return WriteThroughputRow{
		Trial:               trial,
		Seed:                seed,
		FlushThresholdBytes: threshold,
		Operations:          profile.WriteThroughput.Operations,
		ElapsedNS:           elapsed.Nanoseconds(),
	}, nil
}

func buildPointReadFixture(profile Profile, seed, tableCount int) (pointReadFixture, error) {
	if tableCount <= 0 {
		return pointReadFixture{}, fmt.Errorf("invalid table count %d", tableCount)
	}
	if profile.PointRead.EntriesPerSSTable <= 0 {
		return pointReadFixture{}, fmt.Errorf("invalid entries per sstable %d", profile.PointRead.EntriesPerSSTable)
	}

	dir, err := os.MkdirTemp("", "miniquorum-lsmbench-point-read-")
	if err != nil {
		return pointReadFixture{}, err
	}

	// Keep automatic compaction out of this fixture only: each explicit flush
	// must leave one table behind so the selected table-count cell is exact.
	engine, err := openBenchmarkEngine(dir, seed, 1<<30, true, false)
	if err != nil {
		_ = os.RemoveAll(dir)
		return pointReadFixture{}, err
	}

	hitSet := make(map[uint64]struct{}, tableCount*profile.PointRead.EntriesPerSSTable)
	for table := 0; table < tableCount; table++ {
		// Every table covers an overlapping interval, but it stores only every
		// other key in that interval. The odd keys are therefore deterministic
		// in-range misses that still survive the min/max candidate check.
		start := uint64(table * profile.PointRead.EntriesPerSSTable)
		for entry := 0; entry < profile.PointRead.EntriesPerSSTable; entry++ {
			key := start + uint64(2*entry)
			sequence := table*profile.PointRead.EntriesPerSSTable + entry + 1
			if err := engine.Put(fixedKey(key, profile.KeySize), fixedValue(seed, sequence, profile.ValueSize), uint64(sequence)); err != nil {
				_ = engine.Close()
				_ = os.RemoveAll(dir)
				return pointReadFixture{}, err
			}
			hitSet[key] = struct{}{}
		}
		if err := engine.ForceFlush(); err != nil {
			_ = engine.Close()
			_ = os.RemoveAll(dir)
			return pointReadFixture{}, err
		}
	}
	if err := engine.Close(); err != nil {
		_ = os.RemoveAll(dir)
		return pointReadFixture{}, err
	}

	hits := make([]uint64, 0, len(hitSet))
	for key := range hitSet {
		hits = append(hits, key)
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	misses := make([]uint64, 0, tableCount*profile.PointRead.EntriesPerSSTable)
	missMax := uint64((tableCount+1)*profile.PointRead.EntriesPerSSTable - 2)
	for key := uint64(0); key <= missMax; key++ {
		if _, found := hitSet[key]; !found {
			misses = append(misses, key)
		}
	}
	if len(misses) == 0 {
		_ = os.RemoveAll(dir)
		return pointReadFixture{}, errors.New("point-read fixture has no misses")
	}

	queries := make(map[string][][]byte, 2)
	queries["hit"] = makePointReadQueries(hits, profile.PointRead.Operations, seed, tableCount, profile.KeySize)
	queries["miss"] = makePointReadQueries(misses, profile.PointRead.Operations, seed, tableCount, profile.KeySize)
	return pointReadFixture{dir: dir, queries: queries}, nil
}

func makePointReadQueries(pool []uint64, operations, seed, tableCount, keySize int) [][]byte {
	queries := make([][]byte, operations)
	rnd := newBenchmarkRand(int64(seed)*1_000_003 + int64(tableCount)*97 + int64(len(pool))*31)
	for i := range queries {
		queries[i] = fixedKey(pool[rnd.IntN(len(pool))], keySize)
	}
	return queries
}

func collectPointReadRow(profile Profile, trial, seed int, fixture pointReadFixture, sstableCount int, bloomEnabled bool, lookupKind string) (PointReadLatencyRow, error) {
	if fixture.dir == "" {
		return PointReadLatencyRow{}, errors.New("missing point-read fixture")
	}
	if sstableCount <= 0 {
		return PointReadLatencyRow{}, fmt.Errorf("invalid table count %d", sstableCount)
	}
	if profile.PointRead.Operations <= 0 {
		return PointReadLatencyRow{}, fmt.Errorf("invalid point-read operation count %d", profile.PointRead.Operations)
	}
	if lookupKind != "hit" && lookupKind != "miss" {
		return PointReadLatencyRow{}, fmt.Errorf("invalid lookup kind %q", lookupKind)
	}

	queries := fixture.queries[lookupKind]
	if len(queries) != profile.PointRead.Operations {
		return PointReadLatencyRow{}, fmt.Errorf("no keys for lookup kind %s", lookupKind)
	}

	counterFS := lsm.NewCountingFS(lsm.RealFS{})
	engine, err := openBenchmarkEngineWithFS(fixture.dir, counterFS, seed, 1<<20, true, !bloomEnabled)
	if err != nil {
		return PointReadLatencyRow{}, err
	}
	defer func() { _ = engine.Close() }()

	counterFS.Reset()
	latencies := make([]int64, 0, profile.PointRead.Operations)
	var found uint64
	var notFound uint64
	outerStart := time.Now()
	for _, key := range queries {
		start := time.Now()
		_, foundKey, readErr := engine.Read(key)
		latencies = append(latencies, positiveDuration(time.Since(start).Nanoseconds()))
		if readErr != nil {
			return PointReadLatencyRow{}, readErr
		}
		if foundKey {
			found++
		} else {
			notFound++
		}
	}
	elapsed := positiveDuration(time.Since(outerStart).Nanoseconds())
	p50, p95, p99, max := quantileLatency(latencies)
	stats := counterFS.Snapshot()

	return PointReadLatencyRow{
		Trial:            trial,
		Seed:             seed,
		SSTableCount:     sstableCount,
		BloomEnabled:     bloomEnabled,
		LookupKind:       lookupKind,
		Operations:       profile.PointRead.Operations,
		ElapsedNS:        elapsed,
		P50NS:            p50,
		P95NS:            p95,
		P99NS:            p99,
		MaxNS:            max,
		Found:            int64(found),
		NotFound:         int64(notFound),
		SSTableReadCalls: stats.SSTableReadCalls,
		SSTableReadBytes: stats.SSTableReadBytes,
	}, nil
}

func collectBloomFalsePositiveRow(profile Profile, trial, seed, insertedKeys int) (BloomFalsePositiveRow, error) {
	if insertedKeys <= 0 {
		return BloomFalsePositiveRow{}, fmt.Errorf("invalid inserted key count %d", insertedKeys)
	}
	if profile.BloomFP.ProbeKeys <= 0 {
		return BloomFalsePositiveRow{}, fmt.Errorf("invalid probe key count %d", profile.BloomFP.ProbeKeys)
	}

	dir, err := os.MkdirTemp("", "miniquorum-lsmbench-bloom-")
	if err != nil {
		return BloomFalsePositiveRow{}, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// One ForceFlush from a deliberately large threshold creates exactly one
	// production SSTable/Bloom filter for this cell.
	engine, err := openBenchmarkEngine(dir, seed, 1<<30, false, false)
	if err != nil {
		return BloomFalsePositiveRow{}, err
	}
	inserted, probes := bloomTrialKeys(seed, insertedKeys, profile.BloomFP.ProbeKeys, profile.KeySize)
	for i, key := range inserted {
		if err := engine.Put(key, fixedValue(seed, i, profile.ValueSize), uint64(i+1)); err != nil {
			_ = engine.Close()
			return BloomFalsePositiveRow{}, err
		}
	}
	if err := engine.ForceFlush(); err != nil {
		_ = engine.Close()
		return BloomFalsePositiveRow{}, err
	}
	if err := engine.Close(); err != nil {
		return BloomFalsePositiveRow{}, err
	}

	refs, err := os.ReadDir(dir)
	if err != nil {
		return BloomFalsePositiveRow{}, err
	}
	var sstableNames []string
	for _, entry := range refs {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue
		}
		sstableNames = append(sstableNames, entry.Name())
	}
	if len(sstableNames) != 1 {
		return BloomFalsePositiveRow{}, fmt.Errorf("bloom fixture has %d SSTables, want 1", len(sstableNames))
	}
	file, err := os.Open(filepath.Join(dir, sstableNames[0]))
	if err != nil {
		return BloomFalsePositiveRow{}, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return BloomFalsePositiveRow{}, err
	}
	reader, err := lsm.OpenSSTable(file, info.Size())
	_ = file.Close()
	if err != nil {
		return BloomFalsePositiveRow{}, err
	}
	mBits, k := reader.BloomBitCount(), reader.BloomHashCount()
	if mBits == 0 || k == 0 {
		return BloomFalsePositiveRow{}, errors.New("missing bloom metadata")
	}

	var falsePositives uint64
	var falseNegatives uint64
	for _, key := range inserted {
		if !reader.BloomMayContain(key) {
			falseNegatives++
		}
	}
	for _, key := range probes {
		if reader.BloomMayContain(key) {
			falsePositives++
		}
	}

	return BloomFalsePositiveRow{
		Trial:          trial,
		Seed:           seed,
		InsertedKeys:   insertedKeys,
		ProbeKeys:      profile.BloomFP.ProbeKeys,
		MBits:          mBits,
		K:              k,
		FalsePositives: falsePositives,
		FalseNegatives: falseNegatives,
	}, nil
}

func bloomTrialKeys(seed, insertedCount, probeCount, keySize int) ([][]byte, [][]byte) {
	rnd := newBenchmarkRand(int64(seed)*1_000_003 + 17)
	seen := make(map[uint64]struct{}, insertedCount+probeCount)
	next := func() []byte {
		for {
			v := uint64(rnd.r.Int63())
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				return fixedKey(v, keySize)
			}
		}
	}
	inserted := make([][]byte, insertedCount)
	probes := make([][]byte, probeCount)
	for i := range inserted {
		inserted[i] = next()
	}
	for i := range probes {
		probes[i] = next()
	}
	return inserted, probes
}

func collectAmplificationRow(profile Profile, trial, seed int) (AmplificationRow, error) {
	if profile.Amplification.Operations <= 0 || profile.Amplification.Keyspace <= 0 {
		return AmplificationRow{}, errors.New("invalid amplification profile")
	}
	if profile.Amplification.PutsPercent+profile.Amplification.DeletesPercent != 100 {
		return AmplificationRow{}, errors.New("invalid amplification operation split")
	}
	if profile.Amplification.Reads < 0 {
		return AmplificationRow{}, fmt.Errorf("invalid read count %d", profile.Amplification.Reads)
	}
	if profile.Amplification.ReadHitPercent < 0 || profile.Amplification.ReadHitPercent > 100 {
		return AmplificationRow{}, fmt.Errorf("invalid read-hit percentage %d", profile.Amplification.ReadHitPercent)
	}

	dir, err := os.MkdirTemp("", "miniquorum-lsmbench-amplification-")
	if err != nil {
		return AmplificationRow{}, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	operations := make([]amplificationWork, profile.Amplification.Operations)
	live := make(map[uint64]struct{}, profile.Amplification.Keyspace)
	workRand := newBenchmarkRand(int64(seed) + 1_000_000_000)
	puts := profile.Amplification.Operations * profile.Amplification.PutsPercent / 100
	deletes := profile.Amplification.Operations - puts
	if puts <= 0 || deletes <= 0 || puts+deletes != profile.Amplification.Operations {
		return AmplificationRow{}, errors.New("amplification operation split is not exactly representable")
	}
	kinds := make([]bool, profile.Amplification.Operations)
	for i := 0; i < puts; i++ {
		kinds[i] = true
	}
	for i := len(kinds) - 1; i > 0; i-- {
		j := workRand.IntN(i + 1)
		kinds[i], kinds[j] = kinds[j], kinds[i]
	}
	for i := 0; i < profile.Amplification.Operations; i++ {
		put := kinds[i]
		key := uint64(workRand.IntN(profile.Amplification.Keyspace))
		operations[i] = amplificationWork{key: key, keyBytes: fixedKey(key, profile.KeySize), value: fixedValue(seed, i, profile.ValueSize), put: put, sequence: uint64(i + 1)}
		if put {
			live[key] = struct{}{}
		} else {
			delete(live, key)
		}
	}
	if puts+deletes != profile.Amplification.Operations {
		return AmplificationRow{}, fmt.Errorf("amplification split mismatch")
	}

	countingFS := lsm.NewCountingFS(lsm.RealFS{})
	engine, err := openBenchmarkEngineWithFS(dir, countingFS, seed, profile.Amplification.FlushThreshold, false, false)
	if err != nil {
		return AmplificationRow{}, err
	}
	// Exclude initial MANIFEST setup from the measured write-accounting cell.
	countingFS.Reset()
	before := engine.SnapshotBenchmarkCounters()
	for _, work := range operations {
		if work.put {
			if err := engine.Put(work.keyBytes, work.value, work.sequence); err != nil {
				_ = engine.Close()
				return AmplificationRow{}, err
			}
		} else {
			if err := engine.Delete(work.keyBytes, work.sequence); err != nil {
				_ = engine.Close()
				return AmplificationRow{}, err
			}
		}
	}
	if err := engine.ForceFlush(); err != nil {
		_ = engine.Close()
		return AmplificationRow{}, err
	}
	after := engine.SnapshotBenchmarkCounters()
	writeStats := countingFS.Snapshot()
	compactions := int(after.CompletedCompactions - before.CompletedCompactions)

	reads, err := collectAmplificationReads(profile, seed, live)
	if err != nil {
		_ = engine.Close()
		return AmplificationRow{}, err
	}
	countingFS.Reset()
	for _, read := range reads {
		_, found, err := engine.Read(read.key)
		if err != nil {
			_ = engine.Close()
			return AmplificationRow{}, err
		}
		if found != read.wantFound {
			_ = engine.Close()
			return AmplificationRow{}, fmt.Errorf("amplification read result mismatch")
		}
	}
	readStats := countingFS.Snapshot()

	referencedBytes, err := referencedSSTableBytes(dir, engine)
	if err != nil {
		_ = engine.Close()
		return AmplificationRow{}, err
	}
	if err := engine.Close(); err != nil {
		return AmplificationRow{}, err
	}

	logicalUserWriteBytes := uint64(puts*profile.KeySize + puts*profile.ValueSize + deletes*profile.KeySize)
	logicalLiveBytes := uint64(len(live)) * uint64(profile.KeySize+profile.ValueSize)
	if profile.Amplification.Reads != len(reads) {
		return AmplificationRow{}, fmt.Errorf("amplification reads mismatch")
	}
	if len(reads) == 0 {
		return AmplificationRow{}, fmt.Errorf("amplification required zero reads")
	}

	return AmplificationRow{
		Trial:                  trial,
		Seed:                   seed,
		Operations:             profile.Amplification.Operations,
		Puts:                   puts,
		Deletes:                deletes,
		LogicalUserWriteBytes:  logicalUserWriteBytes,
		SSTableWriteBytes:      writeStats.SSTableWriteBytes,
		ManifestWriteBytes:     writeStats.ManifestWriteBytes,
		ReadOperations:         profile.Amplification.Reads,
		SSTableReadCalls:       readStats.SSTableReadCalls,
		SSTableReadBytes:       readStats.SSTableReadBytes,
		LogicalLiveBytes:       logicalLiveBytes,
		ReferencedSSTableBytes: referencedBytes,
		Compactions:            compactions,
	}, nil
}

type amplificationWork struct {
	key      uint64
	keyBytes []byte
	value    []byte
	put      bool
	sequence uint64
}

type amplificationRead struct {
	key       []byte
	wantFound bool
}

func collectAmplificationReads(profile Profile, seed int, live map[uint64]struct{}) ([]amplificationRead, error) {
	if profile.Amplification.Reads <= 0 {
		return nil, fmt.Errorf("invalid reads %d", profile.Amplification.Reads)
	}
	if profile.Amplification.ReadHitPercent < 0 || profile.Amplification.ReadHitPercent > 100 {
		return nil, fmt.Errorf("invalid read-hit percent %d", profile.Amplification.ReadHitPercent)
	}

	liveKeys := make([]uint64, 0, len(live))
	for key := range live {
		liveKeys = append(liveKeys, key)
	}
	sort.Slice(liveKeys, func(i, j int) bool { return liveKeys[i] < liveKeys[j] })
	missKeys := make([]uint64, 0, max(profile.Amplification.Keyspace-len(live), 0))
	for key := 0; key < profile.Amplification.Keyspace; key++ {
		if _, found := live[uint64(key)]; !found {
			missKeys = append(missKeys, uint64(key))
		}
	}
	if len(liveKeys) == 0 && len(missKeys) == 0 {
		return nil, errors.New("no keys available for reads")
	}

	r := newBenchmarkRand(int64(seed) + 2_000_000_000)
	hits := profile.Amplification.Reads * profile.Amplification.ReadHitPercent / 100
	misses := profile.Amplification.Reads - hits
	if hits <= 0 || misses <= 0 || len(liveKeys) == 0 || len(missKeys) == 0 {
		return nil, errors.New("amplification cannot construct exact hit/miss split")
	}
	reads := make([]amplificationRead, 0, profile.Amplification.Reads)
	for i := 0; i < hits; i++ {
		reads = append(reads, amplificationRead{key: fixedKey(liveKeys[r.IntN(len(liveKeys))], profile.KeySize), wantFound: true})
	}
	for i := 0; i < misses; i++ {
		reads = append(reads, amplificationRead{key: fixedKey(missKeys[r.IntN(len(missKeys))], profile.KeySize), wantFound: false})
	}
	for i := len(reads) - 1; i > 0; i-- {
		j := r.IntN(i + 1)
		reads[i], reads[j] = reads[j], reads[i]
	}
	return reads, nil
}

func collectCompactionPauseRow(profile Profile, trial, seed int) (CompactionPauseRow, error) {
	if profile.CompactionPause.Operations <= 0 {
		return CompactionPauseRow{}, fmt.Errorf("invalid compaction pause operation count %d", profile.CompactionPause.Operations)
	}
	if profile.CompactionPause.Keyspace <= 0 {
		return CompactionPauseRow{}, fmt.Errorf("invalid compaction keyspace %d", profile.CompactionPause.Keyspace)
	}

	dir, err := os.MkdirTemp("", "miniquorum-lsmbench-compaction-")
	if err != nil {
		return CompactionPauseRow{}, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	engine, err := openBenchmarkEngine(dir, seed, profile.CompactionPause.FlushThreshold, false, false)
	if err != nil {
		return CompactionPauseRow{}, err
	}
	defer func() { _ = engine.Close() }()

	r := newBenchmarkRand(int64(seed) + 3_000_000_000)
	work := make([]compactionWork, profile.CompactionPause.Operations)
	for i := 0; i < profile.CompactionPause.Operations; i++ {
		key := uint64(r.IntN(profile.CompactionPause.Keyspace))
		work[i] = compactionWork{key: fixedKey(key, profile.KeySize), value: fixedValue(seed, i, profile.ValueSize), sequence: uint64(i + 1)}
	}

	before := engine.SnapshotBenchmarkCounters()
	compactionSamples := make([]int64, 0, profile.CompactionPause.Operations)
	ordinarySamples := make([]int64, 0, profile.CompactionPause.Operations)
	var compactionOps int
	var ordinaryOps int
	for _, operation := range work {
		start := time.Now()
		if err := engine.Put(operation.key, operation.value, operation.sequence); err != nil {
			return CompactionPauseRow{}, err
		}
		elapsed := positiveDuration(time.Since(start).Nanoseconds())
		// Classification is deliberately outside the measured synchronous Put.
		after := engine.SnapshotBenchmarkCounters()
		if after.CompletedCompactions > before.CompletedCompactions {
			compactionOps++
			compactionSamples = append(compactionSamples, elapsed)
		} else {
			ordinaryOps++
			ordinarySamples = append(ordinarySamples, elapsed)
		}
		before = after
	}
	if err := engine.ForceFlush(); err != nil {
		return CompactionPauseRow{}, err
	}
	if compactionOps == 0 {
		return CompactionPauseRow{}, fmt.Errorf("compaction-pause trial %d requires compaction event", trial)
	}
	if compactionOps+ordinaryOps != profile.CompactionPause.Operations {
		return CompactionPauseRow{}, fmt.Errorf("compaction-pause classification mismatch")
	}
	if len(compactionSamples) == 0 || len(ordinarySamples) == 0 {
		return CompactionPauseRow{}, fmt.Errorf("compaction-pause requires both operation classes")
	}

	cp50, cp95, cp99, cpMax := quantileLatency(compactionSamples)
	op50, op95, op99, opMax := quantileLatency(ordinarySamples)
	return CompactionPauseRow{
		Trial:                trial,
		Seed:                 seed,
		Operations:           profile.CompactionPause.Operations,
		CompactionOperations: compactionOps,
		OrdinaryOperations:   ordinaryOps,
		CompactionP50NS:      cp50,
		CompactionP95NS:      cp95,
		CompactionP99NS:      cp99,
		CompactionMaxNS:      cpMax,
		OrdinaryP50NS:        op50,
		OrdinaryP95NS:        op95,
		OrdinaryP99NS:        op99,
		OrdinaryMaxNS:        opMax,
	}, nil
}

type compactionWork struct {
	key      []byte
	value    []byte
	sequence uint64
}

func collectSkiplistRow(profile Profile, trial, seed int) (SkiplistHeightRow, error) {
	if profile.SkiplistHeight.Samples <= 0 {
		return SkiplistHeightRow{}, fmt.Errorf("invalid skiplist sample count %d", profile.SkiplistHeight.Samples)
	}

	r := newBenchmarkRand(int64(seed))
	row := SkiplistHeightRow{
		Trial:   trial,
		Seed:    seed,
		Samples: profile.SkiplistHeight.Samples,
	}
	for i := 0; i < profile.SkiplistHeight.Samples; i++ {
		height := lsm.RandomHeight(r)
		if height < 1 || height > 16 {
			return SkiplistHeightRow{}, fmt.Errorf("invalid skiplist height %d", height)
		}
		row.HeightCounts[height-1]++
	}
	return row, nil
}

func makePrecomputedPairs(count, seed, keySize, valueSize int) (keys, values [][]byte) {
	keys = make([][]byte, count)
	values = make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = fixedKey(uint64(i), keySize)
		values[i] = fixedValue(seed, i, valueSize)
	}
	return keys, values
}

func openBenchmarkEngine(dir string, seed, flushThreshold int, disableAutoCompaction, skipBloom bool) (*lsm.Engine, error) {
	return openBenchmarkEngineWithFS(dir, lsm.RealFS{}, seed, flushThreshold, disableAutoCompaction, skipBloom)
}

func openBenchmarkEngineWithFS(dir string, fs lsm.FS, seed, flushThreshold int, disableAutoCompaction, skipBloom bool) (*lsm.Engine, error) {
	if fs == nil {
		fs = lsm.RealFS{}
	}
	if flushThreshold <= 0 {
		return nil, fmt.Errorf("invalid flush threshold %d", flushThreshold)
	}
	return lsm.Open(dir, lsm.Options{
		FS:                    fs,
		Rand:                  newBenchmarkRand(int64(seed)),
		FlushThreshold:        int64(flushThreshold),
		DisableAutoCompaction: disableAutoCompaction,
		SkipBloom:             skipBloom,
	})
}

func referencedSSTableBytes(dir string, engine *lsm.Engine) (uint64, error) {
	if engine == nil {
		return 0, errors.New("missing engine")
	}
	references := engine.ReferencedSSTables()
	var total uint64
	for _, ref := range references {
		info, err := os.Stat(filepath.Join(dir, ref))
		if err != nil {
			return 0, err
		}
		total += uint64(info.Size())
	}
	return total, nil
}

func fixedKey(v uint64, size int) []byte {
	if size < 0 {
		return nil
	}
	key := make([]byte, size)
	for index := 0; index < 8; index++ {
		key[size-1-index] = byte(v >> (8 * index))
	}
	return key
}

func fixedValue(seed, seq, size int) []byte {
	value := make([]byte, size)
	fill := byte(seed + seq + 13)
	for i := range value {
		value[i] = fill
	}
	return value
}

func currentEnvironment() Environment {
	return Environment{
		GoVersion:  runtime.Version(),
		Goos:       runtime.GOOS,
		Goarch:     runtime.GOARCH,
		NumCPU:     runtime.NumCPU(),
		Gomaxprocs: runtime.GOMAXPROCS(0),
	}
}

func positiveDuration(value int64) int64 {
	if value <= 0 {
		return 1
	}
	return value
}

func quantileLatency(values []int64) (p50, p95, p99, max int64) {
	if len(values) == 0 {
		return 0, 0, 0, 0
	}
	sorted := append(make([]int64, 0, len(values)), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	max = sorted[len(sorted)-1]
	p50 = nearestRank(sorted, 0.50)
	p95 = nearestRank(sorted, 0.95)
	p99 = nearestRank(sorted, 0.99)
	return p50, p95, p99, max
}

func nearestRank(sorted []int64, q float64) int64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	idx := int(math.Ceil(q*float64(n))) - 1
	if idx < 0 {
		return sorted[0]
	}
	if idx >= n {
		return sorted[n-1]
	}
	return sorted[idx]
}

func sortWriteThroughputRows(rows []WriteThroughputRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Trial == rows[j].Trial {
			return rows[i].FlushThresholdBytes < rows[j].FlushThresholdBytes
		}
		return rows[i].Trial < rows[j].Trial
	})
}

func sortPointReadRows(rows []PointReadLatencyRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Trial == rows[j].Trial {
			if rows[i].SSTableCount == rows[j].SSTableCount {
				if rows[i].BloomEnabled == rows[j].BloomEnabled {
					return rows[i].LookupKind < rows[j].LookupKind
				}
				return !rows[i].BloomEnabled && rows[j].BloomEnabled
			}
			return rows[i].SSTableCount < rows[j].SSTableCount
		}
		return rows[i].Trial < rows[j].Trial
	})
}

func sortBloomRows(rows []BloomFalsePositiveRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Trial == rows[j].Trial {
			return rows[i].InsertedKeys < rows[j].InsertedKeys
		}
		return rows[i].Trial < rows[j].Trial
	})
}

func sortAmplificationRows(rows []AmplificationRow) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Trial < rows[j].Trial })
}

func sortCompactionRows(rows []CompactionPauseRow) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Trial < rows[j].Trial })
}

func sortSkiplistRows(rows []SkiplistHeightRow) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Trial < rows[j].Trial })
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
