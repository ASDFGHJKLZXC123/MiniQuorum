package bench

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

func ValidateResult(result RawResult) error {
	if result.Schema != SchemaName {
		return fmt.Errorf("schema mismatch: got %q, want %q", result.Schema, SchemaName)
	}
	if result.SchemaVersion != SchemaVersionV1 {
		return fmt.Errorf("schema version mismatch: got %d, want %d", result.SchemaVersion, SchemaVersionV1)
	}
	if result.Profile != ProfileSmoke && result.Profile != ProfileReport {
		return fmt.Errorf("unknown profile %q", result.Profile)
	}
	if result.CollectedAtUTC == "" {
		return fmt.Errorf("missing collected_at_utc")
	}
	collected, err := time.Parse(time.RFC3339, result.CollectedAtUTC)
	if err != nil {
		return fmt.Errorf("invalid collected_at_utc %q: %w", result.CollectedAtUTC, err)
	}
	if !strings.HasSuffix(result.CollectedAtUTC, "Z") || collected.Location() != time.UTC {
		return fmt.Errorf("collected_at_utc is not UTC: %q", result.CollectedAtUTC)
	}
	if err := validateEnvironment(result.Environment); err != nil {
		return err
	}

	profile, ok := BuiltinProfile(result.Profile)
	if !ok {
		return fmt.Errorf("unknown profile %q", result.Profile)
	}
	if !reflect.DeepEqual(profile, result.Matrix) {
		return fmt.Errorf("matrix mismatch for profile %q", result.Profile)
	}

	if err := validateTrialSeedMapping(profile, result.Matrix); err != nil {
		return err
	}

	expected := ExpectedRowsByFamily(result.Profile)
	if expected == nil {
		return fmt.Errorf("profile %q has no expected layout", result.Profile)
	}
	if len(result.WriteThroughput) != expected["write-throughput"] {
		return fmt.Errorf("write-throughput row count %d, want %d", len(result.WriteThroughput), expected["write-throughput"])
	}
	if len(result.PointReadLatency) != expected["point-read-latency"] {
		return fmt.Errorf("point-read-latency row count %d, want %d", len(result.PointReadLatency), expected["point-read-latency"])
	}
	if len(result.BloomFalsePositive) != expected["bloom-false-positive"] {
		return fmt.Errorf("bloom-false-positive row count %d, want %d", len(result.BloomFalsePositive), expected["bloom-false-positive"])
	}
	if len(result.Amplification) != expected["amplification"] {
		return fmt.Errorf("amplification row count %d, want %d", len(result.Amplification), expected["amplification"])
	}
	if len(result.CompactionPause) != expected["compaction-pause"] {
		return fmt.Errorf("compaction-pause row count %d, want %d", len(result.CompactionPause), expected["compaction-pause"])
	}
	if len(result.SkiplistHeight) != expected["skiplist-height"] {
		return fmt.Errorf("skiplist-height row count %d, want %d", len(result.SkiplistHeight), expected["skiplist-height"])
	}

	if err := validateWriteThroughputRows(result.WriteThroughput, profile); err != nil {
		return err
	}
	if err := validatePointReadRows(result.PointReadLatency, profile); err != nil {
		return err
	}
	if err := validateBloomRows(result.BloomFalsePositive, profile); err != nil {
		return err
	}
	if err := validateAmplificationRows(result.Amplification, profile); err != nil {
		return err
	}
	if err := validateCompactionPauseRows(result.CompactionPause, profile); err != nil {
		return err
	}
	if err := validateSkiplistRows(result.SkiplistHeight, profile); err != nil {
		return err
	}
	return nil
}

func validateEnvironment(environment Environment) error {
	if environment.GoVersion == "" {
		return fmt.Errorf("environment.go_version is required")
	}
	if environment.Goos == "" {
		return fmt.Errorf("environment.goos is required")
	}
	if environment.Goarch == "" {
		return fmt.Errorf("environment.goarch is required")
	}
	if environment.NumCPU <= 0 {
		return fmt.Errorf("environment.num_cpu must be positive")
	}
	if environment.Gomaxprocs <= 0 {
		return fmt.Errorf("environment.gomaxprocs must be positive")
	}
	return nil
}

func validateTrialSeedMapping(profile Profile, matrix Profile) error {
	if len(profile.Trials) != len(profile.Seeds) {
		return fmt.Errorf("profile %q trial/seed length mismatch", matrix.Name)
	}
	seen := make(map[int]bool, len(profile.Trials))
	for index, trial := range profile.Trials {
		if trial <= 0 {
			return fmt.Errorf("profile %q has non-positive trial %d", matrix.Name, trial)
		}
		if seen[trial] {
			return fmt.Errorf("profile %q has duplicate trial %d", matrix.Name, trial)
		}
		seen[trial] = true
		if profile.Seeds[index] <= 0 {
			return fmt.Errorf("profile %q has non-positive seed %d", matrix.Name, profile.Seeds[index])
		}
	}
	return nil
}

func validateWriteThroughputRows(rows []WriteThroughputRow, profile Profile) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seed, ok := profile.SeedForTrial(row.Trial)
		if !ok {
			return fmt.Errorf("write-throughput unexpected trial %d", row.Trial)
		}
		if row.Seed != seed {
			return fmt.Errorf("write-throughput trial %d seed %d should map to %d", row.Trial, row.Seed, seed)
		}
		if !containsInt(profile.WriteThroughput.FlushThresholdBytes, row.FlushThresholdBytes) {
			return fmt.Errorf("write-throughput trial %d unexpected threshold %d", row.Trial, row.FlushThresholdBytes)
		}
		if row.Operations != profile.WriteThroughput.Operations {
			return fmt.Errorf("write-throughput trial %d threshold %d operations %d, want %d", row.Trial, row.FlushThresholdBytes, row.Operations, profile.WriteThroughput.Operations)
		}
		if row.ElapsedNS <= 0 {
			return fmt.Errorf("write-throughput trial %d threshold %d requires positive elapsed_ns", row.Trial, row.FlushThresholdBytes)
		}
		cell := writeCell(row.Trial, row.FlushThresholdBytes)
		if _, duplicate := seen[cell]; duplicate {
			return fmt.Errorf("write-throughput duplicate cell %s", cell)
		}
		seen[cell] = struct{}{}
	}
	if len(seen) != len(profile.Trials)*len(profile.WriteThroughput.FlushThresholdBytes) {
		return fmt.Errorf("write-throughput missing or extra cells")
	}
	return nil
}

func validatePointReadRows(rows []PointReadLatencyRow, profile Profile) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seed, ok := profile.SeedForTrial(row.Trial)
		if !ok {
			return fmt.Errorf("point-read unexpected trial %d", row.Trial)
		}
		if row.Seed != seed {
			return fmt.Errorf("point-read trial %d seed %d should map to %d", row.Trial, row.Seed, seed)
		}
		if !containsInt(profile.PointRead.SSTableCounts, row.SSTableCount) {
			return fmt.Errorf("point-read %s unexpected sstable_count", pointReadCell(row))
		}
		if !containsBool(profile.PointRead.BloomModes, row.BloomEnabled) {
			return fmt.Errorf("point-read %s unexpected bloom mode", pointReadCell(row))
		}
		if !containsString(profile.PointRead.LookupKinds, row.LookupKind) {
			return fmt.Errorf("point-read %s unexpected lookup kind", pointReadCell(row))
		}
		if row.Operations != profile.PointRead.Operations {
			return fmt.Errorf("point-read %s operations %d, want %d", pointReadCell(row), row.Operations, profile.PointRead.Operations)
		}
		if row.ElapsedNS <= 0 {
			return fmt.Errorf("point-read %s requires positive elapsed_ns", pointReadCell(row))
		}
		if row.Found < 0 || row.NotFound < 0 {
			return fmt.Errorf("point-read %s has negative found/not_found", pointReadCell(row))
		}
		if row.Found+row.NotFound != int64(row.Operations) {
			return fmt.Errorf("point-read %s found+not_found mismatch", pointReadCell(row))
		}
		if row.P50NS <= 0 || row.P95NS <= 0 || row.P99NS <= 0 || row.MaxNS <= 0 {
			return fmt.Errorf("point-read %s has non-positive percentile", pointReadCell(row))
		}
		if row.P50NS > row.P95NS || row.P95NS > row.P99NS || row.P99NS > row.MaxNS {
			return fmt.Errorf("point-read %s unordered quantiles", pointReadCell(row))
		}
		if row.ElapsedNS < row.MaxNS {
			return fmt.Errorf("point-read %s elapsed_ns is below max_ns", pointReadCell(row))
		}
		if row.LookupKind == "hit" && row.Found != int64(row.Operations) {
			return fmt.Errorf("point-read %s expected all hit lookups to be found", pointReadCell(row))
		}
		if row.LookupKind == "hit" && (row.SSTableReadCalls == 0 || row.SSTableReadBytes == 0) {
			return fmt.Errorf("point-read %s hit has no SSTable reads", pointReadCell(row))
		}
		if !row.BloomEnabled && row.LookupKind == "miss" && (row.SSTableReadCalls == 0 || row.SSTableReadBytes == 0) {
			return fmt.Errorf("point-read %s Bloom-off miss has no SSTable reads", pointReadCell(row))
		}
		if row.LookupKind == "miss" && row.NotFound != int64(row.Operations) {
			return fmt.Errorf("point-read %s expected all miss lookups to be absent", pointReadCell(row))
		}
		cell := pointReadCell(row)
		if _, duplicate := seen[cell]; duplicate {
			return fmt.Errorf("point-read duplicate cell %s", cell)
		}
		seen[cell] = struct{}{}
	}
	if len(seen) != len(profile.Trials)*len(profile.PointRead.SSTableCounts)*len(profile.PointRead.BloomModes)*len(profile.PointRead.LookupKinds) {
		return fmt.Errorf("point-read missing or extra cells")
	}
	return nil
}

func validateBloomRows(rows []BloomFalsePositiveRow, profile Profile) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seed, ok := profile.SeedForTrial(row.Trial)
		if !ok {
			return fmt.Errorf("bloom-fp unexpected trial %d", row.Trial)
		}
		if row.Seed != seed {
			return fmt.Errorf("bloom-fp trial %d seed %d should map to %d", row.Trial, row.Seed, seed)
		}
		if !containsInt(profile.BloomFP.InsertedKeyCounts, row.InsertedKeys) {
			return fmt.Errorf("bloom-fp trial %d unexpected inserted_keys %d", row.Trial, row.InsertedKeys)
		}
		if row.ProbeKeys != profile.BloomFP.ProbeKeys {
			return fmt.Errorf("bloom-fp trial %d inserted %d probe_keys %d, want %d", row.Trial, row.InsertedKeys, row.ProbeKeys, profile.BloomFP.ProbeKeys)
		}
		if row.MBits <= 0 {
			return fmt.Errorf("bloom-fp trial %d inserted %d has invalid m_bits %d", row.Trial, row.InsertedKeys, row.MBits)
		}
		if row.K <= 0 {
			return fmt.Errorf("bloom-fp trial %d inserted %d has invalid k %d", row.Trial, row.InsertedKeys, row.K)
		}
		if row.FalseNegatives != 0 {
			return fmt.Errorf("bloom-fp trial %d inserted %d has false_negatives %d", row.Trial, row.InsertedKeys, row.FalseNegatives)
		}
		if row.FalsePositives > uint64(row.ProbeKeys) {
			return fmt.Errorf("bloom-fp trial %d inserted %d impossible false positives", row.Trial, row.InsertedKeys)
		}
		cell := bloomCell(row.Trial, row.InsertedKeys)
		if _, duplicate := seen[cell]; duplicate {
			return fmt.Errorf("bloom-fp duplicate cell %s", cell)
		}
		seen[cell] = struct{}{}
	}
	if len(seen) != len(profile.Trials)*len(profile.BloomFP.InsertedKeyCounts) {
		return fmt.Errorf("bloom-fp missing or extra cells")
	}
	return nil
}

func validateAmplificationRows(rows []AmplificationRow, profile Profile) error {
	seen := make(map[int]struct{}, len(rows))
	for _, row := range rows {
		seed, ok := profile.SeedForTrial(row.Trial)
		if !ok {
			return fmt.Errorf("amplification unexpected trial %d", row.Trial)
		}
		if row.Seed != seed {
			return fmt.Errorf("amplification trial %d seed %d should map to %d", row.Trial, row.Seed, seed)
		}
		if row.Operations != profile.Amplification.Operations {
			return fmt.Errorf("amplification trial %d operations %d, want %d", row.Trial, row.Operations, profile.Amplification.Operations)
		}
		if row.Puts < 0 || row.Deletes < 0 || row.Puts+row.Deletes != row.Operations {
			return fmt.Errorf("amplification trial %d has split mismatch", row.Trial)
		}
		if row.Puts != row.Operations*profile.Amplification.PutsPercent/100 || row.Deletes != row.Operations*profile.Amplification.DeletesPercent/100 {
			return fmt.Errorf("amplification trial %d does not use the frozen PUT/DELETE split", row.Trial)
		}
		wantLogical := uint64(row.Puts*(profile.KeySize+profile.ValueSize) + row.Deletes*profile.KeySize)
		if row.LogicalUserWriteBytes != wantLogical {
			return fmt.Errorf("amplification trial %d logical_user_write_bytes %d, want %d", row.Trial, row.LogicalUserWriteBytes, wantLogical)
		}
		if row.LogicalUserWriteBytes == 0 {
			return fmt.Errorf("amplification trial %d logical_user_write_bytes must be positive", row.Trial)
		}
		if row.ReadOperations != profile.Amplification.Reads {
			return fmt.Errorf("amplification trial %d read_operations %d, want %d", row.Trial, row.ReadOperations, profile.Amplification.Reads)
		}
		if row.ReadOperations <= 0 {
			return fmt.Errorf("amplification trial %d has non-positive read_operations", row.Trial)
		}
		if row.SSTableReadCalls == 0 {
			return fmt.Errorf("amplification trial %d has zero sstable_read_calls", row.Trial)
		}
		if row.SSTableReadBytes == 0 {
			return fmt.Errorf("amplification trial %d has zero sstable_read_bytes", row.Trial)
		}
		if row.SSTableWriteBytes == 0 {
			return fmt.Errorf("amplification trial %d has zero sstable_write_bytes", row.Trial)
		}
		if row.ManifestWriteBytes == 0 {
			return fmt.Errorf("amplification trial %d has zero manifest_write_bytes", row.Trial)
		}
		if row.LogicalLiveBytes == 0 {
			return fmt.Errorf("amplification trial %d has zero logical_live_bytes", row.Trial)
		}
		liveUnit := uint64(profile.KeySize + profile.ValueSize)
		if row.LogicalLiveBytes%liveUnit != 0 || row.LogicalLiveBytes > uint64(profile.Amplification.Keyspace)*liveUnit {
			return fmt.Errorf("amplification trial %d has impossible logical_live_bytes", row.Trial)
		}
		if ^uint64(0)-row.SSTableWriteBytes < row.ManifestWriteBytes {
			return fmt.Errorf("amplification trial %d write byte sum overflows", row.Trial)
		}
		if row.ReferencedSSTableBytes == 0 {
			return fmt.Errorf("amplification trial %d has zero referenced_sstable_bytes", row.Trial)
		}
		if row.Compactions < 0 {
			return fmt.Errorf("amplification trial %d has negative compactions", row.Trial)
		}
		if row.ReadOperations > 0 && row.ReferencedSSTableBytes == 0 {
			return fmt.Errorf("amplification trial %d referenced bytes must be positive", row.Trial)
		}
		if _, duplicate := seen[row.Trial]; duplicate {
			return fmt.Errorf("amplification duplicate trial %d", row.Trial)
		}
		seen[row.Trial] = struct{}{}
	}
	if len(seen) != len(profile.Trials) {
		return fmt.Errorf("amplification missing or extra trials")
	}
	return nil
}

func validateCompactionPauseRows(rows []CompactionPauseRow, profile Profile) error {
	seen := make(map[int]struct{}, len(rows))
	for _, row := range rows {
		seed, ok := profile.SeedForTrial(row.Trial)
		if !ok {
			return fmt.Errorf("compaction-pause unexpected trial %d", row.Trial)
		}
		if row.Seed != seed {
			return fmt.Errorf("compaction-pause trial %d seed %d should map to %d", row.Trial, row.Seed, seed)
		}
		if row.Operations != profile.CompactionPause.Operations {
			return fmt.Errorf("compaction-pause trial %d operations %d, want %d", row.Trial, row.Operations, profile.CompactionPause.Operations)
		}
		if row.CompactionOperations <= 0 {
			return fmt.Errorf("compaction-pause trial %d has no compaction operations", row.Trial)
		}
		if row.OrdinaryOperations <= 0 {
			return fmt.Errorf("compaction-pause trial %d has no ordinary operations", row.Trial)
		}
		if row.CompactionOperations+row.OrdinaryOperations != row.Operations {
			return fmt.Errorf("compaction-pause trial %d split mismatch", row.Trial)
		}
		if row.CompactionP50NS > row.CompactionP95NS || row.CompactionP95NS > row.CompactionP99NS || row.CompactionP99NS > row.CompactionMaxNS {
			return fmt.Errorf("compaction-pause trial %d unordered compaction quantiles", row.Trial)
		}
		if row.OrdinaryP50NS > row.OrdinaryP95NS || row.OrdinaryP95NS > row.OrdinaryP99NS || row.OrdinaryP99NS > row.OrdinaryMaxNS {
			return fmt.Errorf("compaction-pause trial %d unordered ordinary quantiles", row.Trial)
		}
		if row.CompactionP50NS <= 0 || row.CompactionP95NS <= 0 || row.CompactionP99NS <= 0 || row.CompactionMaxNS <= 0 || row.OrdinaryP50NS <= 0 || row.OrdinaryP95NS <= 0 || row.OrdinaryP99NS <= 0 || row.OrdinaryMaxNS <= 0 {
			return fmt.Errorf("compaction-pause trial %d has non-positive latency", row.Trial)
		}
		if _, duplicate := seen[row.Trial]; duplicate {
			return fmt.Errorf("compaction-pause duplicate trial %d", row.Trial)
		}
		seen[row.Trial] = struct{}{}
	}
	if len(seen) != len(profile.Trials) {
		return fmt.Errorf("compaction-pause missing or extra trials")
	}
	return nil
}

func validateSkiplistRows(rows []SkiplistHeightRow, profile Profile) error {
	seen := make(map[int]struct{}, len(rows))
	for _, row := range rows {
		seed, ok := profile.SeedForTrial(row.Trial)
		if !ok {
			return fmt.Errorf("skiplist unexpected trial %d", row.Trial)
		}
		if row.Seed != seed {
			return fmt.Errorf("skiplist trial %d seed %d should map to %d", row.Trial, row.Seed, seed)
		}
		if row.Samples != profile.SkiplistHeight.Samples {
			return fmt.Errorf("skiplist trial %d samples %d, want %d", row.Trial, row.Samples, profile.SkiplistHeight.Samples)
		}
		var total uint64
		for _, count := range row.HeightCounts {
			if ^uint64(0)-total < count {
				return fmt.Errorf("skiplist trial %d histogram count overflow", row.Trial)
			}
			total += count
		}
		if total != uint64(row.Samples) {
			return fmt.Errorf("skiplist trial %d histogram sums to %d, want %d", row.Trial, total, row.Samples)
		}
		if _, duplicate := seen[row.Trial]; duplicate {
			return fmt.Errorf("skiplist duplicate trial %d", row.Trial)
		}
		seen[row.Trial] = struct{}{}
	}
	if len(seen) != len(profile.Trials) {
		return fmt.Errorf("skiplist missing or extra trials")
	}
	return nil
}

func containsInt(values []int, candidate int) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func containsBool(values []bool, candidate bool) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func containsString(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func writeCell(trial, threshold int) string {
	return fmt.Sprintf("%d:%d", trial, threshold)
}

func pointReadCell(row PointReadLatencyRow) string {
	return fmt.Sprintf("%d:%d:%t:%s", row.Trial, row.SSTableCount, row.BloomEnabled, row.LookupKind)
}

func bloomCell(trial, inserted int) string {
	return fmt.Sprintf("%d:%d", trial, inserted)
}
