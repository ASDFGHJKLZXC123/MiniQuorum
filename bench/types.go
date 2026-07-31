package bench

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	ProfileSmoke    = "smoke-v1"
	ProfileReport   = "report-v1"
	SchemaName      = "miniquorum.lsm-benchmark"
	SchemaVersionV1 = 1
)

// Environment captures non-personal machine metadata captured with each raw file.
type Environment struct {
	GoVersion  string `json:"go_version"`
	Goos       string `json:"goos"`
	Goarch     string `json:"goarch"`
	NumCPU     int    `json:"num_cpu"`
	Gomaxprocs int    `json:"gomaxprocs"`
}

// Profile is the exact benchmark contract selected for a raw result.
type Profile struct {
	Name            string                 `json:"name"`
	Trials          []int                  `json:"trials"`
	Seeds           []int                  `json:"seeds"`
	KeySize         int                    `json:"key_size"`
	ValueSize       int                    `json:"value_size"`
	WriteThroughput WriteThroughputSpec    `json:"write_throughput"`
	PointRead       PointReadSpec          `json:"point_read_latency"`
	BloomFP         BloomFalsePositiveSpec `json:"bloom_false_positive"`
	Amplification   AmplificationSpec      `json:"amplification"`
	CompactionPause CompactionPauseSpec    `json:"compaction_pause"`
	SkiplistHeight  SkiplistHeightSpec     `json:"skiplist_height"`
}

type WriteThroughputSpec struct {
	FlushThresholdBytes []int `json:"flush_threshold_bytes"`
	Operations          int   `json:"operations"`
}

type PointReadSpec struct {
	SSTableCounts     []int    `json:"sstable_counts"`
	BloomModes        []bool   `json:"bloom_modes"`
	LookupKinds       []string `json:"lookup_kinds"`
	EntriesPerSSTable int      `json:"entries_per_sstable"`
	Operations        int      `json:"operations"`
}

type BloomFalsePositiveSpec struct {
	InsertedKeyCounts []int `json:"inserted_key_counts"`
	ProbeKeys         int   `json:"probe_keys"`
}

type AmplificationSpec struct {
	Operations     int `json:"operations"`
	Keyspace       int `json:"keyspace"`
	PutsPercent    int `json:"puts_percent"`
	DeletesPercent int `json:"deletes_percent"`
	FlushThreshold int `json:"flush_threshold_bytes"`
	Reads          int `json:"reads"`
	ReadHitPercent int `json:"read_hit_percent"`
}

type CompactionPauseSpec struct {
	Operations     int `json:"operations"`
	Keyspace       int `json:"keyspace"`
	FlushThreshold int `json:"flush_threshold_bytes"`
}

type SkiplistHeightSpec struct {
	Samples int `json:"samples"`
}

// RawResult is the typed top-level result schema for every raw JSON file.
type RawResult struct {
	Schema         string      `json:"schema"`
	SchemaVersion  int         `json:"schema_version"`
	Profile        string      `json:"profile"`
	CollectedAtUTC string      `json:"collected_at_utc"`
	Environment    Environment `json:"environment"`
	Matrix         Profile     `json:"matrix"`

	WriteThroughput    []WriteThroughputRow    `json:"write_throughput"`
	PointReadLatency   []PointReadLatencyRow   `json:"point_read_latency"`
	BloomFalsePositive []BloomFalsePositiveRow `json:"bloom_false_positive"`
	Amplification      []AmplificationRow      `json:"amplification"`
	CompactionPause    []CompactionPauseRow    `json:"compaction_pause"`
	SkiplistHeight     []SkiplistHeightRow     `json:"skiplist_height"`
}

type WriteThroughputRow struct {
	Trial               int   `json:"trial"`
	Seed                int   `json:"seed"`
	FlushThresholdBytes int   `json:"flush_threshold_bytes"`
	Operations          int   `json:"operations"`
	ElapsedNS           int64 `json:"elapsed_ns"`
}

type PointReadLatencyRow struct {
	Trial            int    `json:"trial"`
	Seed             int    `json:"seed"`
	SSTableCount     int    `json:"sstable_count"`
	BloomEnabled     bool   `json:"bloom_enabled"`
	LookupKind       string `json:"lookup_kind"`
	Operations       int    `json:"operations"`
	ElapsedNS        int64  `json:"elapsed_ns"`
	P50NS            int64  `json:"p50_ns"`
	P95NS            int64  `json:"p95_ns"`
	P99NS            int64  `json:"p99_ns"`
	MaxNS            int64  `json:"max_ns"`
	Found            int64  `json:"found"`
	NotFound         int64  `json:"not_found"`
	SSTableReadCalls uint64 `json:"sstable_read_calls"`
	SSTableReadBytes uint64 `json:"sstable_read_bytes"`
}

type BloomFalsePositiveRow struct {
	Trial          int    `json:"trial"`
	Seed           int    `json:"seed"`
	InsertedKeys   int    `json:"inserted_keys"`
	ProbeKeys      int    `json:"probe_keys"`
	MBits          uint64 `json:"m_bits"`
	K              uint64 `json:"k"`
	FalsePositives uint64 `json:"false_positives"`
	FalseNegatives uint64 `json:"false_negatives"`
}

type AmplificationRow struct {
	Trial                  int    `json:"trial"`
	Seed                   int    `json:"seed"`
	Operations             int    `json:"operations"`
	Puts                   int    `json:"puts"`
	Deletes                int    `json:"deletes"`
	LogicalUserWriteBytes  uint64 `json:"logical_user_write_bytes"`
	SSTableWriteBytes      uint64 `json:"sstable_write_bytes"`
	ManifestWriteBytes     uint64 `json:"manifest_write_bytes"`
	ReadOperations         int    `json:"read_operations"`
	SSTableReadCalls       uint64 `json:"sstable_read_calls"`
	SSTableReadBytes       uint64 `json:"sstable_read_bytes"`
	LogicalLiveBytes       uint64 `json:"logical_live_bytes"`
	ReferencedSSTableBytes uint64 `json:"referenced_sstable_bytes"`
	Compactions            int    `json:"compactions"`
}

type CompactionPauseRow struct {
	Trial                int   `json:"trial"`
	Seed                 int   `json:"seed"`
	Operations           int   `json:"operations"`
	CompactionOperations int   `json:"compaction_operations"`
	OrdinaryOperations   int   `json:"ordinary_operations"`
	CompactionP50NS      int64 `json:"compaction_p50_ns"`
	CompactionP95NS      int64 `json:"compaction_p95_ns"`
	CompactionP99NS      int64 `json:"compaction_p99_ns"`
	CompactionMaxNS      int64 `json:"compaction_max_ns"`
	OrdinaryP50NS        int64 `json:"ordinary_p50_ns"`
	OrdinaryP95NS        int64 `json:"ordinary_p95_ns"`
	OrdinaryP99NS        int64 `json:"ordinary_p99_ns"`
	OrdinaryMaxNS        int64 `json:"ordinary_max_ns"`
}

type SkiplistHeightRow struct {
	Trial        int          `json:"trial"`
	Seed         int          `json:"seed"`
	Samples      int          `json:"samples"`
	HeightCounts HeightCounts `json:"height_counts"`
}

// HeightCounts is the exact fixed-width height histogram stored in raw files.
// Its decoder rejects shortened arrays rather than silently filling a tail with
// zeroes, which keeps the schema's 16-height contract strict.
type HeightCounts [16]uint64

func (counts *HeightCounts) UnmarshalJSON(data []byte) error {
	var values []uint64
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	if len(values) != len(counts) {
		return fmt.Errorf("height_counts has %d entries, want %d", len(values), len(counts))
	}
	copy(counts[:], values)
	return nil
}

// CollectedAtUTC formats a UTC RFC3339 timestamp.
func CollectedAtUTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
