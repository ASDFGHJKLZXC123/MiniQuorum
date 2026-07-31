package bench

var (
	reportProfile = Profile{
		Name:      ProfileReport,
		Trials:    []int{1, 2, 3, 4, 5},
		Seeds:     []int{1, 2, 3, 4, 5},
		KeySize:   16,
		ValueSize: 128,
		WriteThroughput: WriteThroughputSpec{
			FlushThresholdBytes: []int{65536, 262144, 1048576, 4194304},
			Operations:          100000,
		},
		PointRead: PointReadSpec{
			SSTableCounts:     []int{1, 4, 16, 64},
			BloomModes:        []bool{true, false},
			LookupKinds:       []string{"hit", "miss"},
			EntriesPerSSTable: 4096,
			Operations:        25000,
		},
		BloomFP: BloomFalsePositiveSpec{
			InsertedKeyCounts: []int{1000, 10000, 100000},
			ProbeKeys:         100000,
		},
		Amplification: AmplificationSpec{
			Operations:     250000,
			Keyspace:       25000,
			PutsPercent:    90,
			DeletesPercent: 10,
			FlushThreshold: 65536,
			Reads:          50000,
			ReadHitPercent: 50,
		},
		CompactionPause: CompactionPauseSpec{
			Operations:     100000,
			Keyspace:       100000,
			FlushThreshold: 65536,
		},
		SkiplistHeight: SkiplistHeightSpec{
			Samples: 1000000,
		},
	}

	smokeProfile = Profile{
		Name:      ProfileSmoke,
		Trials:    []int{1},
		Seeds:     []int{1},
		KeySize:   16,
		ValueSize: 128,
		WriteThroughput: WriteThroughputSpec{
			FlushThresholdBytes: []int{65536, 4194304},
			Operations:          2000,
		},
		PointRead: PointReadSpec{
			SSTableCounts:     []int{1, 4},
			BloomModes:        []bool{true, false},
			LookupKinds:       []string{"hit", "miss"},
			EntriesPerSSTable: 256,
			Operations:        1000,
		},
		BloomFP: BloomFalsePositiveSpec{
			InsertedKeyCounts: []int{1000},
			ProbeKeys:         5000,
		},
		Amplification: AmplificationSpec{
			Operations:     5000,
			Keyspace:       500,
			PutsPercent:    90,
			DeletesPercent: 10,
			FlushThreshold: 16384,
			Reads:          1000,
			ReadHitPercent: 50,
		},
		CompactionPause: CompactionPauseSpec{
			Operations:     5000,
			Keyspace:       5000,
			FlushThreshold: 16384,
		},
		SkiplistHeight: SkiplistHeightSpec{
			Samples: 10000,
		},
	}
)

func BuiltinProfile(name string) (Profile, bool) {
	switch name {
	case ProfileSmoke:
		return cloneProfile(smokeProfile), true
	case ProfileReport:
		return cloneProfile(reportProfile), true
	default:
		return Profile{}, false
	}
}

func cloneProfile(profile Profile) Profile {
	profile.Trials = append([]int(nil), profile.Trials...)
	profile.Seeds = append([]int(nil), profile.Seeds...)
	profile.WriteThroughput.FlushThresholdBytes = append([]int(nil), profile.WriteThroughput.FlushThresholdBytes...)
	profile.PointRead.SSTableCounts = append([]int(nil), profile.PointRead.SSTableCounts...)
	profile.PointRead.BloomModes = append([]bool(nil), profile.PointRead.BloomModes...)
	profile.PointRead.LookupKinds = append([]string(nil), profile.PointRead.LookupKinds...)
	profile.BloomFP.InsertedKeyCounts = append([]int(nil), profile.BloomFP.InsertedKeyCounts...)
	return profile
}

func KnownProfiles() []string {
	return []string{ProfileSmoke, ProfileReport}
}

func ExpectedRowsByFamily(profile string) map[string]int {
	p, ok := BuiltinProfile(profile)
	if !ok {
		return nil
	}
	return map[string]int{
		"write-throughput":     len(p.Trials) * len(p.WriteThroughput.FlushThresholdBytes),
		"point-read-latency":   len(p.Trials) * len(p.PointRead.SSTableCounts) * len(p.PointRead.BloomModes) * len(p.PointRead.LookupKinds),
		"bloom-false-positive": len(p.Trials) * len(p.BloomFP.InsertedKeyCounts),
		"amplification":        len(p.Trials),
		"compaction-pause":     len(p.Trials),
		"skiplist-height":      len(p.Trials),
	}
}

func ExpectedRows(profile string) int {
	total := 0
	for _, count := range ExpectedRowsByFamily(profile) {
		total += count
	}
	return total
}

// SeedForTrial returns the paired trial seed from a built-in profile.
func (p Profile) SeedForTrial(trial int) (int, bool) {
	for index, got := range p.Trials {
		if got == trial {
			if index >= len(p.Seeds) {
				return 0, false
			}
			return p.Seeds[index], true
		}
	}
	return 0, false
}

func BuiltinProfileByName(name string) (Profile, bool) { return BuiltinProfile(name) }
