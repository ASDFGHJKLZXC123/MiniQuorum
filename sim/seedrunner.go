package sim

import "fmt"

// RunSeeds builds and runs one Sim per seed, deriving every random draw for
// that run solely from the seed (Config.Seed). scenario, if non-nil, is
// called once per fresh Sim before Run so callers can install crashes or
// partitions; it runs before any invariant has fired. RunSeeds stops and
// returns the first invariant violation, wrapped with its seed.
func RunSeeds(cfg Config, seeds []int64, until VirtualTime, scenario func(*Sim)) error {
	for _, seed := range seeds {
		runCfg := cfg
		runCfg.Seed = seed
		s, err := NewSim(runCfg)
		if err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
		s.RegisterInvariant(SingleLeaderPerTerm)
		if scenario != nil {
			scenario(s)
		}
		if err := s.Run(until); err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
	}
	return nil
}
