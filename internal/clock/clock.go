// Package clock provides the real-time boundary used by server hosts.
package clock

import "time"

// Clock creates host tickers. Raft itself never receives wall-clock time.
type Clock interface {
	NewTicker(d time.Duration) Ticker
}

// Ticker is a stoppable source of real-time ticks.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock implements Clock with time.Ticker.
type RealClock struct{}

// NewRealClock returns the production wall-clock implementation.
func NewRealClock() Clock { return RealClock{} }

func (RealClock) NewTicker(d time.Duration) Ticker { return realTicker{Ticker: time.NewTicker(d)} }

type realTicker struct{ *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.Ticker.C }
