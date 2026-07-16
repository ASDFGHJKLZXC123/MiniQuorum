package clock

import (
	"testing"
	"time"
)

func TestRealClockTickerFiresAndStops(t *testing.T) {
	ticker := NewRealClock().NewTicker(15 * time.Millisecond)
	for i := 0; i < 3; i++ {
		select {
		case <-ticker.C():
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("ticker did not fire for interval %d", i)
		}
	}
	ticker.Stop()
	// A tick may already be buffered when Stop is called; drain it before
	// verifying that no new real-time ticks arrive.
	select {
	case <-ticker.C():
	default:
	}
	select {
	case <-ticker.C():
		t.Fatal("ticker fired after Stop")
	case <-time.After(60 * time.Millisecond):
	}
}
