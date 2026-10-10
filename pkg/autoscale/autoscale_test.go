package autoscale

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runWindow feeds one full window of completions spread over d and returns
// the scaler's resulting limit.
func runWindow(s *Scaler, now *time.Time, d time.Duration) int {
	n := s.limit
	var limit int
	for i := 0; i < n; i++ {
		*now = now.Add(d / time.Duration(n))
		limit, _ = s.Record(*now)
	}
	return limit
}

func TestScalerGrowsWhileThroughputImproves(t *testing.T) {
	now := time.Unix(0, 0)
	s := NewScaler(now)
	// Each window finishes in 1s regardless of size: throughput scales
	// linearly with the limit, so the scaler should climb to the cap.
	for i := 0; i < 20; i++ {
		runWindow(s, &now, time.Second)
	}
	if s.limit != MaxConcurrency {
		t.Fatalf("limit = %d, want %d", s.limit, MaxConcurrency)
	}
}

func TestScalerHoldsOnPlateau(t *testing.T) {
	now := time.Unix(0, 0)
	s := NewScaler(now)
	// Throughput is flat at 8 topics/s whatever the limit.
	for i := 0; i < 10; i++ {
		runWindow(s, &now, time.Duration(s.limit)*time.Second/8)
	}
	if s.limit != 16 {
		t.Fatalf("limit = %d, want 16 (one probe step, then hold)", s.limit)
	}
}

func TestScalerBacksOffWhenThroughputDrops(t *testing.T) {
	now := time.Unix(0, 0)
	s := NewScaler(now)
	// Throughput peaks at limit 24 and degrades sharply beyond it.
	tput := func(limit int) float64 {
		if limit <= 24 {
			return float64(limit)
		}
		return 24 - float64(limit-24)*2
	}
	seen := map[int]bool{}
	for i := 0; i < 30; i++ {
		l := s.limit
		seen[l] = true
		runWindow(s, &now, time.Duration(float64(l)/tput(l)*float64(time.Second)))
	}
	if seen[48] {
		t.Fatalf("scaler went too far past the peak: visited %v", seen)
	}
	if s.limit < 16 || s.limit > 32 {
		t.Fatalf("limit = %d, want near the 24 peak", s.limit)
	}
}

func TestScalerStaysInBounds(t *testing.T) {
	now := time.Unix(0, 0)
	s := NewScaler(now)
	// Ever-worsening throughput must never push below the minimum.
	d := time.Second
	for i := 0; i < 30; i++ {
		runWindow(s, &now, d)
		d *= 2
		if s.limit < MinConcurrency || s.limit > MaxConcurrency {
			t.Fatalf("limit %d out of bounds", s.limit)
		}
	}
}

func TestLimiterRespectsChangingLimit(t *testing.T) {
	l := NewLimiter(2)
	var active, peak atomic.Int32
	var wg sync.WaitGroup
	work := func() {
		defer wg.Done()
		if err := l.Acquire(context.Background()); err != nil {
			t.Error(err)
			return
		}
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		active.Add(-1)
		l.Release()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go work()
	}
	wg.Wait()
	if peak.Load() > 2 {
		t.Fatalf("peak %d exceeded limit 2", peak.Load())
	}

	l.SetLimit(5)
	peak.Store(0)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go work()
	}
	wg.Wait()
	if p := peak.Load(); p > 5 || p < 3 {
		t.Fatalf("peak %d, want 3..5 after raising the limit to 5", p)
	}
}

func TestLimiterAcquireCanceled(t *testing.T) {
	l := NewLimiter(1)
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := l.Acquire(ctx); err == nil {
		t.Fatal("acquire succeeded past a full limiter")
	}
}

func TestPoolRunsAllAndStopsOnCancel(t *testing.T) {
	p := NewPool()
	var ran atomic.Int32
	for i := 0; i < 100; i++ {
		if err := p.Go(context.Background(), func() { ran.Add(1) }); err != nil {
			t.Fatal(err)
		}
	}
	p.Wait()
	if ran.Load() != 100 {
		t.Fatalf("ran %d tasks, want 100", ran.Load())
	}
	if l := p.Limit(); l < MinConcurrency || l > MaxConcurrency {
		t.Fatalf("limit %d out of bounds", l)
	}

	ctx, cancel := context.WithCancel(context.Background())
	block := make(chan struct{})
	p2 := NewPool()
	for i := 0; i < MinConcurrency; i++ {
		_ = p2.Go(ctx, func() { <-block })
	}
	cancel()
	if err := p2.Go(ctx, func() { t.Error("ran past a full, canceled pool") }); err == nil {
		t.Fatal("Go succeeded on a full pool after cancel")
	}
	close(block)
	p2.Wait()
}
