// Package autoscale runs bulk per-item work with a concurrency limit that
// adapts between MinConcurrency and MaxConcurrency in steps of Step, using
// throughput (items finished per second) as the signal.
package autoscale

import (
	"context"
	"sync"
	"time"
)

// Pool runs tasks with autoscaled concurrency. Use Go to start each task
// (it blocks while the pool is full) and Wait to wait for them all.
type Pool struct {
	lim *Limiter
	sc  *Scaler
	wg  sync.WaitGroup
}

// NewPool returns a Pool starting at MinConcurrency.
func NewPool() *Pool {
	return &Pool{lim: NewLimiter(MinConcurrency), sc: NewScaler(time.Now())}
}

// Go waits for a free slot, then runs f in a goroutine. It returns ctx's
// error without running f if ctx is done first.
func (p *Pool) Go(ctx context.Context, f func()) error {
	if err := p.lim.Acquire(ctx); err != nil {
		return err
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			if n, changed := p.sc.Record(time.Now()); changed {
				p.lim.SetLimit(n)
			}
			p.lim.Release()
		}()
		f()
	}()
	return nil
}

// Wait blocks until every task started with Go has returned.
func (p *Pool) Wait() { p.wg.Wait() }

// Limit reports the current concurrency limit.
func (p *Pool) Limit() int {
	p.sc.mu.Lock()
	defer p.sc.mu.Unlock()
	return p.sc.limit
}

// Concurrency bounds and tuning for Pool.
const (
	MinConcurrency = 8
	MaxConcurrency = 64
	Step           = 8
	// Tolerance is the relative throughput change treated as noise:
	// a window must beat the previous one by more than this to count as
	// better, or trail it by more than this to count as worse.
	Tolerance = 0.10
)

// Limiter is a counting semaphore whose capacity can change while in use.
// Lowering the limit never interrupts holders; it only delays new acquires.
type Limiter struct {
	mu      sync.Mutex
	limit   int
	active  int
	changed chan struct{} // closed and replaced whenever a slot may have freed
}

func NewLimiter(limit int) *Limiter {
	return &Limiter{limit: limit, changed: make(chan struct{})}
}

// Acquire blocks until a slot is free or ctx is done.
func (l *Limiter) Acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		if l.active < l.limit {
			l.active++
			l.mu.Unlock()
			return nil
		}
		ch := l.changed
		l.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (l *Limiter) Release() {
	l.mu.Lock()
	l.active--
	l.broadcastLocked()
	l.mu.Unlock()
}

func (l *Limiter) SetLimit(n int) {
	l.mu.Lock()
	l.limit = n
	l.broadcastLocked()
	l.mu.Unlock()
}

func (l *Limiter) broadcastLocked() {
	close(l.changed)
	l.changed = make(chan struct{})
}

// Scaler is a hill-climbing controller. It measures throughput over windows
// of completions (one window = as many completions as the current limit),
// keeps moving the limit in the same direction while throughput improves,
// reverses when it degrades, and holds when the change is within tolerance.
type Scaler struct {
	mu          sync.Mutex
	limit       int
	dir         int // +1 growing, -1 shrinking
	windowStart time.Time
	windowDone  int
	prevTput    float64 // throughput of the previous window, 0 = none yet
}

func NewScaler(start time.Time) *Scaler {
	return &Scaler{limit: MinConcurrency, dir: 1, windowStart: start}
}

// Record notes one completion at now and returns the limit to use next and
// whether it changed.
func (s *Scaler) Record(now time.Time) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windowDone++
	if s.windowDone < s.limit {
		return s.limit, false
	}
	elapsed := now.Sub(s.windowStart).Seconds()
	tput := float64(s.windowDone) / max(elapsed, 1e-9)
	s.windowStart, s.windowDone = now, 0

	prev := s.prevTput
	s.prevTput = tput
	switch {
	case prev == 0:
		// First window: no baseline yet, probe upward.
	case tput > prev*(1+Tolerance):
		// Improved: keep going the same way.
	case tput < prev*(1-Tolerance):
		s.dir = -s.dir // degraded: the last step hurt, undo it
	default:
		return s.limit, false // within noise: hold
	}
	next := min(max(s.limit+s.dir*Step, MinConcurrency), MaxConcurrency)
	if next == s.limit {
		// Pinned at a bound: hold; a later degradation reverses direction.
		return s.limit, false
	}
	s.limit = next
	return next, true
}
