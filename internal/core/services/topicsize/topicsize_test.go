package topicsize

import "testing"

func TestStats(t *testing.T) {
	tests := []struct {
		name       string
		sizes      []int
		wantCount  int
		wantSum    int64
		wantMin    int
		wantMax    int
		wantAvg    float64
		wantStdDev float64
	}{
		{name: "empty"},
		{name: "uniform", sizes: []int{100, 100, 100}, wantCount: 3, wantSum: 300, wantMin: 100, wantMax: 100, wantAvg: 100},
		{name: "mixed", sizes: []int{10, 20, 30}, wantCount: 3, wantSum: 60, wantMin: 10, wantMax: 30, wantAvg: 20, wantStdDev: 8.16496580927726},
		{name: "single", sizes: []int{42}, wantCount: 1, wantSum: 42, wantMin: 42, wantMax: 42, wantAvg: 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Stats(tt.sizes)
			if s.Count != tt.wantCount || s.SumSize != tt.wantSum || s.MinSize != tt.wantMin || s.MaxSize != tt.wantMax {
				t.Errorf("Stats = %+v", s)
			}
			if s.AvgSize != tt.wantAvg {
				t.Errorf("AvgSize = %v, want %v", s.AvgSize, tt.wantAvg)
			}
			if !approx(s.StdDevSize, tt.wantStdDev) {
				t.Errorf("StdDevSize = %v, want %v", s.StdDevSize, tt.wantStdDev)
			}
		})
	}
}

func TestEstimate(t *testing.T) {
	tests := []struct {
		name          string
		totalMessages int64
		sizes         []int
		wantEstimate  int64
		wantLow       int64
		wantHigh      int64
	}{
		{name: "empty sample", totalMessages: 1000, sizes: nil, wantEstimate: 0},
		{name: "zero messages", totalMessages: 0, sizes: []int{100}, wantEstimate: 0},
		{name: "uniform", totalMessages: 1000, sizes: []int{250, 250}, wantEstimate: 250000, wantLow: 250000, wantHigh: 250000},
		{name: "mixed", totalMessages: 100, sizes: []int{10, 20, 30}, wantEstimate: 2000, wantLow: 1000, wantHigh: 3000},
		// Large count: 10 billion messages * 512 bytes = ~5.12 TB, must not overflow int64.
		{name: "large no overflow", totalMessages: 10_000_000_000, sizes: []int{512}, wantEstimate: 5_120_000_000_000, wantLow: 5_120_000_000_000, wantHigh: 5_120_000_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Estimate(tt.totalMessages, Stats(tt.sizes))
			if r.EstimatedBytes != tt.wantEstimate {
				t.Errorf("EstimatedBytes = %d, want %d", r.EstimatedBytes, tt.wantEstimate)
			}
			if r.LowBytes != tt.wantLow {
				t.Errorf("LowBytes = %d, want %d", r.LowBytes, tt.wantLow)
			}
			if r.HighBytes != tt.wantHigh {
				t.Errorf("HighBytes = %d, want %d", r.HighBytes, tt.wantHigh)
			}
			if r.TotalMessages != tt.totalMessages {
				t.Errorf("TotalMessages = %d, want %d", r.TotalMessages, tt.totalMessages)
			}
		})
	}
}

func approx(a, b float64) bool {
	const eps = 1e-9
	d := a - b
	return d < eps && d > -eps
}
