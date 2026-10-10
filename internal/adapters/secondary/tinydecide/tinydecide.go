// Package tinydecide is a SECONDARY adapter around github.com/loicalleyne/tinydecide-go.
// It implements topicsample.Advisor: a content-based second opinion used only
// to flag schema-detection ties for human review, never to pick a schema on
// its own.
package tinydecide

import (
	"fmt"
	"sync"

	tinydecidego "github.com/loicalleyne/tinydecide-go"
)

// Advisor wraps a loaded TinyDecide model. It is safe for concurrent use:
// calls into the model are serialized.
type Advisor struct {
	mu    sync.Mutex
	model *tinydecidego.TinyDecide
}

// Load reads meta.json and model.bin from dir (see tinydecide-go's Load).
func Load(dir string) (*Advisor, error) {
	m, err := tinydecidego.Load(dir)
	if err != nil {
		return nil, fmt.Errorf("tinydecide: load %s: %w", dir, err)
	}
	return &Advisor{model: m}, nil
}

// Review asks the model which of candidates the decoded JSON samples best
// match, as a single choice question per sample, keeping the pick the model
// is most confident about. It reports ok=false if candidates has fewer than
// 2 entries, every sample is empty, or every call to the model fails.
func (a *Advisor) Review(topic string, candidates []string, decodedSamples []string) (pick string, confidence float64, ok bool) {
	if a == nil || a.model == nil || len(candidates) < 2 {
		return "", 0, false
	}
	q := tinydecidego.NewChoice(fmt.Sprintf("Which kind of event is this message from topic %q?", topic), candidates)
	a.mu.Lock()
	defer a.mu.Unlock()

	best := -1
	for _, text := range decodedSamples {
		if text == "" {
			continue
		}
		r, err := a.model.Answer(text, []tinydecidego.Question{q})
		if err != nil || len(r.Answers) == 0 {
			continue
		}
		ans := r.Answers[0]
		if ans.Pick < 0 || ans.Pick >= len(candidates) {
			continue
		}
		if best < 0 || ans.Confidence > confidence {
			best, confidence, ok = ans.Pick, ans.Confidence, true
		}
	}
	if !ok {
		return "", 0, false
	}
	return candidates[best], confidence, true
}
