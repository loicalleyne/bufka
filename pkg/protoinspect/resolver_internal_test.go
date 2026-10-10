package protoinspect

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestWriteEmbeddedConcurrentAndIdempotent(t *testing.T) {
	parent := t.TempDir()
	var wg sync.WaitGroup
	dirs := make([]string, 8)
	errs := make([]error, 8)
	for i := range dirs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dirs[i], errs[i] = writeEmbedded(parent)
		}()
	}
	wg.Wait()
	for i := range dirs {
		if errs[i] != nil || dirs[i] != dirs[0] {
			t.Fatalf("writer %d: dir=%q err=%v", i, dirs[i], errs[i])
		}
	}
	for _, rel := range EmbeddedImports() {
		got, err := os.ReadFile(filepath.Join(dirs[0], filepath.FromSlash(rel)))
		want, _ := googleapis.ReadFile(googleapisRoot + "/" + rel)
		if err != nil || string(got) != string(want) {
			t.Fatalf("%s: content mismatch (err=%v)", rel, err)
		}
	}
	leftovers, _ := filepath.Glob(filepath.Join(dirs[0], "google", "type", ".tmp-*"))
	if len(leftovers) > 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}
	if again, err := writeEmbedded(parent); err != nil || again != dirs[0] {
		t.Fatalf("second call: %q %v", again, err)
	}
}
