package tui

import (
	"slices"
	"testing"
)

func TestSyncManagerImportPathsIncludeLast(t *testing.T) {
	m := &SyncManager{deps: Deps{ProtoImportPaths: []string{"/r"}, ProtoIncludePaths: []string{"/i"}}}
	if got := m.protoImportPaths(); !slices.Equal(got, []string{"/r", "/i"}) {
		t.Fatalf("from deps: %v", got)
	}
	m.SetImportPaths([]string{"/r2"}, nil)
	if got := m.protoImportPaths(); !slices.Equal(got, []string{"/r2"}) {
		t.Fatalf("after set (include cleared): %v", got)
	}
}
