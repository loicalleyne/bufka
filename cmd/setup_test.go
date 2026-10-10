package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/driverinstall"
)

type fakeInstaller struct {
	cur       driverinstall.Installed
	installed bool
	calls     []string
	err       error
}

func (f *fakeInstaller) Location() string { return "/drivers" }

func (f *fakeInstaller) Current() (driverinstall.Installed, bool) { return f.cur, f.installed }

func (f *fakeInstaller) Install(_ context.Context, v string) (driverinstall.Installed, error) {
	f.calls = append(f.calls, v)
	if f.err != nil {
		return driverinstall.Installed{}, f.err
	}
	if v == "" {
		v = driverinstall.PinnedVersion
	}
	f.cur, f.installed = driverinstall.Installed{Version: v, Library: "/drivers/libduckdb.dylib"}, true
	return f.cur, nil
}

func TestRunSetup(t *testing.T) {
	ctx := context.Background()
	ok := func() error { return nil }
	missing := func() error { return database.ErrDriverMissing }

	// Already working: nothing installed.
	f := &fakeInstaller{cur: driverinstall.Installed{Version: "1.5.5", Library: "/d/lib"}, installed: true}
	var out bytes.Buffer
	if err := runSetup(ctx, &out, f, ok, "", false); err != nil || len(f.calls) != 0 || !strings.Contains(out.String(), "nothing to do") {
		t.Fatalf("idempotent: calls=%v err=%v out=%q", f.calls, err, out.String())
	}

	// Missing: installs the pinned version, then checks it loads.
	f = &fakeInstaller{}
	loaded := false
	check := func() error {
		if f.installed {
			loaded = true
			return nil
		}
		return database.ErrDriverMissing
	}
	out.Reset()
	if err := runSetup(ctx, &out, f, check, "", false); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 1 || f.calls[0] != "" || !loaded || !strings.Contains(out.String(), "Installed and verified DuckDB driver "+driverinstall.PinnedVersion) {
		t.Fatalf("install: calls=%v loaded=%v out=%q", f.calls, loaded, out.String())
	}

	// --force and an explicit other version reinstall.
	f = &fakeInstaller{cur: driverinstall.Installed{Version: "1.5.5"}, installed: true}
	_ = runSetup(ctx, &out, f, ok, "", true)
	_ = runSetup(ctx, &out, f, ok, "1.5.6", false)
	if len(f.calls) != 2 || f.calls[1] != "1.5.6" {
		t.Fatalf("force/version: %v", f.calls)
	}

	// Install failures and unloadable drivers are reported.
	f = &fakeInstaller{err: errors.New("network down")}
	if err := runSetup(ctx, &out, f, missing, "", false); err == nil || !strings.Contains(err.Error(), "network down") {
		t.Fatalf("err = %v", err)
	}
	f = &fakeInstaller{}
	if err := runSetup(ctx, &out, f, missing, "", false); err == nil || !strings.Contains(err.Error(), "cannot load it") {
		t.Fatalf("err = %v", err)
	}
}

func TestErrDriverMissingMentionsSetup(t *testing.T) {
	if !strings.Contains(database.ErrDriverMissing.Error(), "bufka setup") {
		t.Fatal(database.ErrDriverMissing)
	}
}
