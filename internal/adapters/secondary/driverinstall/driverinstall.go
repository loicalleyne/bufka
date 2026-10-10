// Package driverinstall is a SECONDARY adapter that installs the DuckDB ADBC
// driver bufka loads (through couac) from the dbc driver registry, using
// the dbc Go library instead of requiring the dbc CLI.
//
// The driver goes to the dbc user directory (macOS: ~/Library/Application
// Support/ADBC/Drivers; Linux: ~/.config/adbc/drivers), where couac's
// WithDriverLookup finds it. Unlike dbc's Client.Install, the version is
// pinned to the one bufka is tested with, and the driver's signature is
// verified against Columnar's key (as "dbc install" does) before it is kept.
//
// Like the dbc CLI, registry requests carry dbc's anonymous machine and
// install identifiers.
package driverinstall

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Masterminds/semver/v3"
	"github.com/columnar-tech/dbc"
	dbcconfig "github.com/columnar-tech/dbc/config"
)

// DriverName is the registry name of the DuckDB driver.
const DriverName = "duckdb"

// PinnedVersion is the DuckDB driver version bufka is tested with.
const PinnedVersion = "1.5.5"

// Installed describes an installed DuckDB driver.
type Installed struct {
	Version  string
	Library  string
	Manifest string
}

// Registry abstracts the dbc client calls used, for tests.
type Registry interface {
	Search(ctx context.Context, pattern string) ([]dbc.Driver, error)
	Download(ctx context.Context, pkg dbc.PkgInfo) (io.ReadCloser, error)
}

// Installer installs the DuckDB driver into one dbc config location.
type Installer struct {
	Registry Registry
	// Config is the dbc config level written to (the user level by default).
	Config dbcconfig.Config
	// Platform is the dbc platform tuple (e.g. macos_arm64).
	Platform string
	// Verify checks the driver library against its detached signature;
	// dbc.SignedByColumnar by default.
	Verify func(lib, sig io.Reader) error
}

// New returns an Installer for the current user and platform.
func New() (*Installer, error) {
	c, err := dbc.NewClient()
	if err != nil {
		return nil, fmt.Errorf("dbc client: %w", err)
	}
	return &Installer{
		Registry: c,
		Config:   c.GetConfig(dbcconfig.ConfigUser),
		Platform: dbcconfig.PlatformTuple(),
		Verify:   dbc.SignedByColumnar,
	}, nil
}

// Location is the directory drivers are installed into.
func (in *Installer) Location() string { return in.Config.Location }

// Current returns the DuckDB driver installed at this location, if any.
func (in *Installer) Current() (Installed, bool) {
	di, err := dbcconfig.GetDriver(in.Config, DriverName)
	if err != nil {
		return Installed{}, false
	}
	out := Installed{Library: di.Driver.Shared.Get(in.Platform), Manifest: filepath.Join(di.FilePath, DriverName+".toml")}
	if di.Version != nil {
		out.Version = di.Version.String()
	}
	return out, true
}

// Install downloads, unpacks and verifies version (PinnedVersion when
// empty) of the DuckDB driver for this platform, then writes its manifest.
// A driver whose signature does not verify is removed and never registered.
func (in *Installer) Install(ctx context.Context, version string) (Installed, error) {
	if version == "" {
		version = PinnedVersion
	}
	want, err := semver.NewVersion(version)
	if err != nil {
		return Installed{}, fmt.Errorf("driver version %q: %w", version, err)
	}

	drivers, err := in.Registry.Search(ctx, DriverName)
	if err != nil && len(drivers) == 0 {
		return Installed{}, fmt.Errorf("search the driver registry: %w", err)
	}
	var driver *dbc.Driver
	for i := range drivers {
		if drivers[i].Path == DriverName {
			driver = &drivers[i]
			break
		}
	}
	if driver == nil {
		return Installed{}, fmt.Errorf("the %s driver is not in the registry", DriverName)
	}
	pkg, err := driver.GetPackage(want, in.Platform, false)
	if err != nil {
		return Installed{}, fmt.Errorf("%s driver %s for %s: %w", DriverName, want, in.Platform, err)
	}

	tarball, err := in.download(ctx, pkg)
	if err != nil {
		return Installed{}, err
	}
	defer func() { _ = os.RemoveAll(filepath.Dir(tarball.Name())) }()

	manifest, err := dbcconfig.InstallDriver(in.Config, DriverName, tarball)
	if err != nil {
		return Installed{}, fmt.Errorf("unpack the driver: %w", err)
	}
	lib := manifest.Driver.Shared.Get(in.Platform)
	if err := in.verify(manifest, lib); err != nil {
		_ = os.RemoveAll(filepath.Dir(lib))
		return Installed{}, err
	}
	if err := dbcconfig.CreateManifest(in.Config, manifest.DriverInfo); err != nil {
		return Installed{}, fmt.Errorf("write the driver manifest: %w", err)
	}
	inst, _ := in.Current()
	if inst.Library == "" {
		inst = Installed{Version: want.String(), Library: lib}
	}
	return inst, nil
}

// download saves pkg to a temporary file named like the package (dbc's
// InstallDriver names the driver directory after it).
func (in *Installer) download(ctx context.Context, pkg dbc.PkgInfo) (*os.File, error) {
	if pkg.Path == nil {
		return nil, errors.New("the registry has no download URL for this driver")
	}
	body, err := in.Registry.Download(ctx, pkg)
	if err != nil {
		return nil, fmt.Errorf("download the driver: %w", err)
	}
	defer func() { _ = body.Close() }()

	dir, err := os.MkdirTemp("", "bufka-driver-*")
	if err != nil {
		return nil, err
	}
	f, err := os.Create(filepath.Join(dir, filepath.Base(pkg.Path.Path)))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	if _, err := io.Copy(f, body); err != nil {
		_ = f.Close()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("download the driver: %w", err)
	}
	return f, nil
}

// verify checks the unpacked library against its detached signature, as
// "dbc install" does. A package without a signature file is rejected.
func (in *Installer) verify(m dbcconfig.Manifest, lib string) error {
	if m.Files.Driver == "" || lib == "" {
		return errors.New("the driver package names no library to verify")
	}
	dir := filepath.Dir(lib)
	sigName := m.Files.Signature
	if sigName == "" {
		sigName = m.Files.Driver + ".sig"
	}
	l, err := os.Open(filepath.Join(dir, m.Files.Driver))
	if err != nil {
		return fmt.Errorf("open the driver library: %w", err)
	}
	defer func() { _ = l.Close() }()
	s, err := os.Open(filepath.Join(dir, sigName))
	if err != nil {
		return fmt.Errorf("the driver signature %s is missing: refusing to install", sigName)
	}
	defer func() { _ = s.Close() }()
	verify := in.Verify
	if verify == nil {
		verify = dbc.SignedByColumnar
	}
	if err := verify(l, s); err != nil {
		return fmt.Errorf("driver signature verification failed: %w", err)
	}
	return nil
}
