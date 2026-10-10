package driverinstall

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/columnar-tech/dbc"
	dbcconfig "github.com/columnar-tech/dbc/config"
	"github.com/go-faster/yaml"
)

const testPlatform = "macos_arm64"

// fakeRegistry serves a duckdb driver index (versions 1.5.5 and 1.5.6) and
// one tarball per version, built in memory.
type fakeRegistry struct {
	withSig    bool
	downloaded []string
}

func (f *fakeRegistry) Search(context.Context, string) ([]dbc.Driver, error) {
	var drivers []dbc.Driver
	idx := `
- name: DuckDB
  path: duckdb
  pkginfo:
    - version: 1.5.5
      packages:
        - platform: macos_arm64
    - version: 1.5.6
      packages:
        - platform: macos_arm64
`
	if err := yaml.Unmarshal([]byte(idx), &drivers); err != nil {
		return nil, err
	}
	reg := &dbc.Registry{BaseURL: &url.URL{Scheme: "https", Host: "registry.test"}}
	for i := range drivers {
		drivers[i].Registry = reg
	}
	return drivers, nil
}

func (f *fakeRegistry) Download(_ context.Context, pkg dbc.PkgInfo) (io.ReadCloser, error) {
	f.downloaded = append(f.downloaded, pkg.Version.String())
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	files := map[string]string{
		"MANIFEST": `name = "DuckDB"
version = "` + pkg.Version.String() + `"
[ADBC]
version = "1.1.0"
[Driver]
entrypoint = "duckdb_adbc_init"
[Files]
driver = "libduckdb.dylib"
signature = "libduckdb.dylib.sig"
`,
		"libduckdb.dylib": "library " + pkg.Version.String(),
	}
	if f.withSig {
		files["libduckdb.dylib.sig"] = "signature"
	}
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			return nil, err
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			return nil, err
		}
	}
	_ = tw.Close()
	_ = gz.Close()
	return io.NopCloser(&buf), nil
}

func newTestInstaller(t *testing.T, reg *fakeRegistry, verify func(lib, sig io.Reader) error) *Installer {
	t.Helper()
	return &Installer{
		Registry: reg,
		Config:   dbcconfig.Config{Level: dbcconfig.ConfigUser, Location: filepath.Join(t.TempDir(), "drivers")},
		Platform: testPlatform,
		Verify:   verify,
	}
}

func acceptSignature(lib, sig io.Reader) error {
	l, _ := io.ReadAll(lib)
	s, _ := io.ReadAll(sig)
	if !strings.HasPrefix(string(l), "library") || string(s) != "signature" {
		return errors.New("bad signature")
	}
	return nil
}

func TestInstallPinnedVersion(t *testing.T) {
	reg := &fakeRegistry{withSig: true}
	in := newTestInstaller(t, reg, acceptSignature)
	if _, ok := in.Current(); ok {
		t.Fatal("nothing installed yet")
	}
	got, err := in.Install(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.downloaded) != 1 || reg.downloaded[0] != PinnedVersion {
		t.Fatalf("must download the pinned %s, not the newest; got %v", PinnedVersion, reg.downloaded)
	}
	if got.Version != PinnedVersion {
		t.Fatalf("installed %+v", got)
	}
	if data, err := os.ReadFile(got.Library); err != nil || string(data) != "library "+PinnedVersion {
		t.Fatalf("library %s: %q %v", got.Library, data, err)
	}
	manifest, err := os.ReadFile(filepath.Join(in.Location(), "duckdb.toml"))
	if err != nil || !strings.Contains(string(manifest), got.Library) {
		t.Fatalf("manifest must point at the library: %s %v", manifest, err)
	}
	if cur, ok := in.Current(); !ok || cur.Version != PinnedVersion {
		t.Fatalf("Current = %+v %v", cur, ok)
	}

	if _, err := in.Install(context.Background(), "1.5.6"); err != nil {
		t.Fatal(err)
	}
	if cur, _ := in.Current(); cur.Version != "1.5.6" {
		t.Fatalf("explicit version: %+v", cur)
	}
	if _, err := in.Install(context.Background(), "9.9.9"); err == nil {
		t.Fatal("an unknown version must fail")
	}
}

func TestInstallRejectsBadSignature(t *testing.T) {
	in := newTestInstaller(t, &fakeRegistry{withSig: true}, func(io.Reader, io.Reader) error {
		return errors.New("not signed by columnar")
	})
	_, err := in.Install(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "signature verification failed") {
		t.Fatalf("err = %v", err)
	}
	if _, ok := in.Current(); ok {
		t.Fatal("an unverified driver must not be registered")
	}
	entries, _ := os.ReadDir(in.Location())
	for _, e := range entries {
		if e.IsDir() {
			t.Fatalf("unverified driver files left behind: %s", e.Name())
		}
	}
}

func TestInstallRejectsMissingSignature(t *testing.T) {
	in := newTestInstaller(t, &fakeRegistry{withSig: false}, acceptSignature)
	if _, err := in.Install(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("err = %v", err)
	}
	if _, ok := in.Current(); ok {
		t.Fatal("must not register a driver without a signature")
	}
}
