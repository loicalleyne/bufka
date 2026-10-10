package protoinspect

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bufbuild/protocompile"
)

// googleapis holds the google/type common types (Money, Date, LatLng, ...)
// from github.com/googleapis/googleapis, Apache 2.0. Like the google/protobuf
// well-known types protocompile bundles, they are imported by many schemas
// but are not part of the protobuf distribution, so protoc users vendor them
// and buf users add buf.build/googleapis/googleapis as a dependency.
//
//go:embed thirdparty/googleapis/google/type/*.proto
var googleapis embed.FS

const googleapisRoot = "thirdparty/googleapis"

// EmbeddedImports lists the import paths (e.g. "google/type/money.proto")
// bufka resolves without any configured path, in addition to protocompile's
// bundled google/protobuf/* well-known types.
func EmbeddedImports() []string {
	var out []string
	_ = fs.WalkDir(googleapis, googleapisRoot, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			out = append(out, strings.TrimPrefix(p, googleapisRoot+"/"))
		}
		return err
	})
	return out
}

// NewResolver returns the resolver every bufka compile uses. Imports are
// looked up, in order: in importPaths (so a local copy of any file wins, as
// with protoc -I), then in protocompile's bundled well-known types
// (google/protobuf/*), then in the embedded google/type common types.
func NewResolver(importPaths []string) protocompile.Resolver {
	return protocompile.CompositeResolver{
		protocompile.WithStandardImports(&protocompile.SourceResolver{ImportPaths: importPaths}),
		&protocompile.SourceResolver{Accessor: func(p string) (io.ReadCloser, error) {
			return googleapis.Open(path.Join(googleapisRoot, filepath.ToSlash(p)))
		}},
	}
}

var (
	embeddedDirOnce sync.Once
	embeddedDir     string
)

// EmbeddedImportDir returns a directory on disk holding the embedded
// google/type files, for compilers that only accept import paths
// (bufarrowlib); append it last so configured paths take precedence.
//
// The directory is <user cache dir>/bufka/googleapis/<content hash>
// (os.UserCacheDir: ~/Library/Caches on macOS, $XDG_CACHE_HOME or ~/.cache
// on Linux, %LocalAppData% on Windows; os.TempDir when unavailable). Because
// the name is derived from the contents, an existing file is never
// rewritten: missing files are written to a temporary name and renamed into
// place, so concurrent bufka processes never see a partial file and nothing
// is replaced while another process (Windows) has it open. It returns ""
// when the files could not be written, in which case google/type imports
// resolve only from configured paths.
func EmbeddedImportDir() string {
	embeddedDirOnce.Do(func() {
		base, err := os.UserCacheDir()
		if err != nil || base == "" {
			base = os.TempDir()
		}
		dir, err := writeEmbedded(filepath.Join(base, "bufka", "googleapis"))
		if err == nil {
			embeddedDir = dir
		}
	})
	return embeddedDir
}

// embeddedHash identifies the embedded file set.
func embeddedHash() string {
	h := sha256.New()
	for _, rel := range EmbeddedImports() {
		b, _ := googleapis.ReadFile(path.Join(googleapisRoot, rel))
		h.Write([]byte(rel))
		h.Write([]byte{0})
		h.Write(b)
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

// writeEmbedded materialises the embedded files under parent/<hash> and
// returns that directory.
func writeEmbedded(parent string) (string, error) {
	dir := filepath.Join(parent, embeddedHash())
	for _, rel := range EmbeddedImports() {
		dst := filepath.Join(dir, filepath.FromSlash(rel))
		if _, err := os.Stat(dst); err == nil {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return "", err
		}
		b, err := googleapis.ReadFile(path.Join(googleapisRoot, rel))
		if err != nil {
			return "", err
		}
		tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
		if err != nil {
			return "", err
		}
		_, werr := tmp.Write(b)
		cerr := tmp.Close()
		if werr != nil || cerr != nil {
			_ = os.Remove(tmp.Name())
			return "", errors.Join(werr, cerr)
		}
		if err := os.Rename(tmp.Name(), dst); err != nil {
			_ = os.Remove(tmp.Name())
			// Another process may have put the identical file in place
			// first (on Windows rename fails if the target exists).
			if _, statErr := os.Stat(dst); statErr != nil {
				return "", err
			}
		}
	}
	return dir, nil
}

// WithEmbeddedImportDir returns importPaths followed by EmbeddedImportDir,
// when available.
func WithEmbeddedImportDir(importPaths []string) []string {
	out := append([]string(nil), importPaths...)
	if d := EmbeddedImportDir(); d != "" {
		out = append(out, d)
	}
	return out
}
