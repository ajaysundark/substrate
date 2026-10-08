// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package imagecache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	erofs "github.com/erofs/go-erofs"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// newTestRegistry starts an in-memory OCI registry and returns its host
// ("127.0.0.1:<port>", which the store treats as a local registry and pulls
// over plain HTTP).
func newTestRegistry(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}
	return srv, u.Host
}

func layerFromEntries(t *testing.T, entries []tarEntry) v1.Layer {
	t.Helper()
	b := buildTar(t, entries)
	l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(b)), nil
	})
	if err != nil {
		t.Fatalf("tarball.LayerFromOpener: %v", err)
	}
	return l
}

func pushImage(t *testing.T, ref string, cfg v1.Config, layers ...v1.Layer) v1.Image {
	t.Helper()
	img, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{Config: cfg})
	if err != nil {
		t.Fatalf("mutate.ConfigFile: %v", err)
	}
	img, err = mutate.AppendLayers(img, layers...)
	if err != nil {
		t.Fatalf("mutate.AppendLayers: %v", err)
	}
	tag, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatalf("name.ParseReference(%q): %v", ref, err)
	}
	if err := remote.Write(tag, img); err != nil {
		t.Fatalf("remote.Write(%q): %v", ref, err)
	}
	return img
}

func newTestStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	s, err := New(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestRetryBackoffFor pins the per-registry split: well-known communal
// registries get the patient fleet-throttling backoff, everything else —
// registries the deployment has its own quota with, including local ones —
// retries fast (their pulls sit on the Run/Restore critical path).
func TestRetryBackoffFor(t *testing.T) {
	for registry, want := range map[string]remote.Backoff{
		"gcr.io":                              dedicatedRegistryBackoff,
		"us.gcr.io":                           dedicatedRegistryBackoff,
		"us-docker.pkg.dev":                   dedicatedRegistryBackoff,
		"123.dkr.ecr.us-east-1.amazonaws.com": dedicatedRegistryBackoff,
		"harbor.example.com":                  dedicatedRegistryBackoff,
		"127.0.0.1:5000":                      dedicatedRegistryBackoff,
		"registry.k8s.io":                     sharedRegistryBackoff,
		"docker.io":                           sharedRegistryBackoff,
		"index.docker.io":                     sharedRegistryBackoff,
		"quay.io":                             sharedRegistryBackoff,
		"ghcr.io":                             sharedRegistryBackoff,
	} {
		if got := retryBackoffFor(registry); got != want {
			t.Errorf("retryBackoffFor(%q) = %+v, want %+v", registry, got, want)
		}
	}
}

// TestEnsureImage_RetriesRateLimit proves a pull survives transient 429s.
// go-containerregistry's retry transport, configured with the per-registry
// backoff picked by retryBackoffFor, covers every request including the /v2/
// auth ping — the first request a throttling registry rejects (as
// registry.k8s.io does per source IP).
func TestEnsureImage_RetriesRateLimit(t *testing.T) {
	origBackoff := dedicatedRegistryBackoff
	dedicatedRegistryBackoff = remote.Backoff{Duration: time.Millisecond, Factor: 2.0, Jitter: 0.1, Steps: 4}
	t.Cleanup(func() { dedicatedRegistryBackoff = origBackoff })

	// throttle > 0 makes the registry reject the next request with a 429;
	// it stays at zero until the test image is pushed.
	var throttle, rejections atomic.Int32
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if throttle.Add(-1) >= 0 {
			rejections.Add(1)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}

	ref := u.Host + "/test/pause:1"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "pause", typeflag: tar.TypeReg, body: "pause"},
	}))
	throttle.Store(2)

	s := newTestStore(t)
	if _, err := s.EnsureImage(context.Background(), ref); err != nil {
		t.Fatalf("EnsureImage through a throttling registry: %v", err)
	}
	if got := rejections.Load(); got != 2 {
		t.Errorf("registry rejected %d requests, want 2 (the throttling was not exercised)", got)
	}
}

func TestEnsureImage_TagPullAndDigestHit(t *testing.T) {
	srv, host := newTestRegistry(t)
	ref := host + "/test/app:latest"

	base := layerFromEntries(t, []tarEntry{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: "#!/sh\n"},
	})
	top := layerFromEntries(t, []tarEntry{
		{name: "app/", typeflag: tar.TypeDir},
		{name: "app/main", typeflag: tar.TypeReg, mode: 0o755, body: "main"},
		{name: "bin/.wh.sh", typeflag: tar.TypeReg},
		{name: "app/.wh..wh..opq", typeflag: tar.TypeReg},
	})
	pushImage(t, ref, v1.Config{Env: []string{"FOO=bar"}}, base, top)

	store := newTestStore(t)
	ctx := context.Background()

	// Tag refs must be cacheable: they resolve to a digest via one HEAD.
	img, err := store.EnsureImage(ctx, ref)
	if err != nil {
		t.Fatalf("EnsureImage(tag): %v", err)
	}
	if len(img.LayerDirs) != 2 {
		t.Fatalf("LayerDirs = %v, want 2 entries", img.LayerDirs)
	}
	if !slices.Equal(img.Config.Env, []string{"FOO=bar"}) {
		t.Errorf("Config.Env = %v, want [FOO=bar]", img.Config.Env)
	}

	// Bottom-first order: the base layer's file is in LayerDirs[0].
	if _, err := os.Stat(filepath.Join(img.LayerDirs[0], layerFSDirName, "bin/sh")); err != nil {
		t.Errorf("base layer content missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(img.LayerDirs[1], layerFSDirName, "app/main")); err != nil {
		t.Errorf("top layer content missing: %v", err)
	}

	// The top layer's whiteout state is recorded beside its tree, not in it.
	wh, err := readWhiteouts(img.LayerDirs[1])
	if err != nil {
		t.Fatalf("readWhiteouts: %v", err)
	}
	if want := []string{"bin/sh"}; !slices.Equal(wh.Whiteouts, want) {
		t.Errorf("whiteouts = %v, want %v", wh.Whiteouts, want)
	}
	if want := []string{"app"}; !slices.Equal(wh.Opaques, want) {
		t.Errorf("opaques = %v, want %v", wh.Opaques, want)
	}
	if _, err := os.Lstat(filepath.Join(img.LayerDirs[1], layerFSDirName, "bin/.wh.sh")); err == nil {
		t.Errorf("whiteout entry written into the layer tree")
	}

	// A digest-pinned ref must now hit the cache with zero registry traffic:
	// kill the registry and pull again through a fresh store on the same root
	// (which also exercises startup recovery over a populated pool).
	srv.Close()
	store2, err := New(store.root)
	if err != nil {
		t.Fatalf("New(existing root): %v", err)
	}
	img2, err := store2.EnsureImage(ctx, host+"/test/app@"+img.Digest.String())
	if err != nil {
		t.Fatalf("EnsureImage(digest, registry down): %v", err)
	}
	if !slices.Equal(img2.LayerDirs, img.LayerDirs) {
		t.Errorf("cache hit LayerDirs = %v, want %v", img2.LayerDirs, img.LayerDirs)
	}
	if !slices.Equal(img2.Config.Env, img.Config.Env) {
		t.Errorf("cache hit Config.Env = %v, want %v", img2.Config.Env, img.Config.Env)
	}
}

// Two images sharing a base layer must share its unpacked tree: the pool
// holds three layer dirs, not four.
func TestEnsureImage_SharedLayersDeduplicated(t *testing.T) {
	_, host := newTestRegistry(t)

	shared := layerFromEntries(t, []tarEntry{
		{name: "lib/", typeflag: tar.TypeDir},
		{name: "lib/base.so", typeflag: tar.TypeReg, body: "base"},
	})
	topA := layerFromEntries(t, []tarEntry{{name: "a", typeflag: tar.TypeReg, body: "a"}})
	topB := layerFromEntries(t, []tarEntry{{name: "b", typeflag: tar.TypeReg, body: "b"}})
	pushImage(t, host+"/test/a:latest", v1.Config{}, shared, topA)
	pushImage(t, host+"/test/b:latest", v1.Config{}, shared, topB)

	store := newTestStore(t)
	ctx := context.Background()

	imgA, err := store.EnsureImage(ctx, host+"/test/a:latest")
	if err != nil {
		t.Fatalf("EnsureImage(a): %v", err)
	}
	imgB, err := store.EnsureImage(ctx, host+"/test/b:latest")
	if err != nil {
		t.Fatalf("EnsureImage(b): %v", err)
	}

	if imgA.LayerDirs[0] != imgB.LayerDirs[0] {
		t.Errorf("shared base layer not deduplicated: %q vs %q", imgA.LayerDirs[0], imgB.LayerDirs[0])
	}
	entries, err := os.ReadDir(store.layersDir())
	if err != nil {
		t.Fatalf("ReadDir(layers): %v", err)
	}
	if len(entries) != 3 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("layer pool holds %d dirs (%v), want 3 (shared base stored once)", len(entries), names)
	}
}

// Startup recovery must sweep temp dirs orphaned by a crash mid-unpack, and
// reject a cache root with an unknown layout version.
func TestNew_RecoveryAndVersioning(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	orphan := filepath.Join(s.layersDir(), ".tmp-deadbeef-123")
	if err := os.MkdirAll(orphan, 0o700); err != nil {
		t.Fatalf("planting orphan: %v", err)
	}
	recOrphan := filepath.Join(s.manifestsDir(), ".deadbeef.json.tmp-456")
	if err := os.WriteFile(recOrphan, []byte("{"), 0o600); err != nil {
		t.Fatalf("planting record orphan: %v", err)
	}
	record := filepath.Join(s.manifestsDir(), "deadbeef.json")
	if err := os.WriteFile(record, []byte("{}"), 0o600); err != nil {
		t.Fatalf("planting record: %v", err)
	}
	if _, err := New(root); err != nil {
		t.Fatalf("New(recovery): %v", err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphaned temp dir survived recovery")
	}
	if _, err := os.Stat(recOrphan); !os.IsNotExist(err) {
		t.Errorf("orphaned manifest temp file survived recovery")
	}
	if _, err := os.Stat(record); err != nil {
		t.Errorf("completed manifest record swept by recovery: %v", err)
	}

	if err := os.WriteFile(filepath.Join(root, versionFileName), []byte("99\n"), 0o600); err != nil {
		t.Fatalf("writing version marker: %v", err)
	}
	if _, err := New(root); err == nil {
		t.Errorf("New accepted an unknown layout version")
	}
}

func TestIsLocalRegistry(t *testing.T) {
	tests := []struct {
		ref  string
		want bool
	}{
		{ref: "localhost/foo", want: true},
		{ref: "localhost:5001/foo", want: true},
		{ref: "127.0.0.1/foo", want: true},
		{ref: "127.0.0.1:5001/foo", want: true},
		{ref: "127.0.0.2/foo", want: true},
		{ref: "127.0.0.2:8080/foo", want: true},
		{ref: "kind-registry/foo", want: false},
		{ref: "kind-registry:5000/foo", want: false},
		{ref: "my-registry.local/foo", want: false},
		{ref: "my-registry.local:8080/foo", want: false},
		{ref: "gcr.io/foo", want: false},
		{ref: "example.com/foo", want: false},
		{ref: "foo", want: false},
		{ref: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := isLocalRegistry(tt.ref)
			if got != tt.want {
				t.Errorf("isLocalRegistry(%q) = %v; want %v", tt.ref, got, tt.want)
			}
		})
	}
}

func TestRewriteLocalRegistry(t *testing.T) {
	s := &Store{localhostRegistryReplacement: "kind-registry:5000"}

	tests := []struct {
		ref  string
		want string
	}{
		{ref: "localhost/foo", want: "kind-registry:5000/foo"},
		{ref: "localhost:5001/foo", want: "kind-registry:5000/foo"},
		{ref: "localhost:8080/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.1/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.1:3000/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.2/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.2:8080/foo", want: "kind-registry:5000/foo"},
		{ref: "kind-registry/foo", want: "kind-registry/foo"},
		{ref: "kind-registry:5000/foo", want: "kind-registry:5000/foo"},
		{ref: "my-registry.local/foo", want: "my-registry.local/foo"},
		{ref: "gcr.io/foo", want: "gcr.io/foo"},
		{ref: "foo", want: "foo"},
		{ref: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := s.rewriteLocalRegistry(tt.ref)
			if got != tt.want {
				t.Errorf("rewriteLocalRegistry(%q) = %q; want %q", tt.ref, got, tt.want)
			}
		})
	}
}

// newBlobCountingRegistry is newTestRegistry with a counter of blob GETs, to
// prove what a pull did and did not download.
func newBlobCountingRegistry(t *testing.T) (host string, blobs *atomic.Int32) {
	t.Helper()
	blobs = new(atomic.Int32)
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
			blobs.Add(1)
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}
	return u.Host, blobs
}

func TestEnsureImage_EROFSLayers(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/erofs:latest"
	base := layerFromEntries(t, []tarEntry{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: strings.Repeat("s", 9000)},
	})
	top := layerFromEntries(t, []tarEntry{
		{name: "app/", typeflag: tar.TypeDir},
		{name: "app/main", typeflag: tar.TypeReg, mode: 0o755, body: "main"},
		{name: "bin/.wh.sh", typeflag: tar.TypeReg},
	})
	pushImage(t, ref, v1.Config{}, base, top)
	ctx := context.Background()

	// Without the option nothing changes: no image, no flag.
	plain := newTestStore(t)
	img, err := plain.EnsureImage(ctx, ref)
	if err != nil {
		t.Fatalf("EnsureImage (plain): %v", err)
	}
	if img.EROFS {
		t.Error("plain store reported EROFS")
	}
	if _, err := os.Stat(LayerEROFSPath(img.LayerDirs[0])); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plain store wrote %s: %v", layerEROFSFileName, err)
	}

	store := newTestStore(t)
	img, err = store.EnsureImageAs(ctx, ref, Form{EROFS: true})
	if err != nil {
		t.Fatalf("EnsureImageAs: %v", err)
	}
	if !img.EROFS {
		t.Error("EROFS = false, want true")
	}
	for i, dir := range img.LayerDirs {
		f, err := os.Open(LayerEROFSPath(dir))
		if err != nil {
			t.Fatalf("layer %d: %v", i, err)
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size()%erofsImageAlign != 0 {
			t.Errorf("layer %d image size %d not a multiple of %d", i, fi.Size(), erofsImageAlign)
		}
		fsys, err := erofs.Open(f)
		if err != nil {
			t.Fatalf("layer %d: erofs.Open: %v", i, err)
		}
		switch i {
		case 0:
			if got, err := fs.ReadFile(fsys, "bin/sh"); err != nil || len(got) != 9000 {
				t.Errorf("layer 0 bin/sh: len=%d err=%v", len(got), err)
			}
		case 1:
			if got, err := fs.ReadFile(fsys, "app/main"); err != nil || string(got) != "main" {
				t.Errorf("layer 1 app/main = %q, %v", got, err)
			}
			if fi, err := fs.Lstat(fsys, "bin/sh"); err != nil || fi.Mode()&fs.ModeCharDevice == 0 {
				t.Errorf("layer 1 bin/sh whiteout: %v, %v", fi, err)
			}
		}
		if d, err := LayerEROFSDigest(dir); err != nil || !strings.HasPrefix(d, "sha256:") {
			t.Errorf("layer %d digest = %q, %v", i, d, err)
		}
		// Only layer.erofs is written for an EROFS consumer.
		n, ok, err := recordedLayerSize(dir)
		if err != nil || !ok {
			t.Fatalf("layer %d size: ok=%v err=%v", i, ok, err)
		}
		if n != fi.Size() {
			t.Errorf("layer %d recorded size %d, want image size %d", i, n, fi.Size())
		}
		if _, err := os.Stat(filepath.Join(dir, layerFSDirName)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("layer %d fs/ should not be unpacked for an EROFS consumer: %v", i, err)
		}
	}

	// A fresh store over the same pool hits without touching the registry.
	store2, err := New(store.root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	img2, err := store2.EnsureImageAs(ctx, host+"/test/erofs@"+img.Digest.String(), Form{EROFS: true})
	if err != nil {
		t.Fatalf("EnsureImageAs (hit): %v", err)
	}
	if !img2.EROFS || !slices.Equal(img2.LayerDirs, img.LayerDirs) {
		t.Errorf("hit = %+v, want EROFS with the same layer dirs", img2)
	}

	// Over the cap the image is served as fs/, and the layers get a tree
	// beside their images.
	img3, err := store2.EnsureImageAs(ctx, ref, Form{EROFS: true, MaxEROFSLayers: 1})
	if err != nil {
		t.Fatalf("EnsureImageAs (capped): %v", err)
	}
	if img3.EROFS || !slices.Equal(img3.LayerDirs, img.LayerDirs) {
		t.Errorf("capped = %+v, want fs/ over the same layer dirs", img3)
	}
	for i, dir := range img3.LayerDirs {
		if !layerComplete(dir, false) || !layerComplete(dir, true) {
			t.Errorf("layer %d should hold both forms after the capped pull", i)
		}
	}
}

// A pool filled by fs/ consumers has trees but no images. An EROFS consumer
// must not be served those layers as complete; the missing image is built
// from a re-downloaded blob while the tree stays in place.
func TestEnsureImage_EROFSBackfill(t *testing.T) {
	host, blobs := newBlobCountingRegistry(t)
	ref := host + "/test/backfill:latest"
	layer := layerFromEntries(t, []tarEntry{
		{name: "etc/", typeflag: tar.TypeDir},
		{name: "etc/os-release", typeflag: tar.TypeReg, body: "ID=test\n"},
	})
	pushImage(t, ref, v1.Config{}, layer)
	ctx := context.Background()

	plain := newTestStore(t)
	img, err := plain.EnsureImage(ctx, ref)
	if err != nil {
		t.Fatalf("EnsureImage (plain): %v", err)
	}
	dir := img.LayerDirs[0]
	// A temp image from an interrupted earlier attempt must be swept.
	stale := filepath.Join(dir, "."+layerEROFSFileName+".tmp-stale")
	if err := os.WriteFile(stale, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(dir, layerFSDirName, "etc/os-release"))
	if err != nil {
		t.Fatal(err)
	}

	blobs.Store(0)
	store, err := New(plain.root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, err := store.cachedImage(img.Digest, Form{EROFS: true}); err != nil || got != nil {
		t.Fatalf("cachedImage before backfill = %v, %v; want miss", got, err)
	}
	img2, err := store.EnsureImageAs(ctx, ref, Form{EROFS: true})
	if err != nil {
		t.Fatalf("EnsureImageAs (backfill): %v", err)
	}
	if !img2.EROFS || img2.LayerDirs[0] != dir {
		t.Fatalf("backfill result = %+v, want EROFS over %s", img2, dir)
	}
	if blobs.Load() == 0 {
		t.Error("backfill fetched no blobs; the image cannot have come from the tar")
	}
	f, err := os.Open(LayerEROFSPath(dir))
	if err != nil {
		t.Fatalf("image after backfill: %v", err)
	}
	defer f.Close()
	fsys, err := erofs.Open(f)
	if err != nil {
		t.Fatalf("erofs.Open: %v", err)
	}
	if got, err := fs.ReadFile(fsys, "etc/os-release"); err != nil || string(got) != "ID=test\n" {
		t.Errorf("etc/os-release = %q, %v", got, err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stale temp image not swept: %v", err)
	}
	if d, err := LayerEROFSDigest(dir); err != nil || !strings.HasPrefix(d, "sha256:") {
		t.Errorf("digest after backfill = %q, %v", d, err)
	}
	after, err := os.Stat(filepath.Join(dir, layerFSDirName, "etc/os-release"))
	if err != nil || !after.ModTime().Equal(before.ModTime()) {
		t.Errorf("fs/ tree was rewritten by the backfill: %v, %v", after, err)
	}

	// Second pull is a plain hit: no further downloads.
	n := blobs.Load()
	if _, err := store.EnsureImageAs(ctx, ref, Form{EROFS: true}); err != nil {
		t.Fatalf("EnsureImageAs (hit): %v", err)
	}
	if blobs.Load() != n {
		t.Errorf("hit after backfill fetched %d more blobs", blobs.Load()-n)
	}
}

// The reverse: a pool filled by an EROFS consumer has images but no trees.
// An fs/ consumer gets the tree unpacked beside the image from a
// re-downloaded blob, with whiteouts.json rewritten to carry the whiteouts
// a host overlay materializes (the image build records only implicit
// directories).
func TestEnsureImage_FSBackfill(t *testing.T) {
	host, blobs := newBlobCountingRegistry(t)
	ref := host + "/test/fsbackfill:latest"
	base := layerFromEntries(t, []tarEntry{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/sh", typeflag: tar.TypeReg, body: "sh"},
		{name: "bin/rm", typeflag: tar.TypeReg, body: "rm"},
	})
	top := layerFromEntries(t, []tarEntry{
		{name: "bin/.wh.rm", typeflag: tar.TypeReg},
		{name: "app/main", typeflag: tar.TypeReg, body: "main"},
	})
	pushImage(t, ref, v1.Config{}, base, top)
	ctx := context.Background()

	store := newTestStore(t)
	img, err := store.EnsureImageAs(ctx, ref, Form{EROFS: true})
	if err != nil {
		t.Fatalf("EnsureImageAs: %v", err)
	}
	topDir := img.LayerDirs[1]
	if wh, err := readWhiteouts(topDir); err != nil || len(wh.Whiteouts) != 0 {
		t.Fatalf("image build recorded whiteouts %v, %v; want none (they live in the image)", wh, err)
	}
	stale := filepath.Join(topDir, "."+layerFSDirName+".tmp-stale")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	imgBefore, err := os.Stat(LayerEROFSPath(topDir))
	if err != nil {
		t.Fatal(err)
	}

	blobs.Store(0)
	if got, err := store.cachedImage(img.Digest, Form{}); err != nil || got != nil {
		t.Fatalf("cachedImage before backfill = %v, %v; want miss", got, err)
	}
	img2, err := store.EnsureImage(ctx, ref)
	if err != nil {
		t.Fatalf("EnsureImage (fs backfill): %v", err)
	}
	if img2.EROFS || !slices.Equal(img2.LayerDirs, img.LayerDirs) {
		t.Fatalf("backfill result = %+v, want fs/ over the same layer dirs", img2)
	}
	if blobs.Load() == 0 {
		t.Error("backfill fetched no blobs; the tree cannot have come from the image")
	}
	if got, err := os.ReadFile(filepath.Join(topDir, layerFSDirName, "app/main")); err != nil || string(got) != "main" {
		t.Errorf("app/main = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(topDir, layerFSDirName, "bin/.wh.rm")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("whiteout entry written into fs/: %v", err)
	}
	wh, err := readWhiteouts(topDir)
	if err != nil || !slices.Equal(wh.Whiteouts, []string{"bin/rm"}) {
		t.Errorf("whiteouts after backfill = %+v, %v; want bin/rm", wh, err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stale temp tree not swept: %v", err)
	}
	imgAfter, err := os.Stat(LayerEROFSPath(topDir))
	if err != nil || !imgAfter.ModTime().Equal(imgBefore.ModTime()) {
		t.Errorf("layer.erofs was rewritten by the backfill: %v, %v", imgAfter, err)
	}

	// Both consumers now hit without downloads.
	n := blobs.Load()
	for _, form := range []Form{{}, {EROFS: true}} {
		if _, err := store.EnsureImageAs(ctx, ref, form); err != nil {
			t.Fatalf("EnsureImageAs(%+v) (hit): %v", form, err)
		}
	}
	if blobs.Load() != n {
		t.Errorf("hits after backfill fetched %d more blobs", blobs.Load()-n)
	}
}
