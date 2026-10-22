//go:build linux

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

package thinupper

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDM stands in for dmsetup and mount: it records every script and keeps
// the mount table the scripts would have produced.
type fakeDM struct {
	mu      sync.Mutex
	scripts []string
	mounts  map[string]string // dir -> dm name
}

var (
	mountRE  = regexp.MustCompile(`create (?:--readonly )?'(thinupper-\d+)'(?s:.*)mount -t ext4 -o '[^']*' /dev/\$dev '([^']*)'`)
	umountRE = regexp.MustCompile(`umount '([^']*)'`)
)

func (f *fakeDM) Run(_ context.Context, script string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scripts = append(f.scripts, script)
	if strings.Contains(script, "-o major,minor") {
		return "253:7\n", nil
	}
	if m := mountRE.FindStringSubmatch(script); m != nil && !strings.Contains(script, "rmdir") {
		f.mounts[m[2]] = m[1]
	}
	if m := umountRE.FindStringSubmatch(script); m != nil && !strings.Contains(script, "mkfs") {
		delete(f.mounts, m[1])
	}
	return "", nil
}

func (f *fakeDM) mounted(dir string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mounts[dir], nil
}

// count returns how many recorded scripts contain s.
func (f *fakeDM) count(s string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, sc := range f.scripts {
		if strings.Contains(sc, s) {
			n++
		}
	}
	return n
}

func newTestManager(t *testing.T, goldenMax int) (*Manager, *fakeDM) {
	t.Helper()
	f := &fakeDM{mounts: map[string]string{}}
	m := &Manager{
		cfg: Config{
			Pool: "thinupper-pool", Prefix: "thinupper", VolumeBytes: 1 << 30,
			StateDir: t.TempDir(), GoldenMax: goldenMax, MountOptions: "nobarrier",
		},
		run:          f,
		mountedDev:   f.mounted,
		unmountLocal: func(string) error { return nil },
	}
	if err := m.init(t.Context()); err != nil {
		t.Fatalf("init: %v", err)
	}
	return m, f
}

func TestParseConfig(t *testing.T) {
	c, err := ParseConfig([]byte("# thin upper\npool=thinupper-pool\nvolume_bytes=4294967296\n\ngolden_max=8\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Pool != "thinupper-pool" || c.VolumeBytes != 4<<30 || c.GoldenMax != 8 || c.Prefix != "thinupper" || c.StateDir != "/var/lib/ate/rootfs-thin.d" {
		t.Errorf("ParseConfig = %+v", c)
	}
	for _, bad := range []string{"", "pool=p\nsize=1", "pool=p\nvolume_bytes=1000", "pool=p\ngolden_max=0", "pool=p\nstate_dir=rel", "pool"} {
		if _, err := ParseConfig([]byte(bad)); err == nil {
			t.Errorf("ParseConfig(%q) succeeded, want an error", bad)
		}
	}
}

func TestInitCreatesEmptyVolumeOnce(t *testing.T) {
	m, f := newTestManager(t, 4)
	if m.emptyID != 16 || m.poolDev != "253:7" {
		t.Fatalf("emptyID=%d poolDev=%q", m.emptyID, m.poolDev)
	}
	// A second worker on the node finds the empty volume.
	m2 := &Manager{cfg: m.cfg, run: f, mountedDev: f.mounted, unmountLocal: m.unmountLocal}
	if err := m2.init(t.Context()); err != nil {
		t.Fatal(err)
	}
	if m2.emptyID != m.emptyID || f.count("mkfs.ext4") != 1 {
		t.Errorf("second init: emptyID=%d, mkfs ran %d times", m2.emptyID, f.count("mkfs.ext4"))
	}
}

func TestColdBootSnapshotsEmptyVolume(t *testing.T) {
	m, f := newTestManager(t, 4)
	dir := filepath.Join(t.TempDir(), "rootfs-upper")
	if err := os.MkdirAll(filepath.Join(dir, "stale"), 0o700); err != nil {
		t.Fatal(err)
	}
	tm, err := m.ColdBoot(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if f.count("'create_snap 17 16'") != 1 {
		t.Errorf("cold boot did not snapshot the empty volume: %q", f.scripts)
	}
	if got, _ := f.mounted(dir); got != "thinupper-17" {
		t.Errorf("mounted at dir = %q, want thinupper-17", got)
	}
	if tm.Snap == 0 || tm.Mount == 0 {
		t.Errorf("timings = %+v", tm)
	}
	// A second cold boot replaces the first volume.
	if _, err := m.ColdBoot(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	if f.count("'delete 17'") != 1 {
		t.Errorf("previous volume not deleted: %q", f.scripts)
	}
}

func TestRestoreBuildsGoldenOnce(t *testing.T) {
	m, f := newTestManager(t, 4)
	fills := 0
	fill := func(root string) error {
		fills++
		if got, _ := f.mounted(root); got == "" {
			t.Errorf("fill root %q has no volume mounted", root)
		}
		return nil
	}
	var wg sync.WaitGroup
	results := make([]Timings, 8)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			results[i], err = m.Restore(context.Background(), filepath.Join(t.TempDir(), "u"), "k1", fill)
			if err != nil {
				t.Errorf("Restore: %v", err)
			}
		}()
	}
	wg.Wait()
	if fills != 1 {
		t.Errorf("golden built %d times, want 1", fills)
	}
	hits := 0
	for _, r := range results {
		if r.GoldenHit {
			hits++
		}
	}
	if hits != 7 {
		t.Errorf("%d golden hits, want 7", hits)
	}
}

func TestExportBecomesGolden(t *testing.T) {
	m, f := newTestManager(t, 4)
	dir := filepath.Join(t.TempDir(), "u")
	if _, err := m.ColdBoot(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	fr, err := m.Snapshot(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if f.count("suspend 'thinupper-17'") != 1 || f.count("'create_snap 18 17'") != 1 || f.count("resume 'thinupper-17'") != 1 {
		t.Fatalf("snapshot scripts: %q", f.scripts)
	}
	if err := m.Export(t.Context(), fr, func(root string) (string, error) { return "k2", nil }); err != nil {
		t.Fatal(err)
	}
	if f.count("create --readonly 'thinupper-18'") != 1 {
		t.Errorf("export did not mount the snapshot read-only: %q", f.scripts)
	}
	tm, err := m.Restore(t.Context(), filepath.Join(t.TempDir(), "u2"), "k2", func(string) error {
		t.Error("restore of an exported key untarred")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !tm.GoldenHit || f.count("'create_snap 19 18'") != 1 {
		t.Errorf("restore did not snapshot the exported volume: %+v %q", tm, f.scripts)
	}
}

func TestEvictLeastRecentlyUsed(t *testing.T) {
	m, f := newTestManager(t, 2)
	noop := func(string) error { return nil }
	for i, k := range []string{"a", "b", "c"} {
		if _, err := m.Restore(t.Context(), filepath.Join(t.TempDir(), "u"), k, noop); err != nil {
			t.Fatal(err)
		}
		// Distinct mtimes, oldest first.
		ts := time.Now().Add(time.Duration(i-10) * time.Minute)
		_ = os.Chtimes(m.goldenPath(k), ts, ts)
	}
	// One more build triggers eviction of "a".
	if _, err := m.Restore(t.Context(), filepath.Join(t.TempDir(), "u"), "d", noop); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]bool{"a": false, "b": false, "c": true, "d": true} {
		_, err := readID(m.goldenPath(k))
		if (err == nil) != want {
			t.Errorf("golden %q present=%v, want %v", k, err == nil, want)
		}
	}
	if f.count("'delete ") < 2 {
		t.Errorf("evicted volumes not deleted: %q", f.scripts)
	}
}

func TestTeardownLeavesPlainDirAlone(t *testing.T) {
	m, f := newTestManager(t, 2)
	before := len(f.scripts)
	if err := m.Teardown(t.Context(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if len(f.scripts) != before {
		t.Errorf("teardown of a plain dir ran %q", f.scripts[before:])
	}
}

func TestUnescapeMountinfo(t *testing.T) {
	if got := unescapeMountinfo(`/var/lib/a\040b`); got != "/var/lib/a b" {
		t.Errorf("got %q", got)
	}
}
