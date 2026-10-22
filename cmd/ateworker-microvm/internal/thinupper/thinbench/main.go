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

// BENCH ONLY. thinbench compares the plain-directory rootfs upper (tar with
// fsync on checkpoint, untar on restore, RemoveAll on teardown) against the
// dm-thin upper (internal/thinupper) on a node, with the same tarutil code
// the worker runs.
//
// Run it as root from a privileged pod with the host root at /host; it
// chroots there first. The pool must exist (see thin_pool.sh in the
// results dir). It lives under cmd/ateworker-microvm to import thinupper. Results go to stdout as JSON lines.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/thinupper"
	"go.ate.dev/substrate/internal/tarutil"
)

var (
	hostRoot  = flag.String("host-root", "/host", "chroot here first; empty to skip")
	work      = flag.String("work", "/var/lib/thinupper-bench/work", "scratch dir on the node disk")
	pool      = flag.String("pool", "thinupper-pool", "thin pool dm name")
	stateDir  = flag.String("state-dir", "/var/lib/thinupper-bench/state", "thinupper state dir")
	reps      = flag.Int("reps", 3, "repetitions per lifecycle cell")
	shapeSel  = flag.String("shapes", "", "comma list of shapes to run (e.g. small-10MiB); empty runs all")
	only      = flag.String("only", "lifecycle,fanout,amp", "comma list of sections")
	fanNs     = flag.String("fanout", "1,100,1000", "fan-out clone counts")
	par       = flag.Int("par", 16, "fan-out parallelism")
	maxFBytes = flag.Int64("max-plain-fanout-bytes", 200<<30, "skip plain fan-out cells above this many bytes")
)

type shape struct {
	name     string
	bytes    int64
	fileSize int64
}

func (s shape) String() string { return fmt.Sprintf("%s-%s", s.name, human(s.bytes)) }

func human(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%dGiB", b>>30)
	default:
		return fmt.Sprintf("%dMiB", b>>20)
	}
}

func emit(v map[string]any) {
	v["ts"] = time.Now().UTC().Format(time.RFC3339)
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		os.Exit(1)
	}
}

// genUpper writes an upper laid out as the worker does for an EROFS
// container: <dir>/app/{fs,work}, data under fs/.
func genUpper(dir string, s shape) {
	fs := filepath.Join(dir, "app", "fs")
	must(os.MkdirAll(filepath.Join(dir, "app", "work"), 0o755))
	buf := make([]byte, s.fileSize)
	n := int(s.bytes / s.fileSize)
	for i := 0; i < n; i++ {
		d := filepath.Join(fs, fmt.Sprintf("d%03d", i/256))
		if i%256 == 0 {
			must(os.MkdirAll(d, 0o755))
		}
		_, _ = rand.Read(buf)
		must(os.WriteFile(filepath.Join(d, fmt.Sprintf("f%05d", i)), buf, 0o644))
	}
}

func usedBytes(path string) int64 {
	var st syscall.Statfs_t
	must(syscall.Statfs(path, &st))
	return int64(st.Blocks-st.Bfree) * st.Bsize
}

// poolUsed returns the pool's used data bytes.
func poolUsed() int64 {
	out, err := thinupper.ExecRunner{}.Run(context.Background(), "dmsetup --noudevsync status "+*pool)
	must(err)
	// 0 <len> thin-pool <trans> <used meta>/<total meta> <used data>/<total data> ...
	f := strings.Fields(out)
	ud := strings.Split(f[5], "/")[0]
	n, _ := strconv.ParseInt(ud, 10, 64)
	tb, _ := thinupper.ExecRunner{}.Run(context.Background(), "dmsetup --noudevsync table "+*pool)
	bs, _ := strconv.ParseInt(strings.Fields(tb)[5], 10, 64) // sectors
	return n * bs * 512
}

func timeIt(fn func()) time.Duration {
	t := time.Now()
	fn()
	return time.Since(t)
}

func median(ds []time.Duration) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func extract(tarPath string) func(root string) error {
	return func(root string) error {
		dst := filepath.Join(root, "app")
		if err := os.MkdirAll(dst, 0o700); err != nil {
			return err
		}
		return tarutil.Extract(tarPath, dst)
	}
}

func main() {
	flag.Parse()
	if *hostRoot != "" {
		must(syscall.Chroot(*hostRoot))
		must(os.Chdir("/"))
		os.Setenv("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
	}
	ctx := context.Background()
	must(os.MkdirAll(*work, 0o755))
	cfg := thinupper.Config{Pool: *pool, Prefix: "thinupper", VolumeBytes: 8 << 30, StateDir: *stateDir,
		GoldenMax: 100000, MountOptions: "nobarrier,commit=30"}
	m, err := thinupper.New(ctx, cfg, thinupper.ExecRunner{})
	must(err)
	emit(map[string]any{"event": "start", "pool_used": poolUsed(), "var_used": usedBytes(*work)})

	shapes := []shape{
		{"small", 10 << 20, 4 << 10}, {"small", 100 << 20, 4 << 10}, {"small", 1 << 30, 4 << 10},
		{"large", 10 << 20, 10 << 20}, {"large", 100 << 20, 10 << 20}, {"large", 1 << 30, 10 << 20},
	}
	if *shapeSel != "" {
		var keep []shape
		for _, s := range shapes {
			if strings.Contains(","+*shapeSel+",", ","+s.String()+",") {
				keep = append(keep, s)
			}
		}
		shapes = keep
	}
	sections := map[string]bool{}
	for _, s := range strings.Split(*only, ",") {
		sections[s] = true
	}
	srcTar := map[string]string{}
	for _, s := range shapes {
		src := filepath.Join(*work, "src-"+s.String())
		if _, err := os.Stat(src); err != nil {
			d := timeIt(func() { genUpper(src, s) })
			emit(map[string]any{"event": "gen", "shape": s.String(), "secs": d.Seconds()})
		}
		tp := filepath.Join(*work, "src-"+s.String()+".tar")
		must(tarutil.CreateWithRoot(ctx, tp, filepath.Join(src, "app")))
		srcTar[s.String()] = tp
	}

	if sections["lifecycle"] {
		for _, s := range shapes {
			lifecycle(ctx, m, s, filepath.Join(*work, "src-"+s.String()), srcTar[s.String()])
		}
	}
	if sections["fanout"] {
		for _, s := range shapes {
			if s.String() != "small-10MiB" && s.String() != "large-100MiB" && s.String() != "large-1GiB" {
				continue
			}
			for _, ns := range strings.Split(*fanNs, ",") {
				n, _ := strconv.Atoi(ns)
				fanout(ctx, m, s, srcTar[s.String()], n)
			}
		}
	}
	if sections["amp"] {
		amplification(ctx, m)
	}
	emit(map[string]any{"event": "end", "pool_used": poolUsed(), "var_used": usedBytes(*work)})
}

// lifecycle times one shape's checkpoint, restore and teardown both ways.
func lifecycle(ctx context.Context, m *thinupper.Manager, s shape, src, srcTar string) {
	var fCkpt, fRest, fTear, tCold, tSnap, tExport, tGolden, tHitSnap, tHitMount, tTear []time.Duration
	var tarBytes, goldenPool int64
	for r := 0; r < *reps; r++ {
		// Plain: checkpoint = tar + fsync of the upper; restore = untar into
		// a fresh dir; teardown = RemoveAll.
		tp := filepath.Join(*work, "ckpt.tar")
		fCkpt = append(fCkpt, timeIt(func() { must(tarutil.CreateWithRoot(ctx, tp, filepath.Join(src, "app"))) }))
		st, _ := os.Stat(tp)
		tarBytes = st.Size()
		dst := filepath.Join(*work, "plain-restore")
		fRest = append(fRest, timeIt(func() {
			must(os.MkdirAll(dst, 0o700))
			must(extract(tp)(dst))
		}))
		fTear = append(fTear, timeIt(func() { must(os.RemoveAll(dst)) }))
		_ = os.Remove(tp)

		// Thin: cold boot; golden build from the tar (restore miss); restore
		// hit; checkpoint = suspend/create_snap/resume then tar from the
		// snapshot; teardown.
		cold := filepath.Join(*work, "thin-cold")
		tm, err := m.ColdBoot(ctx, cold)
		must(err)
		tCold = append(tCold, tm.Snap+tm.Mount)
		must(m.Teardown(ctx, cold))
		key := fmt.Sprintf("life%s%d%d", strings.ReplaceAll(s.String(), "-", ""), r, time.Now().UnixNano())
		p0 := poolUsed()
		dir := filepath.Join(*work, "thin-a")
		tm, err = m.Restore(ctx, dir, key, extract(srcTar))
		must(err)
		goldenPool = poolUsed() - p0
		tGolden = append(tGolden, tm.Golden)
		dir2 := filepath.Join(*work, "thin-b")
		tm, err = m.Restore(ctx, dir2, key, nil)
		must(err)
		tHitSnap = append(tHitSnap, tm.Snap)
		tHitMount = append(tHitMount, tm.Mount)
		var fr thinupper.Frozen
		tSnap = append(tSnap, timeIt(func() { fr, err = m.Snapshot(ctx, dir); must(err) }))
		ck := filepath.Join(*work, "thin-ckpt.tar")
		tExport = append(tExport, timeIt(func() {
			must(m.Export(ctx, fr, func(root string) (string, error) {
				return key + "x", tarutil.CreateWithRoot(ctx, ck, filepath.Join(root, "app"))
			}))
		}))
		_ = os.Remove(ck)
		tTear = append(tTear, timeIt(func() { must(m.Teardown(ctx, dir2)) }))
		must(m.Teardown(ctx, dir))
	}
	emit(map[string]any{"event": "lifecycle", "shape": s.String(), "reps": *reps, "tar_bytes": tarBytes,
		"plain_ckpt_tar_fsync_ms": ms(median(fCkpt)), "plain_restore_untar_ms": ms(median(fRest)), "plain_teardown_rm_ms": ms(median(fTear)),
		"thin_coldboot_ms": ms(median(tCold)), "thin_ckpt_snap_ms": ms(median(tSnap)), "thin_ckpt_export_ms": ms(median(tExport)),
		"thin_restore_miss_golden_ms": ms(median(tGolden)), "thin_restore_hit_snap_ms": ms(median(tHitSnap)),
		"thin_restore_hit_mount_ms": ms(median(tHitMount)), "thin_teardown_ms": ms(median(tTear)),
		"thin_golden_pool_bytes": goldenPool})
}

// fanout restores n clones of one golden upper, par at a time.
func fanout(ctx context.Context, m *thinupper.Manager, s shape, srcTar string, n int) {
	st, _ := os.Stat(srcTar)
	res := map[string]any{"event": "fanout", "shape": s.String(), "n": n, "par": *par, "tar_bytes": st.Size()}
	run := func(fn func(i int)) time.Duration {
		var wg sync.WaitGroup
		sem := make(chan struct{}, *par)
		return timeIt(func() {
			for i := 0; i < n; i++ {
				wg.Add(1)
				sem <- struct{}{}
				go func() { defer wg.Done(); defer func() { <-sem }(); fn(i) }()
			}
			wg.Wait()
		})
	}
	if int64(n)*st.Size() <= *maxFBytes {
		base := filepath.Join(*work, "fan-plain")
		syscall.Sync()
		v0 := usedBytes(*work)
		d := run(func(i int) {
			dst := filepath.Join(base, strconv.Itoa(i))
			must(os.MkdirAll(dst, 0o700))
			must(extract(srcTar)(dst))
		})
		syscall.Sync()
		res["plain_wall_s"] = d.Seconds()
		res["plain_disk_bytes"] = usedBytes(*work) - v0
		res["plain_teardown_s"] = timeIt(func() { must(os.RemoveAll(base)) }).Seconds()
	} else {
		res["plain_skipped"] = "exceeds max-plain-fanout-bytes"
	}
	key := fmt.Sprintf("fan%s%d%d", strings.ReplaceAll(s.String(), "-", ""), n, time.Now().UnixNano())
	p0 := poolUsed()
	base := filepath.Join(*work, "fan-thin")
	var mu sync.Mutex
	var snaps, mounts []time.Duration
	d := run(func(i int) {
		tm, err := m.Restore(ctx, filepath.Join(base, strconv.Itoa(i)), key, extract(srcTar))
		must(err)
		mu.Lock()
		snaps, mounts = append(snaps, tm.Snap), append(mounts, tm.Mount)
		if tm.Golden > 0 {
			res["thin_golden_ms"] = ms(tm.Golden)
		}
		mu.Unlock()
	})
	res["thin_wall_s"] = d.Seconds()
	res["thin_pool_bytes"] = poolUsed() - p0
	sort.Slice(snaps, func(i, j int) bool { return snaps[i] < snaps[j] })
	sort.Slice(mounts, func(i, j int) bool { return mounts[i] < mounts[j] })
	res["thin_snap_p50_ms"], res["thin_snap_p99_ms"] = ms(snaps[len(snaps)/2]), ms(snaps[len(snaps)*99/100])
	res["thin_mount_p50_ms"], res["thin_mount_p99_ms"] = ms(mounts[len(mounts)/2]), ms(mounts[len(mounts)*99/100])
	res["thin_teardown_s"] = run(func(i int) { must(m.Teardown(ctx, filepath.Join(base, strconv.Itoa(i)))) }).Seconds()
	_ = os.RemoveAll(base)
	emit(res)
}

// amplification writes known byte counts into a fresh thin volume and into a
// plain dir and compares the space each allocates.
func amplification(ctx context.Context, m *thinupper.Manager) {
	type pattern struct {
		name  string
		write func(dir string) int64
	}
	pats := []pattern{
		{"seq-1GiB", func(dir string) int64 {
			f, err := os.Create(filepath.Join(dir, "seq"))
			must(err)
			buf := make([]byte, 1<<20)
			_, _ = rand.Read(buf)
			for i := 0; i < 1024; i++ {
				_, err = f.Write(buf)
				must(err)
			}
			must(f.Close())
			return 1 << 30
		}},
		{"small-files-10000x4KiB", func(dir string) int64 {
			buf := make([]byte, 4<<10)
			for i := 0; i < 10000; i++ {
				d := filepath.Join(dir, fmt.Sprintf("d%02d", i/256))
				must(os.MkdirAll(d, 0o755))
				_, _ = rand.Read(buf)
				must(os.WriteFile(filepath.Join(d, strconv.Itoa(i)), buf, 0o644))
			}
			return 10000 * 4 << 10
		}},
		{"random-4KiB-overwrites-10000-in-1GiB-file", nil},
	}
	for _, p := range pats {
		res := map[string]any{"event": "amp", "pattern": p.name}
		key := fmt.Sprintf("amp%d", time.Now().UnixNano())
		dir := filepath.Join(*work, "amp-thin")
		plain := filepath.Join(*work, "amp-plain")
		must(os.MkdirAll(plain, 0o755))
		var written int64
		if p.write == nil {
			// The 1 GiB file comes from a golden volume, as an actor's
			// image-layer-sized upper would; the overwrites then CoW
			// shared blocks.
			src := filepath.Join(*work, "amp-src")
			must(os.MkdirAll(src, 0o755))
			pats[0].write(src)
			tp := filepath.Join(*work, "amp-src.tar")
			must(tarutil.CreateWithRoot(ctx, tp, src))
			_, err := m.Restore(ctx, dir, key, func(root string) error {
				d := filepath.Join(root, "app")
				must(os.MkdirAll(d, 0o755))
				return tarutil.Extract(tp, d)
			})
			must(err)
			must(os.MkdirAll(filepath.Join(plain, "app"), 0o755))
			must(tarutil.Extract(tp, filepath.Join(plain, "app")))
			syscall.Sync()
			overwrite := func(path string) int64 {
				f, err := os.OpenFile(path, os.O_WRONLY, 0)
				must(err)
				buf := make([]byte, 4<<10)
				rb := make([]byte, 8)
				for i := 0; i < 10000; i++ {
					_, _ = rand.Read(rb)
					off := (int64(rb[0]) | int64(rb[1])<<8 | int64(rb[2])<<16) % (1 << 18) * 4096
					_, err = f.WriteAt(buf, off)
					must(err)
				}
				must(f.Close())
				return 10000 * 4 << 10
			}
			// Thin first, synced, so the pool's loop file growth on the
			// node disk is not counted against the plain dir.
			p0 := poolUsed()
			written = overwrite(filepath.Join(dir, "app", "seq"))
			syscall.Sync()
			res["thin_pool_bytes"] = poolUsed() - p0
			v0 := usedBytes(*work)
			overwrite(filepath.Join(plain, "app", "seq"))
			syscall.Sync()
			res["plain_disk_bytes"] = usedBytes(*work) - v0
			_ = os.RemoveAll(src)
			_ = os.Remove(tp)
		} else {
			_, err := m.ColdBoot(ctx, dir)
			must(err)
			syscall.Sync()
			p0 := poolUsed()
			written = p.write(dir)
			syscall.Sync()
			res["thin_pool_bytes"] = poolUsed() - p0
			v0 := usedBytes(*work)
			p.write(plain)
			syscall.Sync()
			res["plain_disk_bytes"] = usedBytes(*work) - v0
		}
		res["bytes_written"] = written
		res["thin_amplification"] = float64(res["thin_pool_bytes"].(int64)) / float64(written)
		res["plain_amplification"] = float64(res["plain_disk_bytes"].(int64)) / float64(written)
		must(m.Teardown(ctx, dir))
		must(os.RemoveAll(plain))
		emit(res)
	}
}
