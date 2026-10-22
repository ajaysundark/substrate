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

// Package thinupper puts each actor's host rootfs upper directory on an ext4
// filesystem on its own dm-thin volume, so the lifecycle steps that walk the
// upper today become thin snapshots:
//
//   - cold boot: create_snap of a pre-formatted empty volume, mounted at the
//     upper dir;
//   - restore: create_snap of a golden volume holding the snapshot's upper,
//     keyed by the content hash of the snapshot's upper tars. A miss untars
//     once into a new golden volume; every later restore of the same tars
//     (golden fan-out, or a local resume of a snapshot this node took) is a
//     metadata operation;
//   - checkpoint: suspend the actor's volume (which freezes and flushes the
//     filesystem), create_snap, resume. The tar for object storage is then
//     taken from the read-only snapshot, which is kept as the golden volume
//     for that tar's hash;
//   - teardown: unmount, remove the device, delete the thin id.
//
// The guest and virtio-fs path do not change: the volume is mounted at the
// same path the plain directory used. The pool is set up out of band and named
// in MarkerFile; without the marker the worker keeps plain directories.
//
// Thin ids, the empty volume and the golden volumes are node-wide state shared
// by every worker pod on the node, kept under Config.StateDir and guarded by
// flock.
package thinupper

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// MarkerFile turns the thin upper on for every worker on the node. Its
// contents are the Config, one key=value per line.
const MarkerFile = "/var/lib/ate/rootfs-thin"

// Config is the node's thin upper setup, parsed from MarkerFile.
type Config struct {
	// Pool is the device-mapper name of the thin pool.
	Pool string
	// Prefix names the thin devices: <Prefix>-<id>.
	Prefix string
	// VolumeBytes is each volume's virtual size, which is also the cap on
	// what one actor can write to its upper.
	VolumeBytes int64
	// StateDir holds the node-wide id counter, golden index and scratch
	// mount points. It must be visible at the same path to every worker
	// pod and to whatever runs the device commands.
	StateDir string
	// GoldenMax caps the golden volumes kept; the least recently used go
	// first.
	GoldenMax int
	// MountOptions are the ext4 options for an actor's volume.
	MountOptions string
	// Helper is the socket of the BENCH ONLY privileged helper that runs
	// the device commands (helper.go); empty runs them in the worker.
	Helper string
}

// ParseConfig reads key=value lines; blank lines and # comments are skipped.
func ParseConfig(b []byte) (Config, error) {
	c := Config{
		Prefix:       "thinupper",
		VolumeBytes:  8 << 30,
		StateDir:     "/var/lib/ate/rootfs-thin.d",
		GoldenMax:    256,
		MountOptions: "nobarrier,commit=30",
	}
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return Config{}, fmt.Errorf("line %q is not key=value", line)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		var err error
		switch k {
		case "pool":
			c.Pool = v
		case "prefix":
			c.Prefix = v
		case "volume_bytes":
			c.VolumeBytes, err = strconv.ParseInt(v, 10, 64)
		case "state_dir":
			c.StateDir = v
		case "golden_max":
			c.GoldenMax, err = strconv.Atoi(v)
		case "mount_options":
			c.MountOptions = v
		case "helper":
			c.Helper = v
		default:
			return Config{}, fmt.Errorf("unknown key %q", k)
		}
		if err != nil {
			return Config{}, fmt.Errorf("key %q: %w", k, err)
		}
	}
	switch {
	case c.Pool == "":
		return Config{}, errors.New("pool is required")
	case c.VolumeBytes <= 0 || c.VolumeBytes%512 != 0:
		return Config{}, fmt.Errorf("volume_bytes must be a positive multiple of 512, got %d", c.VolumeBytes)
	case c.GoldenMax < 1:
		return Config{}, fmt.Errorf("golden_max must be at least 1, got %d", c.GoldenMax)
	case !filepath.IsAbs(c.StateDir):
		return Config{}, fmt.Errorf("state_dir must be absolute, got %q", c.StateDir)
	}
	return c, nil
}

// Runner runs a shell script with the privileges to drive device-mapper and
// mount filesystems, and returns its stdout.
type Runner interface {
	Run(ctx context.Context, script string) (string, error)
}

// ExecRunner runs scripts with the worker's own sh.
type ExecRunner struct{}

// Run implements Runner.
func (ExecRunner) Run(ctx context.Context, script string) (string, error) {
	var stderr strings.Builder
	cmd := exec.CommandContext(ctx, "sh", "-c", script)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// Timings are the thin upper's own phases of one lifecycle step. A zero
// field did not run.
type Timings struct {
	// Golden is the golden volume build on a restore miss: a snapshot of
	// the empty volume, the untar, the unmount.
	Golden time.Duration
	// Snap is the create_snap of the actor's volume (with the suspend and
	// resume around it on a checkpoint).
	Snap time.Duration
	// Mount is activating the volume and mounting it at the upper dir.
	Mount time.Duration
	// Teardown is unmounting, removing and deleting a previous volume at
	// the upper dir.
	Teardown time.Duration
	// GoldenHit reports a restore that found its golden volume.
	GoldenHit bool
}

// Frozen is a thin snapshot of an actor's upper taken at checkpoint.
type Frozen struct {
	id uint64
}

// Manager drives the thin upper on one node.
type Manager struct {
	cfg     Config
	run     Runner
	poolDev string // major:minor of the pool
	emptyID uint64

	// mountedDev returns the device-mapper name of the filesystem mounted
	// at dir in this process's mount namespace, or "" when none is.
	mountedDev func(dir string) (string, error)
	// unmountLocal drops this namespace's mount at dir, if any.
	unmountLocal func(dir string) error
}

// Load returns a Manager for the node's marker file, or nil when the marker
// is absent.
func Load(ctx context.Context, marker string, run Runner) (*Manager, error) {
	b, err := os.ReadFile(marker)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cfg, err := ParseConfig(b)
	if err != nil {
		return nil, fmt.Errorf("while parsing %s: %w", marker, err)
	}
	if cfg.Helper != "" {
		run = SocketRunner{Path: cfg.Helper}
	}
	return New(ctx, cfg, run)
}

// New returns a Manager on cfg's pool, creating the node's empty volume if no
// worker has yet.
func New(ctx context.Context, cfg Config, run Runner) (*Manager, error) {
	m := &Manager{cfg: cfg, run: run, mountedDev: mountedDMName, unmountLocal: unmountLocal}
	return m, m.init(ctx)
}

func (m *Manager) init(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Join(m.cfg.StateDir, "golden"), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(m.cfg.StateDir, "mnt"), 0o700); err != nil {
		return err
	}
	out, err := m.run.Run(ctx, "dmsetup --noudevsync info -c --noheadings -o major,minor "+q(m.cfg.Pool))
	if err != nil {
		return fmt.Errorf("while looking up thin pool %q: %w", m.cfg.Pool, err)
	}
	m.poolDev = strings.ReplaceAll(strings.TrimSpace(out), " ", "")
	if !strings.Contains(m.poolDev, ":") {
		return fmt.Errorf("unexpected device number %q for thin pool %q", m.poolDev, m.cfg.Pool)
	}
	unlock, err := m.lock("lock", syscall.LOCK_EX)
	if err != nil {
		return err
	}
	defer unlock()
	emptyFile := filepath.Join(m.cfg.StateDir, "empty")
	if id, err := readID(emptyFile); err == nil {
		m.emptyID = id
		return nil
	}
	id, err := m.nextIDLocked()
	if err != nil {
		return err
	}
	mnt := m.scratch(id)
	// lazy_itable_init=0: the inode tables are zeroed once here and shared
	// by every snapshot, rather than by ext4lazyinit on each actor's mount.
	script := fmt.Sprintf(`set -e
dmsetup --noudevsync message %s 0 'create_thin %d'
dev=$(%s)
mkfs.ext4 -q -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0,nodiscard,root_owner=0:0 /dev/$dev
mkdir -p %s
mount -t ext4 /dev/$dev %s
rmdir %s/lost+found
chmod 0700 %s
umount %s
rmdir %s
%s`, q(m.cfg.Pool), id, m.activateCmd(id, false), q(mnt), q(mnt), q(mnt), q(mnt), q(mnt), q(mnt), m.removeCmd(id))
	if _, err := m.run.Run(ctx, script); err != nil {
		return fmt.Errorf("while creating the empty volume: %w", err)
	}
	m.emptyID = id
	return writeID(emptyFile, id)
}

// ColdBoot gives dir a fresh volume snapshotted from the empty one,
// replacing whatever dir held.
func (m *Manager) ColdBoot(ctx context.Context, dir string) (Timings, error) {
	var t Timings
	var err error
	if t.Teardown, err = m.reset(ctx, dir); err != nil {
		return t, err
	}
	id, err := m.nextID()
	if err != nil {
		return t, err
	}
	start := time.Now()
	if err := m.createSnap(ctx, id, m.emptyID); err != nil {
		return t, err
	}
	t.Snap = time.Since(start)
	start = time.Now()
	if err := m.activateAndMount(ctx, id, dir, false); err != nil {
		m.discard(ctx, id)
		return t, err
	}
	t.Mount = time.Since(start)
	return t, nil
}

// Restore gives dir a volume snapshotted from the golden volume for key,
// replacing whatever dir held. When no golden volume holds key yet, one is
// built: fill gets the root of a fresh volume to write the upper into.
func (m *Manager) Restore(ctx context.Context, dir, key string, fill func(root string) error) (Timings, error) {
	var t Timings
	var err error
	if t.Teardown, err = m.reset(ctx, dir); err != nil {
		return t, err
	}
	id, err := m.nextID()
	if err != nil {
		return t, err
	}
	gid, golden, snap, hit, err := m.snapGolden(ctx, key, id, fill)
	t.Golden, t.Snap, t.GoldenHit = golden, snap, hit
	if err != nil {
		return t, err
	}
	slog.DebugContext(ctx, "Thin upper restored from golden", slog.Uint64("golden", gid), slog.Bool("hit", hit))
	start := time.Now()
	if err := m.activateAndMount(ctx, id, dir, false); err != nil {
		m.discard(ctx, id)
		return t, err
	}
	t.Mount = time.Since(start)
	return t, nil
}

// snapGolden snapshots the golden volume for key into id, building the golden
// volume first on a miss. The key's lock is held shared across the snapshot so
// an eviction cannot delete the origin under it.
func (m *Manager) snapGolden(ctx context.Context, key string, id uint64, fill func(root string) error) (gid uint64, dGolden, dSnap time.Duration, hit bool, err error) {
	if err := validKey(key); err != nil {
		return 0, 0, 0, false, err
	}
	f, err := os.OpenFile(m.goldenPath(key)+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, 0, 0, false, err
	}
	defer f.Close()
	if err := flock(f, syscall.LOCK_SH); err != nil {
		return 0, 0, 0, false, err
	}
	gid, err = readID(m.goldenPath(key))
	hit = err == nil
	if !hit {
		// Build under the exclusive lock: the first restore of a golden
		// snapshot builds, the rest of the fan-out waits and then hits.
		if err := flock(f, syscall.LOCK_EX); err != nil {
			return 0, 0, 0, false, err
		}
		if gid, err = readID(m.goldenPath(key)); err != nil {
			start := time.Now()
			if gid, err = m.buildGolden(ctx, fill); err != nil {
				return 0, time.Since(start), 0, false, err
			}
			if err := writeID(m.goldenPath(key), gid); err != nil {
				m.discard(ctx, gid)
				return 0, time.Since(start), 0, false, err
			}
			dGolden = time.Since(start)
			defer m.evict(ctx)
		}
		if err := flock(f, syscall.LOCK_SH); err != nil {
			return 0, dGolden, 0, false, err
		}
	}
	now := time.Now()
	_ = os.Chtimes(m.goldenPath(key), now, now)
	start := time.Now()
	if err := m.createSnap(ctx, id, gid); err != nil {
		return gid, dGolden, 0, hit, err
	}
	return gid, dGolden, time.Since(start), hit, nil
}

// buildGolden writes a new volume through fill and leaves it inactive.
func (m *Manager) buildGolden(ctx context.Context, fill func(root string) error) (uint64, error) {
	gid, err := m.nextID()
	if err != nil {
		return 0, err
	}
	if err := m.createSnap(ctx, gid, m.emptyID); err != nil {
		return 0, err
	}
	mnt := m.scratch(gid)
	if err := m.activateAndMount(ctx, gid, mnt, false); err != nil {
		m.discard(ctx, gid)
		return 0, err
	}
	fillErr := fill(mnt)
	if err := m.unmountAndRemove(ctx, gid, mnt); err != nil {
		return 0, errors.Join(fillErr, err)
	}
	if fillErr != nil {
		m.discard(ctx, gid)
		return 0, fillErr
	}
	return gid, nil
}

// Snapshot takes a thin snapshot of the volume mounted at dir. The caller
// must have quiesced every writer (paused the guest); the suspend freezes
// and flushes the filesystem so the snapshot is clean.
func (m *Manager) Snapshot(ctx context.Context, dir string) (Frozen, error) {
	name, err := m.mountedDev(dir)
	if err != nil {
		return Frozen{}, err
	}
	origin, ok := m.idOf(name)
	if !ok {
		return Frozen{}, fmt.Errorf("no thin volume mounted at %q", dir)
	}
	id, err := m.nextID()
	if err != nil {
		return Frozen{}, err
	}
	// The resume runs whatever the message did: a volume left suspended
	// would hang every later writer.
	script := fmt.Sprintf(`dmsetup --noudevsync suspend %s || exit 1
dmsetup --noudevsync message %s 0 'create_snap %d %d'
rc=$?
dmsetup --noudevsync resume %s || exit 1
exit $rc`, q(name), q(m.cfg.Pool), id, origin, q(name))
	if _, err := m.run.Run(ctx, script); err != nil {
		return Frozen{}, fmt.Errorf("while snapshotting thin upper %q: %w", dir, err)
	}
	return Frozen{id: id}, nil
}

// Export mounts f read-only and passes its root to fn, which returns the
// content key of what it read. f then becomes the golden volume for that key,
// so a later restore of the same tars on this node needs no untar. f is
// consumed either way.
func (m *Manager) Export(ctx context.Context, f Frozen, fn func(root string) (string, error)) error {
	mnt := m.scratch(f.id)
	if err := m.activateAndMount(ctx, f.id, mnt, true); err != nil {
		m.discard(ctx, f.id)
		return err
	}
	key, fnErr := fn(mnt)
	if err := m.unmountAndRemove(ctx, f.id, mnt); err != nil {
		return errors.Join(fnErr, err)
	}
	if fnErr != nil {
		m.discard(ctx, f.id)
		return fnErr
	}
	if err := m.register(ctx, key, f.id); err != nil {
		m.discard(ctx, f.id)
		return err
	}
	return nil
}

// Discard deletes a snapshot that will not be exported.
func (m *Manager) Discard(ctx context.Context, f Frozen) {
	m.discard(ctx, f.id)
}

// register keeps id as the golden volume for key, unless one already is.
func (m *Manager) register(ctx context.Context, key string, id uint64) error {
	if err := validKey(key); err != nil {
		return err
	}
	f, err := os.OpenFile(m.goldenPath(key)+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := flock(f, syscall.LOCK_EX); err != nil {
		return err
	}
	if _, err := readID(m.goldenPath(key)); err == nil {
		m.discard(ctx, id)
		return nil
	}
	if err := writeID(m.goldenPath(key), id); err != nil {
		return err
	}
	_ = flock(f, syscall.LOCK_UN)
	m.evict(ctx)
	return nil
}

// evict deletes the least recently used golden volumes above GoldenMax. A
// volume whose lock is held (a restore snapshotting it) is skipped.
func (m *Manager) evict(ctx context.Context) {
	dir := filepath.Join(m.cfg.StateDir, "golden")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type golden struct {
		key   string
		mtime time.Time
	}
	var gs []golden
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".lock") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		gs = append(gs, golden{e.Name(), info.ModTime()})
	}
	if len(gs) <= m.cfg.GoldenMax {
		return
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].mtime.Before(gs[j].mtime) })
	for _, g := range gs[:len(gs)-m.cfg.GoldenMax] {
		f, err := os.OpenFile(m.goldenPath(g.key)+".lock", os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			continue
		}
		if flock(f, syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			if id, err := readID(m.goldenPath(g.key)); err == nil {
				m.discard(ctx, id)
				_ = os.Remove(m.goldenPath(g.key))
			}
		}
		f.Close()
	}
}

// Teardown unmounts the volume at dir and deletes it. A dir with no thin
// volume mounted is left alone.
func (m *Manager) Teardown(ctx context.Context, dir string) error {
	name, err := m.mountedDev(dir)
	if err != nil {
		return err
	}
	id, ok := m.idOf(name)
	if !ok {
		return nil
	}
	if err := m.unmountAndRemove(ctx, id, dir); err != nil {
		return err
	}
	return m.delete(ctx, id)
}

// reset tears down any volume at dir and clears what is left of the dir.
func (m *Manager) reset(ctx context.Context, dir string) (time.Duration, error) {
	start := time.Now()
	name, err := m.mountedDev(dir)
	if err != nil {
		return 0, err
	}
	if _, ok := m.idOf(name); ok {
		if err := m.Teardown(ctx, dir); err != nil {
			return time.Since(start), err
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return time.Since(start), fmt.Errorf("while clearing rootfs upper dir %q: %w", dir, err)
	}
	if name == "" {
		return 0, nil
	}
	return time.Since(start), nil
}

func (m *Manager) createSnap(ctx context.Context, id, origin uint64) error {
	_, err := m.run.Run(ctx, fmt.Sprintf("dmsetup --noudevsync message %s 0 'create_snap %d %d'", q(m.cfg.Pool), id, origin))
	if err != nil {
		return fmt.Errorf("while snapshotting thin volume %d: %w", origin, err)
	}
	return nil
}

// activateAndMount activates id and mounts it at dir, then checks this
// process sees the mount: a runner in another mount namespace reaches us by
// propagation, and writing into the bare dir under a missing mount would put
// the upper on the node disk unnoticed.
func (m *Manager) activateAndMount(ctx context.Context, id uint64, dir string, ro bool) error {
	opts := m.cfg.MountOptions
	if ro {
		opts = "ro,noload"
	}
	script := fmt.Sprintf(`set -e
dev=$(%s)
mkdir -p %s
mount -t ext4 -o %s /dev/$dev %s`, m.activateCmd(id, ro), q(dir), q(opts), q(dir))
	if _, err := m.run.Run(ctx, script); err != nil {
		return fmt.Errorf("while mounting thin volume %d at %q: %w", id, dir, err)
	}
	name, err := m.mountedDev(dir)
	if err != nil {
		return err
	}
	if name != m.name(id) {
		return fmt.Errorf("thin volume %d mounted at %q is not visible here (found %q)", id, dir, name)
	}
	return nil
}

// unmountAndRemove drops every mount of id at dir and deactivates it,
// keeping the thin id.
func (m *Manager) unmountAndRemove(ctx context.Context, id uint64, dir string) error {
	if err := m.unmountLocal(dir); err != nil {
		return err
	}
	script := fmt.Sprintf("umount %s 2>/dev/null\n%s", q(dir), m.removeCmd(id))
	if _, err := m.run.Run(ctx, script); err != nil {
		return fmt.Errorf("while removing thin volume %d: %w", id, err)
	}
	return nil
}

func (m *Manager) delete(ctx context.Context, id uint64) error {
	if _, err := m.run.Run(ctx, fmt.Sprintf("dmsetup --noudevsync message %s 0 'delete %d'", q(m.cfg.Pool), id)); err != nil {
		return fmt.Errorf("while deleting thin volume %d: %w", id, err)
	}
	return nil
}

// discard deletes id on an error path, best effort.
func (m *Manager) discard(ctx context.Context, id uint64) {
	if err := m.delete(ctx, id); err != nil {
		slog.WarnContext(ctx, "Failed to delete thin volume", slog.Uint64("id", id), slog.Any("err", err))
	}
}

// activateCmd activates id and prints its kernel name (dm-N). The devtmpfs
// node exists as soon as the device does; the /dev/mapper link waits on udev.
func (m *Manager) activateCmd(id uint64, ro bool) string {
	flag := ""
	if ro {
		flag = "--readonly "
	}
	n := q(m.name(id))
	return fmt.Sprintf("dmsetup --noudevsync create %s%s --table '0 %d thin %s %d' && dmsetup --noudevsync info -c --noheadings -o blkdevname %s",
		flag, n, m.cfg.VolumeBytes/512, m.poolDev, id, n)
}

// removeCmd deactivates id, retrying while a just-closed opener (udev's
// probe after mkfs, a lazy unmount) still holds it.
func (m *Manager) removeCmd(id uint64) string {
	n := q(m.name(id))
	return fmt.Sprintf("for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do dmsetup --noudevsync remove %s 2>/dev/null && exit 0; sleep 0.05; done; dmsetup --noudevsync remove %s", n, n)
}

func (m *Manager) name(id uint64) string { return m.cfg.Prefix + "-" + strconv.FormatUint(id, 10) }

func (m *Manager) idOf(name string) (uint64, bool) {
	s, ok := strings.CutPrefix(name, m.cfg.Prefix+"-")
	if !ok {
		return 0, false
	}
	id, err := strconv.ParseUint(s, 10, 64)
	return id, err == nil
}

func (m *Manager) scratch(id uint64) string {
	return filepath.Join(m.cfg.StateDir, "mnt", strconv.FormatUint(id, 10))
}

func (m *Manager) goldenPath(key string) string {
	return filepath.Join(m.cfg.StateDir, "golden", key)
}

// nextID allocates a thin id from the node-wide counter.
func (m *Manager) nextID() (uint64, error) {
	unlock, err := m.lock("lock", syscall.LOCK_EX)
	if err != nil {
		return 0, err
	}
	defer unlock()
	return m.nextIDLocked()
}

func (m *Manager) nextIDLocked() (uint64, error) {
	p := filepath.Join(m.cfg.StateDir, "next-id")
	id, err := readID(p)
	if errors.Is(err, os.ErrNotExist) {
		id, err = 16, nil
	}
	if err != nil {
		return 0, err
	}
	// dm-thin ids are 24 bits.
	if id >= 1<<24 {
		return 0, fmt.Errorf("thin ids exhausted")
	}
	return id, writeID(p, id+1)
}

func (m *Manager) lock(name string, how int) (func(), error) {
	f, err := os.OpenFile(filepath.Join(m.cfg.StateDir, name), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := flock(f, how); err != nil {
		f.Close()
		return nil, err
	}
	return func() { f.Close() }, nil
}

func flock(f *os.File, how int) error {
	for {
		err := syscall.Flock(int(f.Fd()), how)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

func readID(p string) (uint64, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
}

func writeID(p string, id uint64) error {
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(id, 10)+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func validKey(key string) error {
	if key == "" || strings.ContainsAny(key, "/.") {
		return fmt.Errorf("invalid golden key %q", key)
	}
	return nil
}

// q quotes s for sh.
func q(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// mountedDMName finds the mount at dir in /proc/self/mountinfo and returns
// its device-mapper name, or "" when dir is not a mount point or not a dm
// device.
func mountedDMName(dir string) (string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	defer f.Close()
	devno := ""
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 {
			continue
		}
		if unescapeMountinfo(fields[4]) == dir {
			devno = fields[2] // the last mount wins, as it is the visible one
		}
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	if devno == "" {
		return "", nil
	}
	b, err := os.ReadFile(filepath.Join("/sys/dev/block", devno, "dm", "name"))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// unescapeMountinfo undoes the octal escapes (\040 for space) in a
// mountinfo path.
func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// unmountLocal drops this namespace's mount at dir. When the device commands
// run in another mount namespace, this process holds a propagated copy that
// the other side's unmount cannot drop while anything here still uses it.
func unmountLocal(dir string) error {
	err := syscall.Unmount(dir, 0)
	switch {
	case err == nil, errors.Is(err, syscall.EINVAL), errors.Is(err, syscall.ENOENT):
		return nil
	case errors.Is(err, syscall.EBUSY):
		return syscall.Unmount(dir, syscall.MNT_DETACH)
	default:
		return fmt.Errorf("while unmounting %q: %w", dir, err)
	}
}
