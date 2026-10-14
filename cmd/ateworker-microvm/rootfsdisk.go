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

package main

// Upper disks: the guest overlay upper of a container on EROFS layer images
// (erofs.go) on a per-container virtio-blk ext4 disk instead of the virtio-fs
// share. Enabled when the SandboxConfig lists the optional mke2fs asset
// (e2fsprogs 1.47 or later, statically linked: the worker image is
// distroless).
//
// On the share, a checkpoint archives the upper file by file while the guest
// is paused (rootfs-rw-<cid>.tar) and a restore extracts it again; under load
// both queue on the node disk and dominate suspend and resume. On a disk the
// upper is one file:
//
//   - Cold boot: the seeded host upper (<cid>/fs from prepareEROFSUppers plus
//     resolv.conf, and an empty <cid>/work) is packed into a sparse ext4 image,
//     <upperBase>/<cid>.upper.ext4, by mke2fs -d. The images are the VM's only
//     virtio-blk disks (the guest image is on virtio-pmem), so the i-th is
//     /dev/vd<a+i>. The agent mounts it at /run/kata-containers/upper/<cid> and
//     the overlay takes upperdir and workdir from there.
//   - Checkpoint: the image is hard-linked into the checkpoint dir as
//     rootfs-disk-<cid>.ext4 in place of the tar. A MEMORY
//     snapshot holds the guest's dirty ext4 pages, which match the image at
//     the pause. A ROOTFS snapshot boots a fresh guest on the image, so the
//     guest unmounts it first (flushUpperDisks).
//   - Restore: the image is copied out of the restore dir, keeping holes, and
//     the snapshot config's disk paths are pointed at this actor's upper dir.
//
// Each disk takes one more PCI slot on segment 0 (see maxEROFSLayers).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
)

const (
	// upperDiskSize is the sparse image size; only written blocks take disk.
	upperDiskSize = "4G"
	// upperDiskSuffix names the host image beside the container's upper dir.
	upperDiskSuffix = ".upper.ext4"
	// rootfsDiskPrefix names the image in a snapshot.
	rootfsDiskPrefix = "rootfs-disk-"
)

// upperDiskPath is the host image backing cid's guest upper.
func upperDiskPath(upperBase, cid string) string {
	return filepath.Join(upperBase, cid+upperDiskSuffix)
}

// rootfsDiskFile is the snapshot file holding the upper image of container
// name.
func rootfsDiskFile(name string) (string, error) {
	if name == "" || name == "." || strings.Contains(name, "/") || !filepath.IsLocal(name) {
		return "", fmt.Errorf("invalid container name %q", name)
	}
	return rootfsDiskPrefix + name + ".ext4", nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// makeUpperDisks builds the upper image of every EROFS container that has
// none yet from its host upper dir. It runs on cold boot once the host upper
// holds its final seed (stageMergedRootfs writes resolv.conf).
func makeUpperDisks(ctx context.Context, mke2fs, upperBase string, ctrs []actorContainer) error {
	if mke2fs == "" {
		return nil
	}
	for _, c := range ctrs {
		if !c.erofs {
			continue
		}
		img := upperDiskPath(upperBase, c.name)
		if fileExists(img) {
			continue // a ROOTFS snapshot's image, put back by stageRootfsUpper
		}
		t := time.Now()
		// No journal: a MEMORY snapshot carries the guest's ext4 state, and a
		// ROOTFS snapshot is taken unmounted. Lazy inode tables, packed
		// metadata, and no backup superblocks or resize inode keep the 4 GiB
		// image to a few extents; the guest mounts with noinit_itable.
		cmd := exec.CommandContext(ctx, mke2fs, "-q", "-F", "-t", "ext4",
			"-O", "^has_journal,^orphan_file,^resize_inode,sparse_super2",
			"-E", "lazy_itable_init=1,root_owner=0:0,packed_meta_blocks=1,num_backup_sb=0",
			"-m", "0", "-i", "65536",
			"-d", filepath.Join(upperBase, c.name),
			img, upperDiskSize)
		cmd.Env = []string{"MKE2FS_CONFIG=/nonexistent"}
		if out, err := cmd.CombinedOutput(); err != nil {
			_ = os.Remove(img)
			return fmt.Errorf("while building the upper disk of %q: %w: %s", c.name, err, out)
		}
		slog.DebugContext(ctx, "Built upper disk", slog.String("container", c.name), slog.Duration("took", time.Since(t)))
	}
	return nil
}

// addUpperDisks attaches the upper image of each EROFS container that has one
// and returns the VM's disks.
func (d *erofsDevices) addUpperDisks(upperBase string, ctrs []actorContainer) []ch.DiskConfig {
	var disks []ch.DiskConfig
	for _, c := range ctrs {
		if !c.erofs {
			continue
		}
		img := upperDiskPath(upperBase, c.name)
		if !fileExists(img) {
			continue
		}
		m := kata.UpperDiskMount(len(disks), c.id)
		disks = append(disks, ch.DiskConfig{Path: img, ImageType: "Raw", NumQueues: 1, QueueSize: 256})
		if d.upperBase == nil {
			d.upperBase = map[string]string{}
		}
		d.mounts = append(d.mounts, m)
		d.upperBase[c.name] = m.MountPoint
	}
	return disks
}

// hasUpperDisk reports whether cid's upper is on a disk.
func hasUpperDisk(upperBase, cid string) bool {
	return fileExists(upperDiskPath(upperBase, cid))
}

// linkUpperDisk puts cid's upper image into checkpointDir, if it has one.
// The actor is torn down after the checkpoint, so a hard link is enough. No
// fsync, as for the tars (#1954): restore falls back to object storage when a
// local checkpoint is missing, and fsync waits behind other actors' writes in
// the shared ext4 journal.
func linkUpperDisk(ctx context.Context, upperBase, checkpointDir, cid string) (bool, error) {
	img := upperDiskPath(upperBase, cid)
	if !fileExists(img) {
		return false, nil
	}
	name, err := rootfsDiskFile(cid)
	if err != nil {
		return true, err
	}
	dst := filepath.Join(checkpointDir, name)
	if err := os.Link(img, dst); errors.Is(err, unix.EXDEV) {
		if err := copySparse(ctx, img, dst); err != nil {
			return true, err
		}
	} else if err != nil {
		return true, fmt.Errorf("while linking upper disk %q: %w", img, err)
	}
	return true, nil
}

// restoreUpperDisk copies cid's upper image out of snapshotDir, if the
// snapshot has one. A copy, not a link: the guest writes to it, and the
// restore dir is not ours to change.
func restoreUpperDisk(ctx context.Context, upperBase, snapshotDir, cid string) (bool, error) {
	name, err := rootfsDiskFile(cid)
	if err != nil {
		return false, err
	}
	src := filepath.Join(snapshotDir, name)
	if !fileExists(src) {
		return false, nil
	}
	return true, copySparse(ctx, src, upperDiskPath(upperBase, cid))
}

// copySparse copies src to dst, keeping holes.
func copySparse(ctx context.Context, src, dst string) error {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()
	si, err := s.Stat()
	if err != nil {
		return err
	}
	d, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Truncate(si.Size()); err != nil {
		return err
	}
	sfd, dfd := int(s.Fd()), int(d.Fd())
	useKernel := true
	for off := int64(0); off < si.Size(); {
		if err := ctx.Err(); err != nil {
			return err
		}
		ds, err := unix.Seek(sfd, off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		} else if err != nil {
			return fmt.Errorf("SEEK_DATA: %w", err)
		}
		de, err := unix.Seek(sfd, ds, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("SEEK_HOLE: %w", err)
		}
		for pos := ds; useKernel && pos < de; {
			roff, woff := pos, pos
			n, err := unix.CopyFileRange(sfd, &roff, dfd, &woff, int(min(de-pos, 1<<30)), 0)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil || n == 0 {
				useKernel = false
				break
			}
			pos += int64(n)
		}
		if !useKernel {
			if _, err := io.Copy(io.NewOffsetWriter(d, ds), io.NewSectionReader(s, ds, de-ds)); err != nil {
				return err
			}
		}
		off = de
	}
	return d.Close()
}

// flushUpperDisks has the guest unmount the upper disks of containers before
// a ROOTFS checkpoint, so the image it ships is clean. Removing a container
// unmounts its overlay; destroying the sandbox unmounts the disks. The
// workload is discarded either way: a ROOTFS restore cold-boots.
func flushUpperDisks(ctx context.Context, vsockPath string, containers []string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ac, err := kata.DialAgent(ctx, vsockPath)
	if err != nil {
		return fmt.Errorf("while dialing the agent to flush upper disks: %w", err)
	}
	defer ac.Close()
	for _, cid := range containers {
		if err := ac.RemoveContainer(ctx, cid); err != nil {
			return err
		}
	}
	return ac.DestroySandbox(ctx)
}

// rewriteUpperDiskPaths points cfg's upper disk paths at upperBase.
func rewriteUpperDiskPaths(cfg map[string]any, cfgPath, upperBase string) (bool, error) {
	if upperBase == "" {
		return false, nil
	}
	disks, _ := cfg["disks"].([]any)
	changed := false
	for _, dk := range disks {
		dm, ok := dk.(map[string]any)
		if !ok {
			return false, fmt.Errorf("snapshot config %q has a malformed disk", cfgPath)
		}
		p, _ := dm["path"].(string)
		if !strings.HasSuffix(p, upperDiskSuffix) {
			continue // the guest image of a snapshot from before it moved to pmem
		}
		if want := filepath.Join(upperBase, filepath.Base(p)); want != p {
			dm["path"] = want
			changed = true
		}
	}
	return changed, nil
}

// rewriteSnapshotUpperDisks points the snapshot config's upper images at
// upperBase: the snapshot recorded the source actor's paths.
func rewriteSnapshotUpperDisks(snapshotDir, upperBase string) error {
	cfgPath := filepath.Join(snapshotDir, "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("parsing %q: %w", cfgPath, err)
	}
	changed, err := rewriteUpperDiskPaths(cfg, cfgPath, upperBase)
	if err != nil || !changed {
		return err
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	// Temp file + rename, as rewriteSnapshotSocketPaths does.
	tmp, err := os.CreateTemp(snapshotDir, ".config.json.tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(out); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), cfgPath)
}
