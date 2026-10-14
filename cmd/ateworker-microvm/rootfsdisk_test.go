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

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCopySparseKeepsDataAndHoles(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "src"), filepath.Join(dir, "dst")
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	const size = 64 << 20
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	a, b := bytes.Repeat([]byte("a"), 8192), bytes.Repeat([]byte("b"), 4096)
	if _, err := f.WriteAt(a, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(b, 40<<20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := copySparse(context.Background(), src, dst); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := os.ReadFile(src)
	if len(got) != size || !bytes.Equal(got, want) {
		t.Fatalf("copy differs: len %d", len(got))
	}
}

func TestRewriteSnapshotUpperDisks(t *testing.T) {
	dir := t.TempDir()
	cfg := map[string]any{"disks": []any{
		map[string]any{"path": "/static/kata.img"},
		map[string]any{"path": "/var/lib/ate/actors/old/rootfs-upper/astropy" + upperDiskSuffix},
	}}
	b, _ := json.Marshal(cfg)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rewriteSnapshotUpperDisks(dir, "/var/lib/ate/actors/new/rootfs-upper"); err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile(filepath.Join(dir, "config.json"))
	var out struct {
		Disks []struct{ Path string } `json:"disks"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Disks[0].Path != "/static/kata.img" || out.Disks[1].Path != "/var/lib/ate/actors/new/rootfs-upper/astropy"+upperDiskSuffix {
		t.Fatalf("got %+v", out.Disks)
	}
}

func TestAddUpperDisksOrderAndOverlay(t *testing.T) {
	base := t.TempDir()
	ctrs := []actorContainer{
		{name: "web", id: "web", erofs: true, erofsLayers: []string{"/l/1"}},
		{name: "host", id: "host", erofs: false},
		{name: "db", id: "db", erofs: true, erofsLayers: []string{"/l/1"}},
		{name: "share", id: "share", erofs: true, erofsLayers: []string{"/l/1"}},
	}
	for _, c := range []string{"web", "db"} {
		if err := os.WriteFile(upperDiskPath(base, c), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	devs := newEROFSDevices(ctrs)
	disks := devs.addUpperDisks(base, ctrs)
	if len(disks) != 2 || disks[0].Path != upperDiskPath(base, "web") || disks[1].Path != upperDiskPath(base, "db") || disks[0].Readonly {
		t.Fatalf("disks %+v", disks)
	}
	cfg := buildVMConfig("uid", "/vmlinux", "/rootfs.img", "", "/c.log", 256, 1, true, false, devs.pmem, devs.pciSegments(), disks)
	if len(cfg.Disks) != 2 || cfg.Pmem[0].File != "/rootfs.img" {
		t.Fatalf("vm config disks %+v pmem %+v", cfg.Disks, cfg.Pmem)
	}
	var devices []string
	for _, m := range devs.mounts[len(devs.pmem):] {
		devices = append(devices, m.Device)
	}
	if len(devices) != 2 || devices[0] != "/dev/vda" || devices[1] != "/dev/vdb" {
		t.Fatalf("devices %v", devices)
	}
	if ovl := devs.overlay(ctrs[2]); ovl.UpperDir != "/run/kata-containers/upper/db/fs" || ovl.WorkDir != "/run/kata-containers/upper/db/work" {
		t.Fatalf("overlay %+v", ovl)
	}
	// A container without a disk keeps its upper on the share.
	if ovl := devs.overlay(ctrs[3]); !strings.Contains(ovl.UpperDir, "/share/") && !strings.Contains(ovl.UpperDir, "/rw/") {
		t.Fatalf("share overlay %+v", ovl)
	}
}

func TestUpperDiskSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	src, ckpt, dst := t.TempDir(), t.TempDir(), t.TempDir()
	if ok, err := linkUpperDisk(ctx, src, ckpt, "web"); ok || err != nil {
		t.Fatalf("no disk: ok=%v err=%v", ok, err)
	}
	img := upperDiskPath(src, "web")
	if err := os.WriteFile(img, []byte("ext4"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ok, err := linkUpperDisk(ctx, src, ckpt, "web"); !ok || err != nil {
		t.Fatalf("link: ok=%v err=%v", ok, err)
	}
	if ok, err := restoreUpperDisk(ctx, dst, ckpt, "web"); !ok || err != nil {
		t.Fatalf("restore: ok=%v err=%v", ok, err)
	}
	if b, _ := os.ReadFile(upperDiskPath(dst, "web")); string(b) != "ext4" {
		t.Fatalf("restored %q", b)
	}
	if ok, _ := restoreUpperDisk(ctx, dst, ckpt, "db"); ok {
		t.Fatal("restored a disk the snapshot does not have")
	}
}
