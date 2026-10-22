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

// BENCH ONLY. The worker pod is not privileged: its device cgroup denies
// /dev/mapper/control and the dm block devices. For benchmarking, a
// privileged helper pod (hack/microvm-assets/bench/rootfs-thin-helper.yaml)
// runs the worker's scripts chrooted into the host root, so dmsetup and mount
// act on the host; the mounts reach the worker through its HostToContainer
// /var/lib/ate mount. Not for production: the socket runs any script as host
// root.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

type helperRequest struct {
	Script string `json:"script"`
}

type helperResponse struct {
	Out string `json:"out"`
	Err string `json:"err,omitempty"`
}

// SocketRunner runs scripts through the helper listening on Path.
type SocketRunner struct {
	Path string
}

// Run implements Runner.
func (r SocketRunner) Run(ctx context.Context, script string) (string, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", r.Path)
	if err != nil {
		return "", fmt.Errorf("while dialing rootfs-thin helper: %w", err)
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if err := json.NewEncoder(conn).Encode(helperRequest{Script: script}); err != nil {
		return "", err
	}
	var resp helperResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return "", fmt.Errorf("while reading rootfs-thin helper reply: %w", err)
	}
	if resp.Err != "" {
		return resp.Out, errors.New(resp.Err)
	}
	return resp.Out, nil
}

// ServeHelper serves scripts on socket, each run with /bin/sh chrooted into
// root, until ctx is done.
func ServeHelper(ctx context.Context, socket, root string) error {
	_ = os.Remove(socket)
	lis, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		lis.Close()
	}()
	slog.InfoContext(ctx, "rootfs-thin helper serving", slog.String("socket", socket), slog.String("root", root))
	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go serveHelperConn(ctx, conn, root)
	}
}

func serveHelperConn(ctx context.Context, conn net.Conn, root string) {
	defer conn.Close()
	var req helperRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var stderr strings.Builder
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", req.Script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: root}
	cmd.Dir = "/"
	cmd.Env = []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin"}
	cmd.Stderr = &stderr
	start := time.Now()
	out, err := cmd.Output()
	resp := helperResponse{Out: string(out)}
	if err != nil {
		resp.Err = fmt.Sprintf("%v: %s", err, strings.TrimSpace(stderr.String()))
		slog.WarnContext(ctx, "rootfs-thin helper script failed", slog.String("err", resp.Err),
			slog.Duration("took", time.Since(start)), slog.String("script", req.Script))
	}
	_ = json.NewEncoder(conn).Encode(resp)
}
