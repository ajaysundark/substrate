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

package kata

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Pre-snapshot guest reclaim.
//
// Free page reporting (virtio-balloon FPR) only punches pages out of the host
// /memfd:ch_ram that the guest kernel has actually freed into >= order-9 buddy
// blocks. A workload that keeps freed heap spans (Go GOGC=100, glibc malloc,
// PyTorch caching allocator) never frees them, so a snapshot taken right after
// an episode carries the full peak footprint. Running a synchronous reclaim
// inside the guest right before vm.pause moves cold anon pages into guest swap
// (zram, when the guest has it) and drops clean page cache, so FPR can report
// the freed blocks before the vCPUs stop.
//
// Guest command (runs as root):
//   1. echo 1 > /proc/sys/vm/drop_caches          clean page cache
//   2. echo 2G > /sys/fs/cgroup/*/memory.reclaim   cgroup v2 proactive reclaim on
//                                                  every top-level cgroup (the
//                                                  root cgroup has no memory.reclaim)
//   3. echo 1 > /proc/sys/vm/compact_memory        merge freed 4 KiB pages into
//                                                  order-9 blocks FPR can report
// then the host waits for the guest's free page reporting delay (2 s in an
// unpatched guest kernel) plus a margin for the reporting worker to hand the
// blocks to the VMM.

// Defaults for ReclaimConfig.
const (
	DefaultReclaimWait   = 2200 * time.Millisecond
	DefaultReclaimTarget = "2G"
)

// ReclaimConfig configures the pre-snapshot guest reclaim.
type ReclaimConfig struct {
	// Target is the byte count written to each memory.reclaim (e.g. "2G").
	Target string
	// Wait is the host-side wait between the guest reclaim and vm.pause. It
	// must cover the guest's free page reporting delay.
	Wait time.Duration
	// DebugConsole allows the kata debug console (a guest root shell on vsock
	// 1026) as a fallback channel when agent ExecProcess fails. Benchmarking
	// only: the guest must boot with WithDebugConsole, which exposes a root
	// shell to anything that can reach the VM's vsock socket.
	DebugConsole bool
}

// WithDebugConsole enables only the kata debug console on vsock 1026 (no agent
// debug logging). Benchmarking only: it is the guest root shell channel the
// pre-snapshot reclaim falls back to when the workload container has no shell
// for ExecProcess.
func WithDebugConsole(kernelParams string) string {
	return appendKernelParams(kernelParams, "agent.debug_console", "agent.debug_console agent.debug_console_vport=1026")
}

// GuestReclaimScript returns the shell script run inside the guest. It prints a
// meminfo line before and after, the per-step durations in ms, and the
// order-9/order-10 free block counts, so the checkpoint log shows what the
// guest actually freed.
func GuestReclaimScript(target string) string {
	return `m() { awk '/^(MemFree|Cached|AnonPages|SwapFree|SwapTotal):/{printf "%s%d ", $1, $2/1024}' /proc/meminfo; }
b() { awk '/Normal|DMA32/{printf "%s:o9=%s,o10=%s ", $4, $(NF-1), $NF}' /proc/buddyinfo; }
echo "pre: $(m)$(b)"
t0=$(date +%s%N)
echo 1 > /proc/sys/vm/drop_caches
t1=$(date +%s%N)
rc=""
for f in /sys/fs/cgroup/*/memory.reclaim; do
  [ -f "$f" ] || continue
  if echo ` + target + ` > "$f" 2>/dev/null; then rc="$rc $(basename $(dirname $f))=ok"; else rc="$rc $(basename $(dirname $f))=partial"; fi
done
t2=$(date +%s%N)
echo 1 > /proc/sys/vm/compact_memory
t3=$(date +%s%N)
echo "post: $(m)$(b)"
echo "ms: drop=$(( (t1-t0)/1000000 )) reclaim=$(( (t2-t1)/1000000 )) compact=$(( (t3-t2)/1000000 )) reclaim_rc:$rc"
`
}

// ReclaimResult is what the checkpoint logs for one pre-snapshot reclaim.
type ReclaimResult struct {
	Channel  string        // "agent", "console", or "none"
	OK       bool          // the guest command ran to completion
	Err      string        // last error text when !OK (all attempted channels)
	ExecTook time.Duration // wall time of the guest command (dial + run)
	WaitTook time.Duration // host-side wait for free page reporting
	Output   string        // guest stdout (meminfo before/after, step timings)
	Attempts string        // per-channel attempt summary for the exec-channel verification
}

// agentExecBroken remembers that the kata-agent ExecProcess channel failed in
// this process. With the console fallback allowed, later checkpoints go
// straight to the console instead of paying a failing RPC each time; without
// it, every checkpoint retries the agent.
var agentExecBroken atomic.Bool

// PreSnapshotReclaim runs the guest reclaim through kata-agent ExecProcess on
// containerID, falling back to the debug console only when cfg.DebugConsole is
// set, then waits cfg.Wait for free page reporting. It never fails the checkpoint: every
// outcome is reported in the result for the caller to log.
func PreSnapshotReclaim(ctx context.Context, cfg ReclaimConfig, vsockPath string, ac *AgentClient, containerID string) ReclaimResult {
	res := ReclaimResult{Channel: "none"}
	target := cfg.Target
	if target == "" {
		target = DefaultReclaimTarget
	}
	script := GuestReclaimScript(target)
	var attempts []string

	tryAgent := func() bool {
		if ac == nil || containerID == "" {
			attempts = append(attempts, "agent:skipped(no guest agent or container id)")
			return false
		}
		if cfg.DebugConsole && agentExecBroken.Load() {
			attempts = append(attempts, "agent:skipped(previously failed)")
			return false
		}
		ectx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		t := time.Now()
		execID := fmt.Sprintf("%s-reclaim-%d", containerID, time.Now().UnixNano())
		code, out, err := ac.ExecShellCapture(ectx, containerID, execID, script)
		took := time.Since(t)
		if err != nil {
			agentExecBroken.Store(true)
			attempts = append(attempts, fmt.Sprintf("agent:failed(%v took=%s)", err, took.Round(time.Millisecond)))
			res.Err = err.Error()
			return false
		}
		attempts = append(attempts, fmt.Sprintf("agent:ok(exit=%d took=%s)", code, took.Round(time.Millisecond)))
		res.Channel, res.OK, res.ExecTook, res.Output = "agent", code == 0, took, out
		if code != 0 {
			res.Err = fmt.Sprintf("guest script exit=%d", code)
		}
		return true
	}
	tryConsole := func() bool {
		if vsockPath == "" {
			attempts = append(attempts, "console:skipped(no vsock path)")
			return false
		}
		cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		t := time.Now()
		out, code, err := DebugConsoleRun(cctx, vsockPath, script)
		took := time.Since(t)
		if err != nil {
			attempts = append(attempts, fmt.Sprintf("console:failed(%v took=%s)", err, took.Round(time.Millisecond)))
			res.Err = err.Error()
			return false
		}
		attempts = append(attempts, fmt.Sprintf("console:ok(exit=%d took=%s)", code, took.Round(time.Millisecond)))
		res.Channel, res.OK, res.ExecTook, res.Output = "console", code == 0, took, out
		if code != 0 {
			res.Err = fmt.Sprintf("guest script exit=%d", code)
		}
		return true
	}

	if !tryAgent() && cfg.DebugConsole {
		tryConsole()
	}
	res.Attempts = strings.Join(attempts, "; ")

	if res.OK {
		// Let the guest's page_reporting worker (PAGE_REPORTING_DELAY after the
		// first free) hand the order-9 blocks to virtio-balloon so CH punches
		// them out of the memfd before the vCPUs are paused.
		wait := cfg.Wait
		t := time.Now()
		select {
		case <-time.After(wait):
		case <-ctx.Done():
		}
		res.WaitTook = time.Since(t)
	}
	return res
}

// DebugConsoleRun runs script through the kata debug console (vsock 1026), a
// root shell in the guest's root namespaces, and returns its combined output
// and exit status. The script is base64-encoded into one command line so the
// interactive PTY shell neither echoes it back line by line nor trips on
// quoting, and a split sentinel ("__ATE''_END__" in the echoed command,
// "__ATE_END__<rc>" in the output) marks completion. Every read and write is
// bounded by ctx (deadline or a 5 s default), unlike DebugConsoleDump.
func DebugConsoleRun(ctx context.Context, vsockPath, script string) (string, int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", vsockPath)
	if err != nil {
		return "", -1, fmt.Errorf("debug-console dial: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline)
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", debugConsoleVsockPort); err != nil {
		return "", -1, fmt.Errorf("debug-console CONNECT: %w", err)
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return "", -1, fmt.Errorf("debug-console CONNECT reply: %w", err)
	}
	if !strings.HasPrefix(line, "OK ") {
		return "", -1, fmt.Errorf("debug-console CONNECT refused: %q", strings.TrimSpace(line))
	}
	enc := base64.StdEncoding.EncodeToString([]byte(script))
	const sentinel = "__ATE_END__"
	if _, err := fmt.Fprintf(conn, "echo %s | base64 -d | sh 2>&1; echo __ATE''_END__$?; exit\n", enc); err != nil {
		return "", -1, fmt.Errorf("debug-console write: %w", err)
	}
	// A PTY shell echoes the command line (and may print a prompt or "no job
	// control" noise before it). Output is what follows the echo; if the shell
	// never echoes, keep everything.
	var pre, out strings.Builder
	cur := &pre
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			clean := strings.TrimRight(line, "\r\n")
			if i := strings.Index(clean, sentinel); i >= 0 && !strings.Contains(clean, "__ATE''_END__") {
				rc, convErr := strconv.Atoi(strings.TrimSpace(clean[i+len(sentinel):]))
				if convErr != nil {
					rc = -1
				}
				if cur == &pre {
					return strings.TrimSpace(pre.String()), rc, nil
				}
				return strings.TrimSpace(out.String()), rc, nil
			}
			if strings.Contains(clean, "base64 -d | sh") {
				cur = &out // the echo of our command line; real output follows
				continue
			}
			if strings.TrimSpace(clean) == "" {
				continue
			}
			cur.WriteString(clean)
			cur.WriteByte('\n')
		}
		if err != nil {
			return strings.TrimSpace(pre.String() + out.String()), -1, fmt.Errorf("debug-console read (no sentinel): %w", err)
		}
	}
}

// ExecProcess executes a command inside a running guest container. Mirrors
// grpc.AgentService/ExecProcess.
func (a *AgentClient) ExecProcess(ctx context.Context, req *agentpb.ExecProcessRequest) error {
	if err := a.client.Call(ctx, "grpc.AgentService", "ExecProcess", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent ExecProcess %q: %w", req.GetContainerId(), err)
	}
	return nil
}

// ExecShellCapture runs "/bin/sh -c <cmd>" inside containerID as root, drains
// its stdout, and waits for it to exit. The container must ship /bin/sh: an
// exec'd process is confined to the container's rootfs, so a distroless image
// fails here with ENOENT.
func (a *AgentClient) ExecShellCapture(ctx context.Context, containerID, execID, cmd string) (int32, string, error) {
	req := &agentpb.ExecProcessRequest{
		ContainerId: containerID,
		ExecId:      execID,
		Process: &agentpb.Process{
			Args: []string{"/bin/sh", "-c", cmd},
			Cwd:  "/",
			User: &agentpb.User{UID: 0, GID: 0},
		},
	}
	if err := a.ExecProcess(ctx, req); err != nil {
		return -1, "", err
	}
	var out strings.Builder
	for {
		data, err := a.ReadStdout(ctx, containerID, execID, 64<<10)
		if len(data) > 0 {
			out.Write(data)
		}
		if err != nil || len(data) == 0 {
			break
		}
	}
	code, err := a.WaitProcess(ctx, containerID, execID)
	return code, strings.TrimSpace(out.String()), err
}
