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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeDebugConsole emulates CH's hybrid vsock handshake and kata's PTY shell:
// it answers "OK 1026", echoes the command line back (as a PTY does), runs the
// decoded script with the local sh, and prints the sentinel line.
func fakeDebugConsole(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	sock := filepath.Join(dir, "clh.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				line, _ := br.ReadString('\n')
				if !strings.HasPrefix(line, "CONNECT 1026") {
					fmt.Fprintf(c, "ERR\n")
					return
				}
				fmt.Fprintf(c, "OK 1026\n")
				cmd, _ := br.ReadString('\n')
				fmt.Fprintf(c, "bash: no job control in this shell\r\n")
				fmt.Fprintf(c, "root@guest:/# %s", strings.ReplaceAll(cmd, "\n", "\r\n")) // PTY echo
				// extract the base64 payload and run it
				fields := strings.Fields(cmd)
				var payload string
				for i, f := range fields {
					if f == "echo" && i+1 < len(fields) {
						payload = fields[i+1]
						break
					}
				}
				script, _ := base64.StdEncoding.DecodeString(payload)
				out, err := exec.Command("sh", "-c", string(script)).CombinedOutput()
				rc := 0
				if ee, ok := err.(*exec.ExitError); ok {
					rc = ee.ExitCode()
				}
				fmt.Fprintf(c, "%s", strings.ReplaceAll(string(out), "\n", "\r\n"))
				fmt.Fprintf(c, "__ATE_END__%d\r\n", rc)
			}(c)
		}
	}()
	return sock
}

func TestDebugConsoleRun(t *testing.T) {
	sock := fakeDebugConsole(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, rc, err := DebugConsoleRun(ctx, sock, "echo hello; echo 'it''s quoted \"ok\"'; exit 3")
	if err != nil {
		t.Fatalf("DebugConsoleRun: %v", err)
	}
	if rc != 3 {
		t.Errorf("rc = %d, want 3", rc)
	}
	if want := "hello\nits quoted \"ok\""; out != want {
		t.Errorf("out = %q, want %q", out, want)
	}
}

func TestGuestReclaimScriptRunsLocally(t *testing.T) {
	// The script must be valid POSIX sh and tolerate missing or unwritable
	// sysctl and cgroup files (non-root): run it with the local sh and check it
	// emits the pre/post/ms lines.
	out, _ := exec.Command("sh", "-c", GuestReclaimScript("2G")).CombinedOutput()
	for _, want := range []string{"pre: MemFree:", "post: MemFree:", "ms: drop="} {
		if !strings.Contains(string(out), want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestPreSnapshotReclaimChannels(t *testing.T) {
	sock := fakeDebugConsole(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No agent and no console fallback: nothing runs and nothing waits.
	res := PreSnapshotReclaim(ctx, ReclaimConfig{Wait: time.Hour}, sock, nil, "")
	if res.OK || res.Channel != "none" || res.WaitTook != 0 {
		t.Errorf("without the console fallback: got %+v, want channel none, not OK, no wait", res)
	}
	if !strings.Contains(res.Attempts, "agent:skipped") || strings.Contains(res.Attempts, "console") {
		t.Errorf("without the console fallback: attempts = %q, want only the skipped agent", res.Attempts)
	}

	// The console fallback runs the script and then waits cfg.Wait.
	res = PreSnapshotReclaim(ctx, ReclaimConfig{Wait: 10 * time.Millisecond, DebugConsole: true}, sock, nil, "")
	if res.Channel != "console" || !strings.Contains(res.Output, "post: MemFree:") {
		t.Errorf("with the console fallback: got %+v, want the console channel and the script output", res)
	}
	if res.WaitTook < 10*time.Millisecond {
		t.Errorf("with the console fallback: waited %s, want at least 10ms", res.WaitTook)
	}
}

func TestWithDebugConsole(t *testing.T) {
	if got := WithDebugConsole(WithAgentDebug("a=b")); strings.Count(got, "agent.debug_console_vport") != 1 {
		t.Errorf("duplicate debug console params: %q", got)
	}
	if got := WithDebugConsole("a=b"); strings.Contains(got, "agent.log=") {
		t.Errorf("WithDebugConsole must not enable agent debug logging: %q", got)
	}
}
