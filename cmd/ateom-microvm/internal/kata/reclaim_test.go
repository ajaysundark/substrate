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
	"os/exec"
	"strings"
	"testing"
	"time"
)

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

func TestPreSnapshotReclaimWithoutAgent(t *testing.T) {
	// Without a guest agent nothing runs and nothing waits.
	res := PreSnapshotReclaim(t.Context(), ReclaimConfig{Wait: time.Hour}, nil, "")
	if res.OK || res.Channel != "none" || res.WaitTook != 0 {
		t.Errorf("got %+v, want channel none, not OK, no wait", res)
	}
	if !strings.Contains(res.Attempts, "agent:skipped") {
		t.Errorf("attempts = %q, want the skipped agent", res.Attempts)
	}
}
