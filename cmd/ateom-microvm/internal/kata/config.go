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
	"strconv"
	"strings"
)

const (
	// TODO(#1724): Tune the following values for Substrate actors.
	// DefaultMemoryMiB is the default guest memory size (MiB).
	DefaultMemoryMiB = 2048
	// DefaultVCPUs is the default guest vCPU count.
	DefaultVCPUs = 1
)

// BaseKernelParams is the guest kernel command line parameters ateom boots with
const BaseKernelParams = "cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1"

// WithAgentDebug enables guest agent debug logging and the debug console on vsock 1026
func WithAgentDebug(kernelParams string) string {
	return appendKernelParams(kernelParams, "agent.log=", "agent.log=debug agent.debug_console agent.debug_console_vport=1026")
}

// WithPageReportingDelay sets the guest free page reporting delay to delayMS
// milliseconds through the page_reporting.delay_ms kernel parameter. A
// negative delayMS leaves kernelParams unchanged, so the guest kernel keeps its
// built-in delay (2 s). The parameter needs a guest kernel that carries the
// mm/page_reporting delay_ms patch; other kernels ignore it.
func WithPageReportingDelay(kernelParams string, delayMS int) string {
	if delayMS < 0 {
		return kernelParams
	}
	return appendKernelParams(kernelParams, "page_reporting.delay_ms=", "page_reporting.delay_ms="+strconv.Itoa(delayMS))
}

// appendKernelParams appends add to a kernel_params string unless marker is
// already present (so repeated calls are no-ops).
func appendKernelParams(kernelParams, marker, add string) string {
	if strings.Contains(kernelParams, marker) {
		return kernelParams
	}
	if kernelParams == "" {
		return add
	}
	return kernelParams + " " + add
}
