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
	"fmt"
	"strings"
	"time"
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

// MemAgentConfig configures the kata-agent mem-agent, which periodically runs
// MGLRU-based memcg reclaim and memory compaction inside the guest.
type MemAgentConfig struct {
	// MemcgPeriod is the interval between memcg reclaim runs. Zero disables
	// memcg reclaim.
	MemcgPeriod time.Duration
	// CompactPeriod is the interval between compaction runs. Zero disables
	// compaction.
	CompactPeriod time.Duration
}

// WithMemAgent enables the kata-agent mem-agent with cfg's periods. It also
// turns on PSI (psi=1), which mem-agent reads to back off under memory
// pressure. The remaining mem-agent settings are fixed: memcg reclaim may
// swap with swappiness up to 60, both loops back off above 5% PSI, and
// compaction targets order-9 blocks, the size free page reporting returns.
// Memcg reclaim swaps only if the guest has swap (for example zram, see
// WithZramPercent).
func WithMemAgent(kernelParams string, cfg MemAgentConfig) string {
	params := []string{
		"psi=1",
		"agent.mem_agent_enable=true",
		fmt.Sprintf("agent.mem_agent_memcg_disable=%t", cfg.MemcgPeriod <= 0),
		fmt.Sprintf("agent.mem_agent_compact_disable=%t", cfg.CompactPeriod <= 0),
	}
	if cfg.MemcgPeriod > 0 {
		params = append(params,
			"agent.mem_agent_memcg_swap=true",
			"agent.mem_agent_memcg_swappiness_max=60",
			fmt.Sprintf("agent.mem_agent_memcg_period_secs=%d", periodSecs(cfg.MemcgPeriod)),
			"agent.mem_agent_memcg_period_psi_percent_limit=5",
			"agent.mem_agent_memcg_eviction_psi_percent_limit=5",
			"agent.mem_agent_memcg_eviction_run_aging_count_min=1",
		)
	}
	if cfg.CompactPeriod > 0 {
		params = append(params,
			fmt.Sprintf("agent.mem_agent_compact_period_secs=%d", periodSecs(cfg.CompactPeriod)),
			"agent.mem_agent_compact_period_psi_percent_limit=5",
			"agent.mem_agent_compact_psi_percent_limit=5",
			"agent.mem_agent_compact_sec_max=30",
			"agent.mem_agent_compact_order=9",
			"agent.mem_agent_compact_threshold=1",
			"agent.mem_agent_compact_force_times=1",
		)
	}
	return appendKernelParams(kernelParams, "agent.mem_agent_enable=", strings.Join(params, " "))
}

// periodSecs rounds d up to whole seconds, the unit mem-agent takes.
func periodSecs(d time.Duration) int64 {
	return int64((d + time.Second - 1) / time.Second)
}

// WithZramPercent asks the guest rootfs to size its zram swap device at
// percent of guest memory, through the zram_percent kernel parameter. Zero
// leaves kernelParams unchanged. Only a guest rootfs whose init reads
// zram_percent (and a guest kernel with CONFIG_ZRAM) acts on it; the stock kata
// rootfs ignores it.
func WithZramPercent(kernelParams string, percent int) string {
	if percent <= 0 {
		return kernelParams
	}
	return appendKernelParams(kernelParams, "zram_percent=", fmt.Sprintf("zram_percent=%d", percent))
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
