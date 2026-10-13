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
	"strings"
	"testing"
	"time"
)

func TestBaseKernelParamsNoDebugConsole(t *testing.T) {
	for _, forbidden := range []string{"agent.debug_console", "agent.debug_console_vport", "1026"} {
		if strings.Contains(BaseKernelParams, forbidden) {
			t.Errorf("expected BaseKernelParams %q not to contain %q", BaseKernelParams, forbidden)
		}
	}
}

func TestWithAgentDebug(t *testing.T) {
	got := WithAgentDebug(BaseKernelParams)
	for _, want := range []string{
		"agent.log=debug",
		"agent.debug_console",
		"agent.debug_console_vport=1026",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("expected WithAgentDebug(%q) to contain %q, but got %q", BaseKernelParams, want, got)
		}
	}
	if again := WithAgentDebug(got); again != got {
		t.Errorf("expected WithAgentDebug to be idempotent, but got %q then %q", got, again)
	}
}

func TestWithMemAgent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cfg    MemAgentConfig
		want   []string
		absent []string
	}{
		{
			name: "memcg and compact every 10s",
			cfg:  MemAgentConfig{MemcgPeriod: 10 * time.Second, CompactPeriod: 10 * time.Second},
			want: []string{
				"psi=1", "agent.mem_agent_enable=true",
				"agent.mem_agent_memcg_disable=false", "agent.mem_agent_memcg_period_secs=10", "agent.mem_agent_memcg_swap=true",
				"agent.mem_agent_compact_disable=false", "agent.mem_agent_compact_period_secs=10", "agent.mem_agent_compact_order=9",
			},
		},
		{
			name:   "compact only, period rounded up",
			cfg:    MemAgentConfig{CompactPeriod: 1500 * time.Millisecond},
			want:   []string{"agent.mem_agent_memcg_disable=true", "agent.mem_agent_compact_disable=false", "agent.mem_agent_compact_period_secs=2"},
			absent: []string{"agent.mem_agent_memcg_period_secs", "agent.mem_agent_memcg_swap"},
		},
		{
			name:   "memcg only",
			cfg:    MemAgentConfig{MemcgPeriod: time.Minute},
			want:   []string{"agent.mem_agent_memcg_disable=false", "agent.mem_agent_memcg_period_secs=60", "agent.mem_agent_compact_disable=true"},
			absent: []string{"agent.mem_agent_compact_period_secs"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := WithMemAgent(BaseKernelParams, tc.cfg)
			if !strings.HasPrefix(got, BaseKernelParams+" ") {
				t.Errorf("WithMemAgent dropped the base params: %q", got)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("WithMemAgent(%+v) = %q, missing %q", tc.cfg, got, want)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(got, absent) {
					t.Errorf("WithMemAgent(%+v) = %q, must not contain %q", tc.cfg, got, absent)
				}
			}
			if again := WithMemAgent(got, tc.cfg); again != got {
				t.Errorf("expected WithMemAgent to be idempotent, but got %q then %q", got, again)
			}
		})
	}
}

func TestWithZramPercent(t *testing.T) {
	if got := WithZramPercent(BaseKernelParams, 0); got != BaseKernelParams {
		t.Errorf("WithZramPercent(0) = %q, want the params unchanged", got)
	}
	got := WithZramPercent(BaseKernelParams, 50)
	if want := BaseKernelParams + " zram_percent=50"; got != want {
		t.Errorf("WithZramPercent(50) = %q, want %q", got, want)
	}
	if again := WithZramPercent(got, 50); again != got {
		t.Errorf("expected WithZramPercent to be idempotent, but got %q then %q", got, again)
	}
}
