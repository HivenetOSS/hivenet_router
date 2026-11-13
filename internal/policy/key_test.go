// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"bytes"
	"context"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/test/testutil"

	"github.com/libp2p/go-libp2p/core/peer"
)

// keyedStub is a KeyedStrategy that records the key of every Select.
type keyedStub struct {
	bytes int
	keys  [][]byte
}

func (k *keyedStub) PrefixBytes() int { return k.bytes }

func (k *keyedStub) Select(c []ScoredCandidate, req SelectRequest) *domain.Agent {
	k.keys = append(k.keys, req.Key)
	return c[0].Agent
}

type listerStub map[string][]*domain.Agent

func (l listerStub) ListByModel(model string) []*domain.Agent { return l[model] }

// withStrategy registers s under name for the duration of the test.
func withStrategy(t *testing.T, name string, s Strategy) {
	t.Helper()
	registry[name] = s
	t.Cleanup(func() { delete(registry, name) })
}

func keyTestExecutor(pol *Policy, agents ...*domain.Agent) *Executor {
	stor := testutil.NoopStorage{}
	ev := NewEvaluator(stor, metrics.NewUniversalCounterStore(stor, nil))
	return NewExecutor(listerStub{"m": agents}, ev, pol, 3, 0)
}

func llmAgent(id string) *domain.Agent {
	return domain.NewAgent(peer.ID(id), domain.AgentMetadata{Model: "m", Capability: domain.CapabilityLLM}, "")
}

// The session asks for a key only when a step's strategy routes on it, and
// for the largest prefix any of its steps needs.
func TestSession_KeyPrefixBytes(t *testing.T) {
	withStrategy(t, "keyed-small", &keyedStub{bytes: 512})
	withStrategy(t, "keyed-large", &keyedStub{bytes: 4096})

	plain := keyTestExecutor(&Policy{RoutingPolicy: PolicyStep{Strategy: "least-loaded"}})
	if n := plain.NewSession("m", domain.CapabilityLLM).KeyPrefixBytes(); n != 0 {
		t.Errorf("least-loaded policy: KeyPrefixBytes = %d, want 0", n)
	}

	chain := keyTestExecutor(&Policy{
		RoutingPolicy: PolicyStep{Strategy: "keyed-small"},
		FallbackChain: []FallbackStep{
			{PolicyStep: PolicyStep{Strategy: "least-loaded"}},
			{PolicyStep: PolicyStep{Strategy: "keyed-large"}},
		},
	})
	if n := chain.NewSession("m", domain.CapabilityLLM).KeyPrefixBytes(); n != 4096 {
		t.Errorf("chain: KeyPrefixBytes = %d, want 4096 (largest step)", n)
	}
}

// Every Select of a session, retries included, passes the key to the strategy.
func TestSession_SelectPassesKey(t *testing.T) {
	stub := &keyedStub{bytes: 1024}
	withStrategy(t, "keyed", stub)
	exec := keyTestExecutor(&Policy{RoutingPolicy: PolicyStep{Strategy: "keyed", MaxTries: 3}},
		llmAgent("a"), llmAgent("b"))

	key := []byte("routing-key")
	s := exec.NewSession("m", domain.CapabilityLLM)
	s.SetKey(key)
	first, err := s.Select(context.Background())
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	first.DecrementLoad()
	s.RecordFailure(first.ID)
	second, err := s.Select(context.Background())
	if err != nil {
		t.Fatalf("Select after a failure: %v", err)
	}
	second.DecrementLoad()

	if len(stub.keys) != 2 {
		t.Fatalf("strategy called %d times, want 2", len(stub.keys))
	}
	for i, k := range stub.keys {
		if !bytes.Equal(k, key) {
			t.Errorf("call %d: key %q, want %q", i, k, key)
		}
	}
}
