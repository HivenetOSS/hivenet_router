// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/router"
)

// a published event reaches every live subscriber. Captures the
// happy path a live subscriber relies on.
func TestRegistrationNotifier_FanOutToEverySubscriber(t *testing.T) {
	n := router.NewRegistrationNotifier()
	a := n.Subscribe()
	b := n.Subscribe()
	defer n.Unsubscribe(a)
	defer n.Unsubscribe(b)

	want := domain.RegistrationEvent{
		EventType:    domain.RegistrationRegistered,
		DeploymentID: "dep-1",
		ReplicaID:    "dep-1-0",
		AgentID:      "12D3Koo...",
		Timestamp:    time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC),
	}
	n.Publish(want)

	for _, sub := range []*router.RegistrationSubscription{a, b} {
		select {
		case got := <-sub.Events:
			assert.Equal(t, want, got)
		case <-time.After(time.Second):
			t.Fatal("subscriber did not receive the published event")
		}
	}
}

// Unsubscribe closes the channel and reclaims the slot; a published
// event after Unsubscribe is not delivered.
func TestRegistrationNotifier_UnsubscribeStopsDelivery(t *testing.T) {
	n := router.NewRegistrationNotifier()
	sub := n.Subscribe()
	require.Equal(t, 1, n.SubscriberCount())

	n.Unsubscribe(sub)
	assert.Equal(t, 0, n.SubscriberCount())

	// Channel must be closed.
	_, open := <-sub.Events
	assert.False(t, open, "Unsubscribe must close the channel")

	// Publish after Unsubscribe is a no-op (no panic, no goroutine leak).
	n.Publish(domain.RegistrationEvent{})
}

// a stuck subscriber must not block Publish. The send is
// non-blocking — excess events are dropped per subscriber, not buffered into
// the registry path.
func TestRegistrationNotifier_PublishIsNonBlockingOnFullBuffer(t *testing.T) {
	n := router.NewRegistrationNotifier()
	sub := n.Subscribe()
	defer n.Unsubscribe(sub)

	// Channel buffer is 16 (internal); push enough events to overflow without
	// reading.
	const overflow = 100
	done := make(chan struct{})
	go func() {
		for i := 0; i < overflow; i++ {
			n.Publish(domain.RegistrationEvent{
				EventType: domain.RegistrationRegistered,
				ReplicaID: "dep-1-0",
				Timestamp: time.Now(),
			})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber buffer — must be non-blocking")
	}

	assert.Greater(t, sub.Dropped, int64(0), "overflow must register as Dropped events")
}
