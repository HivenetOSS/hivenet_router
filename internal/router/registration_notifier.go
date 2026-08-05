// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"sync"

	"hivenet_router/internal/domain"
)

// RegistrationSubscription is one live SSE subscriber's channel. The router
// publishes into Events with a non-blocking send: a slow / stuck consumer
// drops events rather than backing up the registry path, and Dropped lets it
// surface that in logs / a future `:` keepalive comment.
type RegistrationSubscription struct {
	Events  chan domain.RegistrationEvent
	Dropped int64
}

// RegistrationNotifier fans out registration deltas to every live subscriber.
// Subscribers are typically SSE handlers on /admin/registration-stream — one
// per HTTP request. Cancelled subscribers must call Unsubscribe so their slot
// is reclaimed.
//
// Subscription Events channel buffer size is intentionally small (16): a fast
// agent fleet generates one event per connect/disconnect, and the watcher's
// own debounce + snapshot resync repairs anything the buffer drops under
// pressure. Keeping the buffer bounded is what protects the registry write
// path from a stuck consumer.
type RegistrationNotifier struct {
	mu          sync.RWMutex
	subscribers map[*RegistrationSubscription]struct{}
}

// NewRegistrationNotifier builds an empty notifier.
func NewRegistrationNotifier() *RegistrationNotifier {
	return &RegistrationNotifier{subscribers: make(map[*RegistrationSubscription]struct{})}
}

// Subscribe registers a new channel; the caller iterates Events until the
// context is cancelled and then calls Unsubscribe.
func (n *RegistrationNotifier) Subscribe() *RegistrationSubscription {
	sub := &RegistrationSubscription{Events: make(chan domain.RegistrationEvent, 16)}
	n.mu.Lock()
	n.subscribers[sub] = struct{}{}
	n.mu.Unlock()
	return sub
}

// Unsubscribe removes a subscriber and closes its channel.
func (n *RegistrationNotifier) Unsubscribe(sub *RegistrationSubscription) {
	n.mu.Lock()
	if _, ok := n.subscribers[sub]; ok {
		delete(n.subscribers, sub)
		close(sub.Events)
	}
	n.mu.Unlock()
}

// Publish broadcasts an event to every subscriber. The send is non-blocking:
// a full channel increments Dropped and the event is skipped for that
// subscriber. The router's registry path must never wait on a slow watcher.
func (n *RegistrationNotifier) Publish(ev domain.RegistrationEvent) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for sub := range n.subscribers {
		select {
		case sub.Events <- ev:
		default:
			sub.Dropped++
		}
	}
}

// SubscriberCount is exposed for tests / diagnostics.
func (n *RegistrationNotifier) SubscriberCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.subscribers)
}
