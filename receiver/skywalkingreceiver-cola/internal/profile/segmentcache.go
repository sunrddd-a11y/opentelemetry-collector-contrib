// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"sync"
	"time"
)

// SegmentIdent is trace identity derived from an ingested SkyWalking segment.
type SegmentIdent struct {
	OTelTraceID     string
	SWTraceID       string
	Service         string
	ServiceInstance string
}

type segEntry struct {
	ident   SegmentIdent
	expires time.Time
}

// SegmentCache maps SkyWalking traceSegmentId to OTel/SW trace ids.
type SegmentCache struct {
	mu      sync.Mutex
	maxSize int
	ttl     time.Duration
	items   map[string]segEntry
}

func NewSegmentCache(maxSize int, ttl time.Duration) *SegmentCache {
	if maxSize <= 0 {
		maxSize = 100_000
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &SegmentCache{
		maxSize: maxSize,
		ttl:     ttl,
		items:   make(map[string]segEntry),
	}
}

func (c *SegmentCache) Put(segmentID string, ident SegmentIdent) {
	if c == nil || segmentID == "" {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.items) >= c.maxSize {
		c.evictExpiredLocked(now)
		if len(c.items) >= c.maxSize {
			// Drop an arbitrary entry to stay bounded.
			for k := range c.items {
				delete(c.items, k)
				break
			}
		}
	}
	c.items[segmentID] = segEntry{ident: ident, expires: now.Add(c.ttl)}
}

func (c *SegmentCache) Get(segmentID string) (SegmentIdent, bool) {
	if c == nil || segmentID == "" {
		return SegmentIdent{}, false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[segmentID]
	if !ok || now.After(e.expires) {
		if ok {
			delete(c.items, segmentID)
		}
		return SegmentIdent{}, false
	}
	return e.ident, true
}

func (c *SegmentCache) evictExpiredLocked(now time.Time) {
	for k, e := range c.items {
		if now.After(e.expires) {
			delete(c.items, k)
		}
	}
}
