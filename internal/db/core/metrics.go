package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/InsulaLabs/insi/pkg/models"
)

type Counters struct {
	OP_VS          atomic.Uint64
	OP_Cache       atomic.Uint64
	OP_Events      atomic.Uint64
	OP_Subscribers atomic.Uint64
	OP_Blobs       atomic.Uint64
	OP_System      atomic.Uint64
}

func (c *Counters) Diff(other *Counters) *Counters {
	vs := c.OP_VS.Load() - other.OP_VS.Load()
	cache := c.OP_Cache.Load() - other.OP_Cache.Load()
	events := c.OP_Events.Load() - other.OP_Events.Load()
	subscribers := c.OP_Subscribers.Load() - other.OP_Subscribers.Load()
	blobs := c.OP_Blobs.Load() - other.OP_Blobs.Load()
	system := c.OP_System.Load() - other.OP_System.Load()
	res := Counters{}
	res.OP_VS.Store(vs)
	res.OP_Cache.Store(cache)
	res.OP_Events.Store(events)
	res.OP_Subscribers.Store(subscribers)
	res.OP_Blobs.Store(blobs)
	res.OP_System.Store(system)
	return &res
}

type Metrics struct {
	// active counters are the counters that are being updated
	// as the system is running
	ActiveCounters Counters

	// last sampled counters
	PreviousCounters Counters

	// Calculated as a running total of ops per second
	CountersPerSecond models.OpsPerSecondCounters

	// We, once a second, will diff the atomics
	// to get ops per second
	LastPeriod time.Time

	// lock for safe concurrent access to non-atomic fields
	lock sync.Mutex
}

func (c *Core) IndVSOp() {
	// add to in-memory monotonic counter
	c.metrics.ActiveCounters.OP_VS.Add(1)
}

func (c *Core) IndCacheOp() {
	// add to in-memory monotonic counter
	c.metrics.ActiveCounters.OP_Cache.Add(1)
}

func (c *Core) IndEventsOp() {
	// add to in-memory monotonic counter
	c.metrics.ActiveCounters.OP_Events.Add(1)
}

func (c *Core) IndSubscribersOp() {
	// add to in-memory monotonic counter
	c.metrics.ActiveCounters.OP_Subscribers.Add(1)
}

func (c *Core) IndBlobsOp() {
	// add to in-memory monotonic counter
	c.metrics.ActiveCounters.OP_Blobs.Add(1)
}

func (c *Core) IndSystemOp() {
	// add to in-memory monotonic counter
	c.metrics.ActiveCounters.OP_System.Add(1)
}

func (c *Core) UpdateCounters() {
	c.metrics.lock.Lock()
	defer c.metrics.lock.Unlock()

	now := time.Now()
	if c.metrics.LastPeriod.IsZero() {
		c.metrics.LastPeriod = now
		// Also initialize PreviousCounters to the current ActiveCounters on the first run
		c.metrics.PreviousCounters.OP_VS.Store(c.metrics.ActiveCounters.OP_VS.Load())
		c.metrics.PreviousCounters.OP_Cache.Store(c.metrics.ActiveCounters.OP_Cache.Load())
		c.metrics.PreviousCounters.OP_Events.Store(c.metrics.ActiveCounters.OP_Events.Load())
		c.metrics.PreviousCounters.OP_Subscribers.Store(c.metrics.ActiveCounters.OP_Subscribers.Load())
		c.metrics.PreviousCounters.OP_Blobs.Store(c.metrics.ActiveCounters.OP_Blobs.Load())
		c.metrics.PreviousCounters.OP_System.Store(c.metrics.ActiveCounters.OP_System.Load())
		return
	}

	elapsed := now.Sub(c.metrics.LastPeriod).Seconds()
	if elapsed < 1 {
		// Avoid division by zero or inflating numbers if called too frequently.
		// We expect this to be called every second.
		return
	}

	diff := c.metrics.ActiveCounters.Diff(&c.metrics.PreviousCounters)

	c.metrics.CountersPerSecond.OP_VS = float64(diff.OP_VS.Load()) / elapsed
	c.metrics.CountersPerSecond.OP_Cache = float64(diff.OP_Cache.Load()) / elapsed
	c.metrics.CountersPerSecond.OP_Events = float64(diff.OP_Events.Load()) / elapsed
	c.metrics.CountersPerSecond.OP_Subscribers = float64(diff.OP_Subscribers.Load()) / elapsed
	c.metrics.CountersPerSecond.OP_Blobs = float64(diff.OP_Blobs.Load()) / elapsed
	c.metrics.CountersPerSecond.OP_System = float64(diff.OP_System.Load()) / elapsed
	// Update PreviousCounters for the next interval
	c.metrics.PreviousCounters.OP_VS.Store(c.metrics.ActiveCounters.OP_VS.Load())
	c.metrics.PreviousCounters.OP_Cache.Store(c.metrics.ActiveCounters.OP_Cache.Load())
	c.metrics.PreviousCounters.OP_Events.Store(c.metrics.ActiveCounters.OP_Events.Load())
	c.metrics.PreviousCounters.OP_Subscribers.Store(c.metrics.ActiveCounters.OP_Subscribers.Load())
	c.metrics.PreviousCounters.OP_Blobs.Store(c.metrics.ActiveCounters.OP_Blobs.Load())
	c.metrics.PreviousCounters.OP_System.Store(c.metrics.ActiveCounters.OP_System.Load())
	c.metrics.LastPeriod = now

	c.logger.Debug("Ops/sec",
		"vs", fmt.Sprintf("%.2f", c.metrics.CountersPerSecond.OP_VS),
		"cache", fmt.Sprintf("%.2f", c.metrics.CountersPerSecond.OP_Cache),
		"events", fmt.Sprintf("%.2f", c.metrics.CountersPerSecond.OP_Events),
		"subscribers", fmt.Sprintf("%.2f", c.metrics.CountersPerSecond.OP_Subscribers),
		"blobs", fmt.Sprintf("%.2f", c.metrics.CountersPerSecond.OP_Blobs),
		"system", fmt.Sprintf("%.2f", c.metrics.CountersPerSecond.OP_System),
	)
}

func (c *Core) GetOpsPerSecond() models.OpsPerSecondCounters {
	c.metrics.lock.Lock()
	defer c.metrics.lock.Unlock()
	return c.metrics.CountersPerSecond
}

func (c *Core) opsPerSecondHandler(w http.ResponseWriter, r *http.Request) {
	c.IndSystemOp()

	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path, rcPrivate)
		return
	}

	ops := c.GetOpsPerSecond()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ops); err != nil {
		c.logger.Error("failed to encode ops per second", "error", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
