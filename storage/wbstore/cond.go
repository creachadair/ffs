package wbstore

import "sync"

// cond is an edge-triggered condition.
//
// Waiters block on the Ready channel for the condition to occur.
// Signal wakes all pending waiters and resets the condition.
type cond struct {
	μ  sync.Mutex
	ch chan struct{}
}

func newCond() *cond { return &cond{ch: make(chan struct{})} }

// Signal wakes all pending waiters and resets the condition.
func (c *cond) Signal() {
	c.μ.Lock()
	defer c.μ.Unlock()
	close(c.ch)
	c.ch = make(chan struct{})
}

// Ready returns a channel that is closed when the condition is signaled (by a
// call to Signal).
func (c *cond) Ready() <-chan struct{} {
	c.μ.Lock()
	defer c.μ.Unlock()
	return c.ch
}

// handoff is a level-triggered rendezvous.
//
// Waiters block on the Ready channel for a handoff to occur.
// Set delivers a handoff which will persist until consumed by a waiter.
//
// Handoffs do not stack; once a handoff is pending additional calls to Set
// will be no-ops.
type handoff struct {
	ch chan interface{}
}

func newHandoff() *handoff { return &handoff{ch: make(chan interface{}, 1)} }

// Set delivers a handoff to the rendezvous if one is not already present.
func (h *handoff) Set(v interface{}) {
	select {
	case h.ch <- v:
	default:
	}
}

// Ready returns a channel that delivers a value when a handoff is available.
func (h *handoff) Ready() <-chan interface{} { return h.ch }
