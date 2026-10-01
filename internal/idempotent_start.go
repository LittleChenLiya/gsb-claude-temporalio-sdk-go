package internal

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// idempotentStartCall is shared by every ExecuteWorkflow call that joins an
// in-flight start carrying the same client idempotency key.
type idempotentStartCall struct {
	// doneCh is closed once the leader call has finished, so joiners unblock
	// without observing a partially written run/err.
	doneCh chan struct{}
	// waiters counts joiners that attached to the leader; not decremented when
	// a joiner gives up early, so it only grows while the call is in flight.
	waiters int
	run     WorkflowRun
	err     error
}

// idempotentStartCoordinator coalesces concurrent start workflow requests that
// carry the same user-supplied idempotency key: only the leader sends the
// StartWorkflowExecution RPC and all joiners wait for its result.
//
// Entries are always removed when the leader finishes (including when its
// context is canceled), so a canceled or failed start never leaves pending
// state behind and a later caller starts a fresh RPC.
type idempotentStartCoordinator struct {
	mu      sync.Mutex
	pending map[string]*idempotentStartCall
}

func newIdempotentStartCoordinator() *idempotentStartCoordinator {
	return &idempotentStartCoordinator{pending: make(map[string]*idempotentStartCall)}
}

// idempotencyKeyRequestIDNamespace is a fixed namespace used only to derive
// gRPC request IDs from user idempotency keys.
var idempotencyKeyRequestIDNamespace = uuid.MustParse("9f2c4b6a-7e1d-4f3b-9a8c-6d5e2f1a0b4c")

// requestIDFromIdempotencyKey maps a user idempotency key to a stable UUID v5
// request ID: equal keys always yield the same RequestId, different keys
// produce different RequestIds.
func requestIDFromIdempotencyKey(key string) string {
	return uuid.NewSHA1(idempotencyKeyRequestIDNamespace, []byte(key)).String()
}

// do runs leaderFn as the leader for key if no call for that key is in flight,
// otherwise it waits for the in-flight call to finish. A joiner whose context
// is canceled while waiting returns the context error but does not affect the
// leader or the shared entry.
func (c *idempotentStartCoordinator) do(
	ctx context.Context,
	key string,
	leaderFn func(context.Context) (WorkflowRun, error),
) (WorkflowRun, error) {
	c.mu.Lock()
	if call, ok := c.pending[key]; ok {
		call.waiters++
		c.mu.Unlock()
		select {
		case <-call.doneCh:
			return call.run, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	call := &idempotentStartCall{doneCh: make(chan struct{})}
	c.pending[key] = call
	c.mu.Unlock()

	// Remove the pending state before broadcasting the result so that a
	// canceled leader cannot keep later callers waiting or coalesce onto it.
	defer func() {
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
		close(call.doneCh)
	}()

	call.run, call.err = leaderFn(ctx)
	return call.run, call.err
}

// waitersFor reports the number of joiners currently waiting on the in-flight
// call for key. Intended for tests and diagnostics.
func (c *idempotentStartCoordinator) waitersFor(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if call, ok := c.pending[key]; ok {
		return call.waiters
	}
	return 0
}
