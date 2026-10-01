package internal

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/sdk/internal/common/metrics"
)

// fakeIdempotentStartService is an in-process WorkflowService that records
// every StartWorkflowExecution attempt (and its RequestId), can fail the
// first attempts with a retriable error, and can block until released.
type fakeIdempotentStartService struct {
	workflowservice.UnimplementedWorkflowServiceServer

	mu sync.Mutex
	// startAttempts records RequestIds in arrival order.
	startAttempts []string
	// release unblocks a blocked StartWorkflowExecution handler.
	release chan struct{}
	// block makes the handler wait for release (and return Canceled if its
	// context is canceled first).
	block bool
	// failFirst is the number of initial attempts that fail with Unavailable.
	failFirst int
}

func newFakeIdempotentStartService() *fakeIdempotentStartService {
	return &fakeIdempotentStartService{
		release: make(chan struct{}),
	}
}

func (f *fakeIdempotentStartService) GetSystemInfo(
	context.Context, *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (f *fakeIdempotentStartService) StartWorkflowExecution(
	ctx context.Context, req *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	f.mu.Lock()
	f.startAttempts = append(f.startAttempts, req.RequestId)
	fail := len(f.startAttempts) <= f.failFirst
	block := f.block
	f.mu.Unlock()

	if block {
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, status.Error(codes.Canceled, ctx.Err().Error())
		}
	}
	if fail {
		return nil, status.Error(codes.Unavailable, "simulated transient timeout")
	}
	return &workflowservice.StartWorkflowExecutionResponse{
		RunId:               "run-1",
		FirstExecutionRunId: "first-run-1",
	}, nil
}

func (f *fakeIdempotentStartService) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.startAttempts)
}

func (f *fakeIdempotentStartService) attemptRequestIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.startAttempts...)
}

// startIdempotentTestGRPCServer runs the fake service on a local listener.
func startIdempotentTestGRPCServer(t *testing.T, f *fakeIdempotentStartService) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(srv, f)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)
	return l.Addr().String()
}

// newIdempotentTestClient builds a client wired with the same required
// interceptors as DialClient, including the gRPC retry interceptor.
func newIdempotentTestClient(t *testing.T, addr string) *WorkflowClient {
	t.Helper()
	excludeInternal := &atomic.Bool{}
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(requiredInterceptors(
			&ClientOptions{MetricsHandler: metrics.NopHandler}, excludeInternal)...),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return NewServiceClient(workflowservice.NewWorkflowServiceClient(conn), conn, ClientOptions{})
}

func startOptionsWithKey(key string) StartWorkflowOptions {
	return StartWorkflowOptions{
		ID:             "wf-idempotency-test",
		TaskQueue:      "tq-idempotency-test",
		IdempotencyKey: key,
	}
}

// TestIdempotencyKeyRetriesReuseRequestId verifies that a start whose first
// RPC attempt fails with a retriable Unavailable and then succeeds performs
// the local retry with the same gRPC RequestId derived from the key.
func TestIdempotencyKeyRetriesReuseRequestId(t *testing.T) {
	fake := newFakeIdempotentStartService()
	fake.failFirst = 1
	addr := startIdempotentTestGRPCServer(t, fake)
	c := newIdempotentTestClient(t, addr)

	run, err := c.ExecuteWorkflow(t.Context(), startOptionsWithKey("order-123"), "test-workflow")
	require.NoError(t, err)
	require.Equal(t, "wf-idempotency-test", run.GetID())

	require.Eventually(t, func() bool { return fake.attemptCount() == 2 }, 5*time.Second, 10*time.Millisecond)
	ids := fake.attemptRequestIDs()
	require.Len(t, ids, 2)
	require.NotEmpty(t, ids[0])
	require.Equal(t, ids[0], ids[1], "local retry must reuse the same request id")
	require.Equal(t, requestIDFromIdempotencyKey("order-123"), ids[0])
	require.NotEqual(t, requestIDFromIdempotencyKey("order-456"), ids[0])
}

// TestIdempotencyKeyWithoutKeyIsUnaffected verifies calls without a key keep
// using random, distinct request IDs and are not coalesced.
func TestIdempotencyKeyWithoutKeyIsUnaffected(t *testing.T) {
	fake := newFakeIdempotentStartService()
	addr := startIdempotentTestGRPCServer(t, fake)
	c := newIdempotentTestClient(t, addr)

	opts := StartWorkflowOptions{ID: "wf-no-key", TaskQueue: "tq"}
	_, err := c.ExecuteWorkflow(t.Context(), opts, "test-workflow")
	require.NoError(t, err)
	_, err = c.ExecuteWorkflow(t.Context(), opts, "test-workflow")
	require.NoError(t, err)

	require.Eventually(t, func() bool { return fake.attemptCount() == 2 }, 5*time.Second, 10*time.Millisecond)
	ids := fake.attemptRequestIDs()
	require.NotEqual(t, ids[0], ids[1], "without a key each start gets a fresh random request id")
}

// TestIdempotencyKeyConcurrentSameKeyCoalesces verifies concurrent starts with
// the same key send exactly one RPC and all callers share its result.
func TestIdempotencyKeyConcurrentSameKeyCoalesces(t *testing.T) {
	fake := newFakeIdempotentStartService()
	fake.block = true
	addr := startIdempotentTestGRPCServer(t, fake)
	c := newIdempotentTestClient(t, addr)

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	runs := make([]WorkflowRun, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runs[i], errs[i] = c.ExecuteWorkflow(t.Context(), startOptionsWithKey("shared-key"), "test-workflow")
		}(i)
	}

	// Only the leader reaches the service.
	require.Eventually(t, func() bool { return fake.attemptCount() == 1 }, 5*time.Second, 10*time.Millisecond)
	close(fake.release)
	wg.Wait()

	require.Equal(t, 1, fake.attemptCount(), "same key must coalesce to a single RPC")
	for i := range n {
		require.NoError(t, errs[i])
		require.Equal(t, "wf-idempotency-test", runs[i].GetID())
		require.Equal(t, "run-1", runs[i].GetRunID())
	}
}

// TestIdempotencyKeyConcurrentDifferentKeysDoNotMerge verifies concurrent
// starts with different keys are each sent independently.
func TestIdempotencyKeyConcurrentDifferentKeysDoNotMerge(t *testing.T) {
	fake := newFakeIdempotentStartService()
	fake.block = true
	addr := startIdempotentTestGRPCServer(t, fake)
	c := newIdempotentTestClient(t, addr)

	const n = 4
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			suffix := string(rune('a' + i))
			opts := startOptionsWithKey("distinct-key-" + suffix)
			opts.ID = "wf-distinct-" + suffix
			_, _ = c.ExecuteWorkflow(t.Context(), opts, "test-workflow")
		}(i)
	}

	require.Eventually(t, func() bool { return fake.attemptCount() == n }, 5*time.Second, 10*time.Millisecond)
	close(fake.release)
	wg.Wait()

	require.Equal(t, n, fake.attemptCount(), "different keys must not be merged")
	ids := fake.attemptRequestIDs()
	seen := map[string]struct{}{}
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	require.Len(t, seen, n, "each key must use a distinct request id")
}

// TestIdempotencyKeyCanceledCallDoesNotRetainPending verifies that canceling
// the leader removes its pending state: a later caller with the same key is
// not coalesced onto the canceled call and performs a fresh RPC.
func TestIdempotencyKeyCanceledCallDoesNotRetainPending(t *testing.T) {
	fake := newFakeIdempotentStartService()
	fake.block = true
	addr := startIdempotentTestGRPCServer(t, fake)
	c := newIdempotentTestClient(t, addr)

	leaderCtx, cancelLeader := context.WithCancel(t.Context())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := c.ExecuteWorkflow(leaderCtx, startOptionsWithKey("cancel-key"), "test-workflow")
		leaderErr <- err
	}()

	// Wait for the leader RPC to reach the service, then cancel it.
	require.Eventually(t, func() bool { return fake.attemptCount() == 1 }, 5*time.Second, 10*time.Millisecond)
	cancelLeader()
	select {
	case err := <-leaderErr:
		var canceled *serviceerror.Canceled
		require.ErrorAs(t, err, &canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not return after cancellation")
	}

	// A fresh start with the same key issues a new RPC rather than waiting on
	// the canceled pending entry.
	fake.mu.Lock()
	fake.block = false
	fake.mu.Unlock()

	run, err := c.ExecuteWorkflow(t.Context(), startOptionsWithKey("cancel-key"), "test-workflow")
	require.NoError(t, err)
	require.Equal(t, "wf-idempotency-test", run.GetID())
	require.Equal(t, 2, fake.attemptCount(), "canceled call must not retain pending state")
}

// TestIdempotencyKeyJoinerCanceledDoesNotRemovePending verifies that a joiner
// canceling its own wait does not disturb the leader or coalescing.
func TestIdempotencyKeyJoinerCanceledDoesNotRemovePending(t *testing.T) {
	fake := newFakeIdempotentStartService()
	fake.block = true
	addr := startIdempotentTestGRPCServer(t, fake)
	c := newIdempotentTestClient(t, addr)

	leaderErr := make(chan error, 1)
	go func() {
		_, err := c.ExecuteWorkflow(t.Context(), startOptionsWithKey("joiner-key"), "test-workflow")
		leaderErr <- err
	}()
	require.Eventually(t, func() bool { return fake.attemptCount() == 1 }, 5*time.Second, 10*time.Millisecond)

	joinerCtx, cancelJoiner := context.WithCancel(t.Context())
	joinerErr := make(chan error, 1)
	go func() {
		_, err := c.ExecuteWorkflow(joinerCtx, startOptionsWithKey("joiner-key"), "test-workflow")
		joinerErr <- err
	}()
	cancelJoiner()
	select {
	case err := <-joinerErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("joiner did not return after its context was canceled")
	}

	// The leader remains pending and completes normally: still a single RPC.
	close(fake.release)
	select {
	case err := <-leaderErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not return")
	}
	require.Equal(t, 1, fake.attemptCount())
}

// TestIdempotentStartCoordinatorUnit exercises the coordinator directly.
func TestIdempotentStartCoordinatorUnit(t *testing.T) {
	t.Run("same key coalesces and different keys do not merge", func(t *testing.T) {
		var sameKeyCalls atomic.Int32
		var otherKeyCalls atomic.Int32
		release := make(chan struct{})
		coord := newIdempotentStartCoordinator()

		leaderReady := make(chan struct{})
		leaderFn := func(ctx context.Context) (WorkflowRun, error) {
			if sameKeyCalls.Add(1) == 1 {
				close(leaderReady)
			}
			<-release
			return nil, nil
		}

		var wg sync.WaitGroup
		for range 3 {
			wg.Add(1)
			go func() { defer wg.Done(); _, _ = coord.do(t.Context(), "k", leaderFn) }()
		}
		<-leaderReady
		// The other two goroutines join the leader before it is released.
		require.Eventually(t, func() bool { return coord.waitersFor("k") == 2 }, 5*time.Second, time.Millisecond)

		// A different key runs independently and is not coalesced.
		otherDone := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = coord.do(t.Context(), "other", func(context.Context) (WorkflowRun, error) {
				otherKeyCalls.Add(1)
				close(otherDone)
				return nil, nil
			})
		}()
		<-otherDone

		close(release)
		wg.Wait()
		require.Equal(t, int32(1), sameKeyCalls.Load(), "same key must coalesce to one leader call")
		require.Equal(t, int32(1), otherKeyCalls.Load(), "different key runs independently")
	})

	t.Run("leader cancellation removes pending state", func(t *testing.T) {
		var calls atomic.Int32
		coord := newIdempotentStartCoordinator()

		ctx, cancel := context.WithCancel(t.Context())
		leaderDone := make(chan struct{})
		go func() {
			_, _ = coord.do(ctx, "k", func(ctx context.Context) (WorkflowRun, error) {
				calls.Add(1)
				<-ctx.Done()
				return nil, ctx.Err()
			})
			close(leaderDone)
		}()
		require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
		cancel()
		<-leaderDone

		// Pending entry was removed: a new call becomes the leader.
		_, err := coord.do(t.Context(), "k", func(context.Context) (WorkflowRun, error) {
			calls.Add(1)
			return nil, nil
		})
		require.NoError(t, err)
		require.Equal(t, int32(2), calls.Load())
	})

	t.Run("leader error is shared with joiners", func(t *testing.T) {
		coord := newIdempotentStartCoordinator()
		wantErr := errors.New("boom")
		release := make(chan struct{})
		leaderStarted := make(chan struct{})

		var wg sync.WaitGroup
		errs := make([]error, 3)
		for i := range 3 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = coord.do(t.Context(), "k", func(context.Context) (WorkflowRun, error) {
					select {
					case <-leaderStarted:
					default:
						close(leaderStarted)
					}
					<-release
					return nil, wantErr
				})
			}(i)
		}
		<-leaderStarted
		require.Eventually(t, func() bool { return coord.waitersFor("k") == 2 }, 5*time.Second, time.Millisecond)
		close(release)
		wg.Wait()
		for _, err := range errs {
			require.ErrorIs(t, err, wantErr)
		}
	})

	t.Run("deterministic request id per key", func(t *testing.T) {
		id := requestIDFromIdempotencyKey("abc")
		require.Equal(t, id, requestIDFromIdempotencyKey("abc"))
		require.NotEqual(t, id, requestIDFromIdempotencyKey("abd"))
		require.Len(t, id, 36)
	})
}
