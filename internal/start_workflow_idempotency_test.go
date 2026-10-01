package internal

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeWorkflowService is a controllable in-process WorkflowService used to
// exercise the client start idempotency handling without a real server.
type fakeWorkflowService struct {
	workflowservice.UnimplementedWorkflowServiceServer

	mu            sync.Mutex
	startRequests []*workflowservice.StartWorkflowExecutionRequest

	// failStarts transient start attempts to fail with an Unavailable status
	// before succeeding. This simulates a timeout/restart that the local gRPC
	// retry should recover from.
	failStarts int

	// blockStart, when non-nil, blocks the first start attempt until either the
	// channel is closed or the attempt context is cancelled.
	blockStart chan struct{}
	blocked    chan struct{} // closed once the first attempt is blocking
	blockOnce  sync.Once
}

func (s *fakeWorkflowService) StartWorkflowExecution(
	ctx context.Context,
	req *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	s.mu.Lock()
	s.startRequests = append(s.startRequests, req)
	fail := s.failStarts > 0
	if fail {
		s.failStarts--
	}
	block := s.blockStart != nil
	s.mu.Unlock()

	if block {
		s.blockOnce.Do(func() { close(s.blocked) })
		select {
		case <-s.blockStart:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}

	if fail {
		return nil, status.Error(codes.Unavailable, "transient unavailable")
	}
	return &workflowservice.StartWorkflowExecutionResponse{
		RunId:              "run-" + req.WorkflowId,
		FirstExecutionRunId: "run-" + req.WorkflowId,
	}, nil
}

func (s *fakeWorkflowService) GetSystemInfo(
	_ context.Context,
	_ *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{
		Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{},
	}, nil
}

func (s *fakeWorkflowService) requests() []*workflowservice.StartWorkflowExecutionRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*workflowservice.StartWorkflowExecutionRequest, len(s.startRequests))
	copy(out, s.startRequests)
	return out
}

// newIdempotencyTestClient starts a fake gRPC server on an in-process listener
// and returns a WorkflowClient connected to it with the SDK interceptors
// (including the local retry interceptor) wired in.
func newIdempotencyTestClient(t *testing.T, svc *fakeWorkflowService) *WorkflowClient {
	t.Helper()

	listener := bufconn.Listen(4 * 1024 * 1024)
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, svc)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	opts := ClientOptions{Namespace: "test-namespace"}
	var excludeInternal atomic.Bool
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(requiredInterceptors(&opts, &excludeInternal)...),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := NewServiceClient(workflowservice.NewWorkflowServiceClient(conn), conn, opts)
	// Pre-set capabilities so ensureInitialized does not issue a call.
	client.capabilities = &workflowservice.GetSystemInfoResponse_Capabilities{}
	return client
}

func startOptions(key, workflowID string) StartWorkflowOptions {
	return StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                "test-task-queue",
		IdempotencyKey:           key,
		WorkflowExecutionTimeout: time.Minute,
	}
}

// TestStartWorkflowIdempotencyKeyRetryReusesRequestID verifies that when the
// first start attempt fails transiently (simulated timeout/restart) and the
// local gRPC retry succeeds, both attempts carry the same request identifier
// derived from the idempotency key.
func TestStartWorkflowIdempotencyKeyRetryReusesRequestID(t *testing.T) {
	svc := &fakeWorkflowService{failStarts: 1}
	client := newIdempotencyTestClient(t, svc)

	run, err := client.ExecuteWorkflow(
		t.Context(),
		startOptions("retry-key", "wf-retry"),
		"test-workflow",
	)
	require.NoError(t, err)
	require.Equal(t, "wf-retry", run.GetID())

	reqs := svc.requests()
	require.Len(t, reqs, 2, "expected the transient failure to be retried once")
	require.Equal(t, reqs[0].RequestId, reqs[1].RequestId,
		"local retries must reuse the same request identifier")
	require.Equal(t, idempotencyKeyToRequestID("retry-key"), reqs[0].RequestId,
		"request identifier must be derived from the idempotency key")
}

// TestStartWorkflowIdempotencyKeyConcurrentSameKeyMerged verifies that
// concurrent start calls sharing the same idempotency key are merged into a
// single request and all receive the same result.
func TestStartWorkflowIdempotencyKeyConcurrentSameKeyMerged(t *testing.T) {
	svc := &fakeWorkflowService{}
	client := newIdempotencyTestClient(t, svc)

	const n = 8
	var wg sync.WaitGroup
	runs := make([]WorkflowRun, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runs[i], errs[i] = client.ExecuteWorkflow(
				t.Context(),
				startOptions("shared-key", "wf-shared"),
				"test-workflow",
			)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.Equal(t, "wf-shared", runs[i].GetID())
	}

	reqs := svc.requests()
	require.Len(t, reqs, 1, "concurrent calls with the same key must be merged into one request")
	require.Equal(t, idempotencyKeyToRequestID("shared-key"), reqs[0].RequestId)

	// After the merged call completes, the pending state must be dropped: a
	// later retry with the same key issues a fresh request rather than being
	// merged into the completed one.
	run, err := client.ExecuteWorkflow(
		t.Context(),
		startOptions("shared-key", "wf-shared"),
		"test-workflow",
	)
	require.NoError(t, err)
	require.Equal(t, "wf-shared", run.GetID())
	require.Len(t, svc.requests(), 2, "retry after completion must issue a new request")
}

// TestStartWorkflowIdempotencyKeyDifferentKeysNotMerged verifies that
// concurrent start calls with different idempotency keys are not merged and
// carry distinct request identifiers.
func TestStartWorkflowIdempotencyKeyDifferentKeysNotMerged(t *testing.T) {
	svc := &fakeWorkflowService{}
	client := newIdempotencyTestClient(t, svc)

	var wg sync.WaitGroup
	for _, key := range []string{"key-a", "key-b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			_, err := client.ExecuteWorkflow(
				t.Context(),
				startOptions(key, "wf-"+key),
				"test-workflow",
			)
			require.NoError(t, err)
		}(key)
	}
	wg.Wait()

	reqs := svc.requests()
	require.Len(t, reqs, 2, "calls with different keys must not be merged")
	require.NotEqual(t, reqs[0].RequestId, reqs[1].RequestId,
		"different keys must produce different request identifiers")
	requestIDFor := func(workflowID string) string {
		for _, req := range reqs {
			if req.WorkflowId == workflowID {
				return req.RequestId
			}
		}
		t.Fatalf("no request found for workflow %q", workflowID)
		return ""
	}
	require.Equal(t, idempotencyKeyToRequestID("key-a"), requestIDFor("wf-key-a"))
	require.Equal(t, idempotencyKeyToRequestID("key-b"), requestIDFor("wf-key-b"))
}

// TestStartWorkflowIdempotencyKeyCancelDropsPendingState verifies that after a
// start call is cancelled, its pending state is not retained: a subsequent
// retry with the same key issues a fresh request instead of waiting on the
// cancelled attempt.
func TestStartWorkflowIdempotencyKeyCancelDropsPendingState(t *testing.T) {
	blockStart := make(chan struct{})
	svc := &fakeWorkflowService{
		blockStart: blockStart,
		blocked:    make(chan struct{}),
	}
	client := newIdempotencyTestClient(t, svc)

	ctx1, cancel1 := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, err := client.ExecuteWorkflow(
			ctx1,
			startOptions("cancel-key", "wf-cancel"),
			"test-workflow",
		)
		errCh <- err
	}()

	// Wait until the first attempt is in flight, then cancel it.
	<-svc.blocked
	cancel1()
	select {
	case err := <-errCh:
		// The gRPC layer reports the cancellation as a service error whose
		// message carries the context cancellation reason.
		require.ErrorContains(t, err, context.Canceled.Error())
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled start call did not return")
	}

	// Release the handler in case it is still waiting on the channel.
	close(blockStart)

	// A retry with the same key must issue a fresh request and succeed.
	run, err := client.ExecuteWorkflow(
		t.Context(),
		startOptions("cancel-key", "wf-cancel"),
		"test-workflow",
	)
	require.NoError(t, err)
	require.Equal(t, "wf-cancel", run.GetID())

	reqs := svc.requests()
	require.Len(t, reqs, 2, "retry after cancellation must issue a new request")
	require.Equal(t, reqs[0].RequestId, reqs[1].RequestId,
		"the fresh request still carries the same key-derived identifier")
}
