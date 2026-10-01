package internal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	workflowservicemock "go.temporal.io/api/workflowservicemock/v1"

	ilog "go.temporal.io/sdk/internal/log"
)

var errDrainActivityCanceled = NewCanceledError()

// blockingActivityFixture is a fully controllable activity: it blocks until
// release is signaled (then returns nil), until its context is canceled (it
// returns a CanceledError, exactly as a real activity does on a server cancel),
// or, when cooperative, until the worker stop channel closes.
type blockingActivityFixture struct {
	started chan struct{}
	release chan struct{}

	// cooperative makes the activity observe the worker stop channel and return
	// a CanceledError (cooperative graceful shutdown path).
	cooperative bool
}

func newBlockingActivityFixture(cooperative bool) *blockingActivityFixture {
	return &blockingActivityFixture{
		started:     make(chan struct{}),
		release:     make(chan struct{}),
		cooperative: cooperative,
	}
}

func (f *blockingActivityFixture) run(ctx context.Context) error {
	select {
	case f.started <- struct{}{}:
	default:
	}
	if f.cooperative {
		select {
		case <-GetWorkerStopChannel(ctx):
			return errDrainActivityCanceled
		case <-f.release:
			return nil
		case <-ctx.Done():
			return errDrainActivityCanceled
		}
	}
	select {
	case <-f.release:
		return nil
	case <-ctx.Done():
		// A server-requested activity cancellation cancels the context with a
		// CanceledError cause; mirror that so the worker sends a canceled
		// callback rather than a failed one.
		return errDrainActivityCanceled
	}
}

// drainTestHarness wires a real activity task handler to a gomock workflow
// service and runs a single activity worker with a controllable stop timeout.
type drainTestHarness struct {
	t               *testing.T
	service         *workflowservicemock.MockWorkflowServiceClient
	worker          *activityWorker
	taskToken       []byte
	cancelCallbacks *activityCancellationCallbacks
}

func newDrainTestHarness(t *testing.T, stopTimeout time.Duration, act *blockingActivityFixture) *drainTestHarness {
	t.Helper()
	ctrl := gomock.NewController(t)
	svc := workflowservicemock.NewMockWorkflowServiceClient(ctrl)

	now := time.Now()
	token := []byte("drain-token-" + uuid.NewString())
	task := &workflowservice.PollActivityTaskQueueResponse{
		Attempt:   1,
		TaskToken: token,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "drain-wf",
			RunId:      "drain-run",
		},
		ActivityType:           &commonpb.ActivityType{Name: "BlockingActivity"},
		ActivityId:             uuid.NewString(),
		ScheduledTime:          timestamppb.New(now),
		StartedTime:            timestamppb.New(now),
		ScheduleToCloseTimeout: durationpb.New(time.Hour),
		StartToCloseTimeout:    durationpb.New(time.Hour),
		WorkflowType:           &commonpb.WorkflowType{Name: "drainWorkflowType"},
		WorkflowNamespace:      DefaultNamespace,
	}

	// Polls return the blocking activity task. Counts come from the drain
	// tracker, not from the number of poll responses.
	svc.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(task, nil).AnyTimes()
	svc.EXPECT().RespondActivityTaskCompleted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil).AnyTimes()
	svc.EXPECT().RespondActivityTaskCanceled(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskCanceledResponse{}, nil).AnyTimes()
	svc.EXPECT().RespondActivityTaskFailed(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskFailedResponse{}, nil).AnyTimes()

	tuner, err := NewFixedSizeTuner(FixedSizeTunerOptions{
		NumWorkflowSlots:      defaultMaxConcurrentTaskExecutionSize,
		NumActivitySlots:      10,
		NumLocalActivitySlots: defaultMaxConcurrentLocalActivityExecutionSize,
	})
	require.NoError(t, err)

	registry := newRegistry()
	registry.RegisterActivityWithOptions(act.run, RegisterActivityOptions{Name: "BlockingActivity"})

	cancelCallbacks := newActivityCancellationCallbacks()
	params := workerExecutionParameters{
		Namespace: DefaultNamespace,
		TaskQueue: "drain-task-queue",
		ActivityTaskPollerBehavior: NewPollerBehaviorSimpleMaximum(
			PollerBehaviorSimpleMaximumOptions{MaximumNumberOfPollers: 1},
		),
		Logger:                        ilog.NewNopLogger(),
		Tuner:                         tuner,
		WorkerStopTimeout:             stopTimeout,
		activityCancellationCallbacks: cancelCallbacks,
	}
	client := &WorkflowClient{workflowService: svc}
	w := newActivityWorker(client, params, nil, registry, nil)
	require.NoError(t, w.Start())

	return &drainTestHarness{
		t:               t,
		service:         svc,
		worker:          w,
		taskToken:       token,
		cancelCallbacks: cancelCallbacks,
	}
}

// cancelActivity requests cancellation of the in-flight activity exactly the
// way a server cancel does (via the shared cancellation callback registry).
func (h *drainTestHarness) cancelActivity() bool {
	return h.cancelCallbacks.cancel(h.taskToken)
}

// TestStopDrain_GracefulRelease verifies that an in-flight activity that
// completes after Stop() but before the timeout is counted as completed.
func TestStopDrain_GracefulRelease(t *testing.T) {
	fixture := newBlockingActivityFixture(false)
	h := newDrainTestHarness(t, 5*time.Second, fixture)

	<-fixture.started

	stopDone := make(chan struct{})
	go func() {
		h.worker.Stop()
		close(stopDone)
	}()

	// Let the drain begin, then release the activity so it completes within
	// the stop timeout.
	time.Sleep(100 * time.Millisecond)
	close(fixture.release)

	select {
	case <-stopDone:
	case <-time.After(6 * time.Second):
		t.Fatal("worker Stop() did not return")
	}

	report := h.worker.worker.stopDrainReport()
	assert.True(t, report.Graceful, "report: %+v", report)
	assert.Equal(t, 1, report.Completed, "report: %+v", report)
	assert.Equal(t, 0, report.Canceled, "report: %+v", report)
	assert.Equal(t, 0, report.Failed, "report: %+v", report)
	assert.Equal(t, 0, report.TimedOut, "report: %+v", report)
}

// TestStopDrain_CanceledOnStop verifies that an in-flight activity whose
// context is canceled during the drain (a server cancel arriving while the
// worker stops) reports a canceled callback and is counted as canceled.
func TestStopDrain_CanceledOnStop(t *testing.T) {
	fixture := newBlockingActivityFixture(false)
	h := newDrainTestHarness(t, 5*time.Second, fixture)

	<-fixture.started

	stopDone := make(chan struct{})
	go func() {
		h.worker.Stop()
		close(stopDone)
	}()

	// Let the drain begin, then deliver a server cancel so the activity returns
	// a canceled result while the worker is draining.
	time.Sleep(100 * time.Millisecond)
	require.True(t, h.cancelActivity(), "activity cancel callback must be registered")

	select {
	case <-stopDone:
	case <-time.After(6 * time.Second):
		t.Fatal("worker Stop() did not return")
	}

	report := h.worker.worker.stopDrainReport()
	assert.True(t, report.Graceful, "report: %+v", report)
	assert.Equal(t, 0, report.Completed, "report: %+v", report)
	assert.Equal(t, 1, report.Canceled, "report: %+v", report)
	assert.Equal(t, 0, report.Failed, "report: %+v", report)
	assert.Equal(t, 0, report.TimedOut, "report: %+v", report)
}

// TestStopDrain_CooperativeStopChannel verifies that an activity which
// cooperatively watches the worker stop channel and returns a canceled error
// is drained without a timeout. Because its context is not server-canceled,
// the SDK reports it as failed (existing semantics), so the report stays
// consistent with the actual callback: Failed rather than Canceled.
func TestStopDrain_CooperativeStopChannel(t *testing.T) {
	fixture := newBlockingActivityFixture(true)
	h := newDrainTestHarness(t, 5*time.Second, fixture)

	<-fixture.started

	stopDone := make(chan struct{})
	go func() {
		h.worker.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(6 * time.Second):
		t.Fatal("worker Stop() did not return")
	}

	report := h.worker.worker.stopDrainReport()
	assert.True(t, report.Graceful, "report: %+v", report)
	assert.Equal(t, 0, report.Completed, "report: %+v", report)
	assert.Equal(t, 0, report.Canceled, "report: %+v", report)
	assert.Equal(t, 1, report.Failed, "report: %+v", report)
	assert.Equal(t, 0, report.TimedOut, "report: %+v", report)
}

// TestStopDrain_Timeout verifies that an activity that never releases is cut
// off by WorkerStopTimeout and counted as timed out.
func TestStopDrain_Timeout(t *testing.T) {
	fixture := newBlockingActivityFixture(false)
	h := newDrainTestHarness(t, 300*time.Millisecond, fixture)

	<-fixture.started

	start := time.Now()
	h.worker.Stop()
	elapsed := time.Since(start)

	report := h.worker.worker.stopDrainReport()
	assert.False(t, report.Graceful, "report: %+v", report)
	assert.Equal(t, 0, report.Completed, "report: %+v", report)
	assert.Equal(t, 0, report.Canceled, "report: %+v", report)
	assert.Equal(t, 0, report.Failed, "report: %+v", report)
	assert.Equal(t, 1, report.TimedOut, "report: %+v", report)

	// Stop must return around the timeout rather than waiting indefinitely.
	assert.Less(t, elapsed, 3*time.Second, "Stop took too long: %v", elapsed)

	// Release afterwards so the blocked goroutine can wind down.
	close(fixture.release)
}

// TestStopDrain_RepeatedStopNoDoubleCount verifies that calling Stop more than
// once does not panic, does not recount, and returns the same report.
func TestStopDrain_RepeatedStopNoDoubleCount(t *testing.T) {
	fixture := newBlockingActivityFixture(false)
	h := newDrainTestHarness(t, 5*time.Second, fixture)

	<-fixture.started

	stopDone := make(chan struct{})
	go func() {
		h.worker.Stop()
		close(stopDone)
	}()

	// Deliver a server cancel so the first drain settles with exactly one
	// canceled task.
	time.Sleep(100 * time.Millisecond)
	require.True(t, h.cancelActivity())
	<-stopDone

	first := h.worker.worker.stopDrainReport()
	require.Equal(t, 1, first.Canceled, "report: %+v", first)

	// Repeated stops must be no-ops and never close stop channels twice.
	assert.NotPanics(t, func() {
		h.worker.Stop()
		h.worker.Stop()
	})

	second := h.worker.worker.stopDrainReport()
	assert.Equal(t, first, second)
	assert.Equal(t, 1, second.Canceled, "report: %+v", second)
	assert.Equal(t, 0, second.Completed, "report: %+v", second)
	assert.Equal(t, 0, second.TimedOut, "report: %+v", second)
}

// TestStopDrain_AggregatedWorkerCallback verifies the public-style path: an
// AggregatedWorker invokes OnWorkerStopDrainReport exactly once, even when
// Stop() is called repeatedly, and with a zero-drain report when no tasks were
// in flight.
func TestStopDrain_AggregatedWorkerCallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	svc.EXPECT().GetSystemInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.GetSystemInfoResponse{}, nil).AnyTimes()
	svc.EXPECT().DescribeNamespace(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&workflowservice.DescribeNamespaceResponse{
			NamespaceInfo: &namespacepb.NamespaceInfo{
				Name:  DefaultNamespace,
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
		}, nil).AnyTimes()
	svc.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.PollActivityTaskQueueResponse{}, nil).AnyTimes()
	svc.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.PollWorkflowTaskQueueResponse{}, nil).AnyTimes()
	svc.EXPECT().RespondActivityTaskCompleted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil).AnyTimes()
	svc.EXPECT().RespondWorkflowTaskCompleted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).AnyTimes()
	svc.EXPECT().ShutdownWorker(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.ShutdownWorkerResponse{}, nil).AnyTimes()

	client := NewServiceClient(svc, nil, ClientOptions{Namespace: DefaultNamespace})

	var cbCount atomic.Int32
	var got WorkerStopDrainReport
	w := NewAggregatedWorker(client, "drain-agg-tq", WorkerOptions{
		WorkerStopTimeout: 2 * time.Second,
		OnWorkerStopDrainReport: func(r WorkerStopDrainReport) {
			cbCount.Add(1)
			got = r
		},
	})
	require.NoError(t, w.Start())

	// No in-flight tasks: graceful zero report.
	w.Stop()
	assert.Equal(t, int32(1), cbCount.Load(), "callback must fire exactly once")
	assert.True(t, got.Graceful)
	assert.Zero(t, got.Completed)
	assert.Zero(t, got.Canceled)
	assert.Zero(t, got.Failed)
	assert.Zero(t, got.TimedOut)

	// Repeated stops must not re-invoke the callback.
	w.Stop()
	w.Stop()
	assert.Equal(t, int32(1), cbCount.Load(), "repeated Stop must not re-report")
}

// TestStopDrainTracker_Unit pins down tracker edge cases: pre-drain
// completions are ignored, drain completions count once, and finish is
// idempotent with leftovers counted as timed out.
func TestStopDrainTracker_Unit(t *testing.T) {
	tr := newWorkerDrainTracker()

	// A task completing before drain begins is never counted.
	tr.taskStarted("early")
	tr.taskFinished("early", DrainOutcomeCompleted)

	tr.beginDrain()
	tr.beginDrain() // repeated begin is a no-op

	tr.taskStarted("inflight")
	tr.taskStarted("late")
	tr.taskFinished("late", DrainOutcomeCompleted)
	tr.taskFinished("late", DrainOutcomeCompleted) // double-finish no double-count
	tr.taskFinished("inflight", DrainOutcomeCanceled)

	r1 := tr.finishDrain(false)
	assert.Equal(t, 1, r1.Completed)
	assert.Equal(t, 1, r1.Canceled)
	assert.Equal(t, 0, r1.Failed)
	assert.Equal(t, 0, r1.TimedOut)
	assert.False(t, r1.Graceful)

	tr2 := newWorkerDrainTracker()
	tr2.beginDrain()
	tr2.taskStarted("stuck")
	r2 := tr2.finishDrain(false)
	assert.Equal(t, 1, r2.TimedOut)
	r3 := tr2.finishDrain(false)
	assert.Equal(t, r2, r3, "repeated finish must return the same report")
}
