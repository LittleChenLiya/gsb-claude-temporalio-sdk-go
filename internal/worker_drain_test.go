package internal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// blockingActivity is a controllable activity that blocks until released.
type blockingActivity struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingActivity() *blockingActivity {
	return &blockingActivity{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (a *blockingActivity) Execute(_ context.Context) (string, error) {
	a.once.Do(func() { close(a.started) })
	<-a.release
	return "done", nil
}

func (a *blockingActivity) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-a.started:
	case <-time.After(5 * time.Second):
		t.Fatal("activity did not start")
	}
}

func (a *blockingActivity) Release() {
	close(a.release)
}

// newDrainTestWorker creates an AggregatedWorker with a mock service that
// returns a single activity task on poll. The worker is configured with the
// given stop timeout.
func newDrainTestWorker(
	t *testing.T,
	ctrl *gomock.Controller,
	stopTimeout time.Duration,
	onDrain func(WorkerDrainReport),
) (*AggregatedWorker, *blockingActivity) {
	t.Helper()

	service := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	service.EXPECT().GetSystemInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.GetSystemInfoResponse{}, nil).AnyTimes()
	service.EXPECT().DescribeNamespace(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.DescribeNamespaceResponse{}, nil).AnyTimes()
	service.EXPECT().ShutdownWorker(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.ShutdownWorkerResponse{}, nil).AnyTimes()

	activity := newBlockingActivity()

	// Return a single activity task, then block on subsequent polls to simulate
	// a long poll.
	pollCount := 0
	service.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *workflowservice.PollActivityTaskQueueRequest, _ ...interface{}) (*workflowservice.PollActivityTaskQueueResponse, error) {
			pollCount++
			if pollCount == 1 {
				return &workflowservice.PollActivityTaskQueueResponse{
					TaskToken:              []byte("task-token-1"),
					ActivityId:             "activity-1",
					ActivityType:           &commonpb.ActivityType{Name: "blockingActivity"},

					WorkflowType:           &commonpb.WorkflowType{Name: "test-workflow"},
					WorkflowExecution:      &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"},
					ScheduledTime:          timestamppb.Now(),
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
					StartToCloseTimeout:    durationpb.New(time.Hour),
					HeartbeatTimeout:       nil,
					Attempt:                1,
				}, nil
			}
			<-time.After(10 * time.Second)
			return &workflowservice.PollActivityTaskQueueResponse{}, nil
		}).AnyTimes()

	service.EXPECT().RespondActivityTaskCompleted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil).AnyTimes()
	service.EXPECT().RespondActivityTaskFailed(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskFailedResponse{}, nil).AnyTimes()
	service.EXPECT().RespondActivityTaskCanceled(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskCanceledResponse{}, nil).AnyTimes()

	client := NewServiceClient(service, nil, ClientOptions{Namespace: "test-namespace"})
	worker := NewAggregatedWorker(client, "test-drain-tq", WorkerOptions{
		WorkerStopTimeout:     stopTimeout,
		OnWorkerDrainReport:   onDrain,
		DisableWorkflowWorker: true,
	})
	worker.RegisterActivityWithOptions(activity.Execute, RegisterActivityOptions{Name: "blockingActivity"})

	return worker, activity
}

// TestWorkerDrainReport_GracefulStop verifies that an activity which completes
// during the drain window is counted as Completed.
func TestWorkerDrainReport_GracefulStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var report WorkerDrainReport
	var reportCalled bool
	worker, activity := newDrainTestWorker(t, ctrl, 5*time.Second, func(r WorkerDrainReport) {
		report = r
		reportCalled = true
	})

	require.NoError(t, worker.Start())
	activity.waitStarted(t)

	// Release the activity before stopping, so it completes during drain.
	activity.Release()
	worker.Stop()

	assert.True(t, reportCalled, "drain report callback should be invoked")
	assert.Equal(t, int64(1), report.Completed, "activity that completes during drain should be counted as completed")
	assert.Equal(t, int64(0), report.Canceled)
	assert.Equal(t, int64(0), report.TimedOut)
}

// TestWorkerDrainReport_Timeout verifies that an activity still running when
// the stop timeout elapses is counted as TimedOut.
func TestWorkerDrainReport_Timeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var report WorkerDrainReport
	var reportCalled bool
	worker, activity := newDrainTestWorker(t, ctrl, 200*time.Millisecond, func(r WorkerDrainReport) {
		report = r
		reportCalled = true
	})

	require.NoError(t, worker.Start())
	activity.waitStarted(t)

	// Do NOT release the activity; the stop timeout should elapse.
	worker.Stop()

	assert.True(t, reportCalled, "drain report callback should be invoked")
	assert.Equal(t, int64(0), report.Completed)
	assert.Equal(t, int64(0), report.Canceled)
	assert.Equal(t, int64(1), report.TimedOut, "activity still running at stop timeout should be counted as timed out")
}

// TestWorkerDrainReport_RepeatedStop verifies that calling Stop multiple times
// does not double-count the drain report.
func TestWorkerDrainReport_RepeatedStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var report WorkerDrainReport
	var callCount int
	worker, activity := newDrainTestWorker(t, ctrl, 5*time.Second, func(r WorkerDrainReport) {
		report = r
		callCount++
	})

	require.NoError(t, worker.Start())
	activity.waitStarted(t)
	activity.Release()

	// Call Stop multiple times.
	worker.Stop()
	worker.Stop()
	worker.Stop()

	assert.Equal(t, 1, callCount, "drain report callback should be invoked exactly once")
	assert.Equal(t, int64(1), report.Completed, "repeated Stop must not double-count")
	assert.Equal(t, int64(0), report.Canceled)
	assert.Equal(t, int64(0), report.TimedOut)
}

// TestWorkerDrainReport_NilCallback verifies that a nil callback does not panic.
func TestWorkerDrainReport_NilCallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	worker, activity := newDrainTestWorker(t, ctrl, 5*time.Second, nil)

	require.NoError(t, worker.Start())
	activity.waitStarted(t)
	activity.Release()

	// Should not panic with nil callback.
	worker.Stop()
}

// TestWorkerDrainReport_MixedOutcomes verifies that multiple activities with
// different outcomes (one completes, one times out) are counted correctly in
// a single drain report.
func TestWorkerDrainReport_MixedOutcomes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	service.EXPECT().GetSystemInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.GetSystemInfoResponse{}, nil).AnyTimes()
	service.EXPECT().DescribeNamespace(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.DescribeNamespaceResponse{}, nil).AnyTimes()
	service.EXPECT().ShutdownWorker(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.ShutdownWorkerResponse{}, nil).AnyTimes()

	activity1 := newBlockingActivity()
	activity2 := newBlockingActivity()

	// Return two activity tasks with different types, then block on subsequent polls.
	pollCount := 0
	service.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *workflowservice.PollActivityTaskQueueRequest, _ ...interface{}) (*workflowservice.PollActivityTaskQueueResponse, error) {
			pollCount++
			if pollCount == 1 {
				return &workflowservice.PollActivityTaskQueueResponse{
					TaskToken:              []byte("task-token-1"),
					ActivityId:             "activity-1",
					ActivityType:           &commonpb.ActivityType{Name: "blockingActivity1"},
					WorkflowType:           &commonpb.WorkflowType{Name: "test-workflow"},
					WorkflowExecution:      &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"},
					ScheduledTime:          timestamppb.Now(),
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
					StartToCloseTimeout:    durationpb.New(time.Hour),
					HeartbeatTimeout:       nil,
					Attempt:                1,
				}, nil
			}
			if pollCount == 2 {
				return &workflowservice.PollActivityTaskQueueResponse{
					TaskToken:              []byte("task-token-2"),
					ActivityId:             "activity-2",
					ActivityType:           &commonpb.ActivityType{Name: "blockingActivity2"},
					WorkflowType:           &commonpb.WorkflowType{Name: "test-workflow"},
					WorkflowExecution:      &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"},
					ScheduledTime:          timestamppb.Now(),
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
					StartToCloseTimeout:    durationpb.New(time.Hour),
					HeartbeatTimeout:       nil,
					Attempt:                1,
				}, nil
			}
			<-time.After(10 * time.Second)
			return &workflowservice.PollActivityTaskQueueResponse{}, nil
		}).AnyTimes()

	service.EXPECT().RespondActivityTaskCompleted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil).AnyTimes()
	service.EXPECT().RespondActivityTaskFailed(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskFailedResponse{}, nil).AnyTimes()
	service.EXPECT().RespondActivityTaskCanceled(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RespondActivityTaskCanceledResponse{}, nil).AnyTimes()

	client := NewServiceClient(service, nil, ClientOptions{Namespace: "test-namespace"})

	var report WorkerDrainReport
	var reportCalled bool
	worker := NewAggregatedWorker(client, "test-drain-tq", WorkerOptions{
		WorkerStopTimeout: 200 * time.Millisecond,
		OnWorkerDrainReport: func(r WorkerDrainReport) {
			report = r
			reportCalled = true
		},
		DisableWorkflowWorker: true,
	})
	worker.RegisterActivityWithOptions(activity1.Execute, RegisterActivityOptions{Name: "blockingActivity1"})
	worker.RegisterActivityWithOptions(activity2.Execute, RegisterActivityOptions{Name: "blockingActivity2"})

	require.NoError(t, worker.Start())
	activity1.waitStarted(t)
	activity2.waitStarted(t)

	// Release activity1 so it completes during drain; keep activity2 blocked
	// so it times out.
	activity1.Release()
	worker.Stop()

	assert.True(t, reportCalled, "drain report callback should be invoked")
	assert.Equal(t, int64(1), report.Completed, "activity1 should be counted as completed")
	assert.Equal(t, int64(0), report.Canceled)
	assert.Equal(t, int64(1), report.TimedOut, "activity2 should be counted as timed out")
}
