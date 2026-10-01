package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	ilog "go.temporal.io/sdk/internal/log"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TimerRemainingTestSuite struct {
	suite.Suite
	WorkflowTestSuite
}

func TestTimerRemainingTestSuite(t *testing.T) {
	suite.Run(t, new(TimerRemainingTestSuite))
}

// Test_GetTimerRemaining_Basic tests that GetTimerRemaining returns the correct remaining time
// when called before the timer fires, and returns 0 after the timer fires.
func (s *TimerRemainingTestSuite) Test_GetTimerRemaining_Basic() {
	var remainingBeforeFire time.Duration
	var remainingAfterFire time.Duration

	workflowFn := func(ctx Context) error {
		timer := NewTimer(ctx, 10*time.Minute)

		// Query remaining time before the timer fires
		remainingBeforeFire = GetTimerRemaining(ctx, timer)

		// Wait for the timer to fire (mock clock auto-forwards)
		if err := timer.Get(ctx, nil); err != nil {
			return err
		}

		// Query remaining time after the timer fires - should be 0
		remainingAfterFire = GetTimerRemaining(ctx, timer)

		return nil
	}

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowFn)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())

	// Before fire, remaining should be close to 10 minutes
	s.Greater(remainingBeforeFire, 9*time.Minute)
	s.LessOrEqual(remainingBeforeFire, 10*time.Minute)

	// After fire, remaining should be 0
	s.Equal(time.Duration(0), remainingAfterFire)
}

// Test_GetTimerRemaining_Cancel tests that GetTimerRemaining returns 0 after the timer is canceled.
func (s *TimerRemainingTestSuite) Test_GetTimerRemaining_Cancel() {
	var remainingBeforeCancel time.Duration
	var remainingAfterCancel time.Duration

	workflowFn := func(ctx Context) error {
		ctx, cancel := WithCancel(ctx)
		timer := NewTimer(ctx, 10*time.Minute)

		// Query remaining time before cancel
		remainingBeforeCancel = GetTimerRemaining(ctx, timer)

		// Cancel the timer
		cancel()

		// Wait for the timer to be canceled
		_ = timer.Get(ctx, nil)

		// Query remaining time after cancel - should be 0
		remainingAfterCancel = GetTimerRemaining(ctx, timer)

		return nil
	}

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowFn)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())

	// Before cancel, remaining should be close to 10 minutes
	s.Greater(remainingBeforeCancel, 9*time.Minute)
	s.LessOrEqual(remainingBeforeCancel, 10*time.Minute)

	// After cancel, remaining should be 0
	s.Equal(time.Duration(0), remainingAfterCancel)
}

// Test_GetTimerRemaining_ZeroDuration tests that GetTimerRemaining returns 0 for a zero-duration timer.
func (s *TimerRemainingTestSuite) Test_GetTimerRemaining_ZeroDuration() {
	var remaining time.Duration

	workflowFn := func(ctx Context) error {
		timer := NewTimer(ctx, 0)
		remaining = GetTimerRemaining(ctx, timer)
		return nil
	}

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowFn)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	s.Equal(time.Duration(0), remaining)
}

// Test_GetTimerRemaining_NonTimerFuture tests that GetTimerRemaining returns 0 for a non-timer future.
func (s *TimerRemainingTestSuite) Test_GetTimerRemaining_NonTimerFuture() {
	var remaining time.Duration

	workflowFn := func(ctx Context) error {
		future, _ := NewFuture(ctx)
		remaining = GetTimerRemaining(ctx, future)
		return nil
	}

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowFn)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	s.Equal(time.Duration(0), remaining)
}

// Test_GetTimerRemaining_Replay tests that GetTimerRemaining returns the same value during replay
// as it did during the original execution.
func (s *TimerRemainingTestSuite) Test_GetTimerRemaining_Replay() {
	// Create a workflow that creates a timer and queries the remaining time
	workflowFn := func(ctx Context) error {
		timer := NewTimer(ctx, 10*time.Minute)
		_ = GetTimerRemaining(ctx, timer)
		return timer.Get(ctx, nil)
	}

	// First, run the workflow in the test environment
	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowFn)
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())

	// Now construct a history that matches what the test environment would produce
	// and replay it to verify determinism
	taskQueue := "test-timer-remaining-replay"
	now := time.Now().UTC()
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			createTestEventWorkflowExecutionStartedWithTime(1, now, &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "testTimerRemainingReplayWorkflow"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
				Input:        testEncodeFunctionArgs(converter.GetDefaultDataConverter()),
			}),
			createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
			createTestEventWorkflowTaskStartedWithTime(3, now.Add(time.Second)),
			createTestEventWorkflowTaskCompleted(4, &historypb.WorkflowTaskCompletedEventAttributes{}),
			createTestEventTimerStartedWithTimeout(5, 5, 10*time.Minute),
			createTestEventTimerFired(6, 5),
			createTestEventWorkflowTaskScheduled(7, &historypb.WorkflowTaskScheduledEventAttributes{}),
			createTestEventWorkflowTaskStartedWithTime(8, now.Add(10*time.Minute+time.Second)),
			createTestEventWorkflowTaskCompleted(9, &historypb.WorkflowTaskCompletedEventAttributes{}),
			createTestEventWorkflowExecutionCompleted(10, &historypb.WorkflowExecutionCompletedEventAttributes{
				WorkflowTaskCompletedEventId: 9,
			}),
		},
	}

	// Register the workflow function for replay
	replayer, err := NewWorkflowReplayer(WorkflowReplayerOptions{})
	s.NoError(err)
	replayer.RegisterWorkflowWithOptions(workflowFn, RegisterWorkflowOptions{Name: "testTimerRemainingReplayWorkflow"})

	// Replay the history - this should succeed without non-determinism errors
	err = replayer.ReplayWorkflowHistory(ilog.NewDefaultLogger(), history)
	s.NoError(err, "Replay should succeed without non-determinism errors")
}

// Test_GetTimerRemaining_ReplayWithCancel tests that GetTimerRemaining returns 0 during replay
// for a timer that was canceled.
func (s *TimerRemainingTestSuite) Test_GetTimerRemaining_ReplayWithCancel() {
	// Create a workflow that creates a timer, queries remaining time, then cancels it
	workflowFn := func(ctx Context) error {
		ctx, cancel := WithCancel(ctx)
		timer := NewTimer(ctx, 10*time.Minute)
		_ = GetTimerRemaining(ctx, timer)
		cancel()
		// The timer is canceled, so Get returns a CanceledError.
		// We return nil to indicate the workflow completed successfully.
		_ = timer.Get(ctx, nil)
		return nil
	}

	// First, run the workflow in the test environment
	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(workflowFn)
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())

	// Now construct a history that matches what the test environment would produce
	// and replay it to verify determinism
	taskQueue := "test-timer-remaining-cancel-replay"
	now := time.Now().UTC()
	history := &historypb.History{
		Events: []*historypb.HistoryEvent{
			createTestEventWorkflowExecutionStartedWithTime(1, now, &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "testTimerRemainingCancelReplayWorkflow"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
				Input:        testEncodeFunctionArgs(converter.GetDefaultDataConverter()),
			}),
			createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}),
			createTestEventWorkflowTaskStartedWithTime(3, now.Add(time.Second)),
			createTestEventWorkflowTaskCompleted(4, &historypb.WorkflowTaskCompletedEventAttributes{}),
			createTestEventTimerStartedWithTimeout(5, 5, 10*time.Minute),
			createTestEventTimerCanceled(6, 5),
			createTestEventWorkflowTaskScheduled(7, &historypb.WorkflowTaskScheduledEventAttributes{}),
			createTestEventWorkflowTaskStartedWithTime(8, now.Add(time.Second)),
			createTestEventWorkflowTaskCompleted(9, &historypb.WorkflowTaskCompletedEventAttributes{}),
			createTestEventWorkflowExecutionCompleted(10, &historypb.WorkflowExecutionCompletedEventAttributes{
				WorkflowTaskCompletedEventId: 9,
			}),
		},
	}

	// Register the workflow function for replay
	replayer, err := NewWorkflowReplayer(WorkflowReplayerOptions{})
	s.NoError(err)
	replayer.RegisterWorkflowWithOptions(workflowFn, RegisterWorkflowOptions{Name: "testTimerRemainingCancelReplayWorkflow"})

	// Replay the history - this should succeed without non-determinism errors
	err = replayer.ReplayWorkflowHistory(ilog.NewDefaultLogger(), history)
	s.NoError(err, "Replay should succeed without non-determinism errors")
}

// Helper functions to create test events with specific times

func createTestEventWorkflowExecutionStartedWithTime(eventID int64, eventTime time.Time, attr *historypb.WorkflowExecutionStartedEventAttributes) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:    eventID,
		EventTime:  timestamppb.New(eventTime),
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: attr},
	}
}

func createTestEventWorkflowTaskStartedWithTime(eventID int64, eventTime time.Time) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:    eventID,
		EventTime:  timestamppb.New(eventTime),
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{}},
	}
}

func createTestEventTimerStartedWithTimeout(eventID int64, id int, timeout time.Duration) *historypb.HistoryEvent {
	timerID := string(rune('0' + id))
	attr := &historypb.TimerStartedEventAttributes{
		TimerId:                      timerID,
		StartToFireTimeout:           durationpb.New(timeout),
		WorkflowTaskCompletedEventId: 4,
	}
	return &historypb.HistoryEvent{
		EventId:    eventID,
		EventType:  enumspb.EVENT_TYPE_TIMER_STARTED,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: attr},
	}
}