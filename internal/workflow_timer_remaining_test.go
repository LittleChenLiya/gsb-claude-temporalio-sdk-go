package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// timerRemainingTestWorkflow records the remaining time of a 1 hour timer at three points:
// right after creation, after sleeping 25 minutes of workflow time, and after the timer fired.
func timerRemainingTestWorkflow(ctx Context) ([]time.Duration, error) {
	results := make([]time.Duration, 0, 3)
	timer := NewTimerHandle(ctx, time.Hour)
	results = append(results, timer.RemainingTime())
	if err := Sleep(ctx, 25*time.Minute); err != nil {
		return nil, err
	}
	results = append(results, timer.RemainingTime())
	if err := timer.Get(ctx, nil); err != nil {
		return nil, err
	}
	results = append(results, timer.RemainingTime())
	return results, nil
}

// TestTimerRemainingTime_WorkflowTime advances virtual time in the test environment and verifies the
// remaining time of a workflow timer, including that a fired timer reports zero.
func TestTimerRemainingTime_WorkflowTime(t *testing.T) {
	var suite WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(timerRemainingTestWorkflow)
	env.ExecuteWorkflow(timerRemainingTestWorkflow)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var results []time.Duration
	require.NoError(t, env.GetWorkflowResult(&results))
	require.Equal(t, []time.Duration{time.Hour, 35 * time.Minute, 0}, results)
}

// timerRemainingCancelTestWorkflow cancels a pending timer and records the remaining time before and after.
func timerRemainingCancelTestWorkflow(ctx Context) ([]time.Duration, error) {
	timerCtx, cancel := WithCancel(ctx)
	timer := NewTimerHandle(timerCtx, time.Hour)
	before := timer.RemainingTime()
	cancel()
	err := timer.Get(timerCtx, nil)
	if !IsCanceledError(err) {
		return nil, fmt.Errorf("expected CanceledError from canceled timer, got: %v", err)
	}
	after := timer.RemainingTime()
	return []time.Duration{before, after}, nil
}

// TestTimerRemainingTime_Canceled verifies that a canceled timer reports zero remaining time and that
// its future resolves with a CanceledError.
func TestTimerRemainingTime_Canceled(t *testing.T) {
	var suite WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(timerRemainingCancelTestWorkflow)
	env.ExecuteWorkflow(timerRemainingCancelTestWorkflow)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var results []time.Duration
	require.NoError(t, env.GetWorkflowResult(&results))
	require.Equal(t, []time.Duration{time.Hour, 0}, results)
}

// timerRemainingQueryWorkflow exposes the remaining time of its 1 hour timer through a query handler so
// callers can decide whether to keep waiting before canceling.
func timerRemainingQueryWorkflow(ctx Context) error {
	timer := NewTimerHandle(ctx, time.Hour)
	if err := SetQueryHandler(ctx, "timer-remaining", func() (time.Duration, error) {
		return timer.RemainingTime(), nil
	}); err != nil {
		return err
	}
	return timer.Get(ctx, nil)
}

// TestTimerRemainingTime_Query queries the remaining time from outside the workflow while virtual time
// is advanced by the test environment.
func TestTimerRemainingTime_Query(t *testing.T) {
	var suite WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(timerRemainingQueryWorkflow)

	var queried time.Duration
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow("timer-remaining")
		require.NoError(t, err)
		require.NoError(t, value.Get(&queried))
	}, 30*time.Minute)

	env.ExecuteWorkflow(timerRemainingQueryWorkflow)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 30*time.Minute, queried)
}

func timerRemainingTestEvent(event *historypb.HistoryEvent, eventTime time.Time) *historypb.HistoryEvent {
	event.EventTime = timestamppb.New(eventTime)
	return event
}

// TestTimerRemainingTime_Replay replays a history with fixed event timestamps and verifies that the
// remaining time computed from workflow time is deterministic across replay: 1 hour at creation,
// 35 minutes after the first 25 minute timer fired, and zero after the timer itself fired.
func TestTimerRemainingTime_Replay(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t25 := t0.Add(25 * time.Minute)
	t60 := t0.Add(time.Hour)
	taskQueue := "taskQueue1"

	// observed is filled in by the workflow during replay.
	observed := make([]time.Duration, 0, 3)
	replayWorkflow := func(ctx Context) error {
		timer := NewTimerHandle(ctx, time.Hour)
		observed = append(observed, timer.RemainingTime())
		if err := Sleep(ctx, 25*time.Minute); err != nil {
			return err
		}
		observed = append(observed, timer.RemainingTime())
		if err := timer.Get(ctx, nil); err != nil {
			return err
		}
		observed = append(observed, timer.RemainingTime())
		return nil
	}

	// Timer command IDs are predicted from the workflow task started event ID (3) + 2, so the 1 hour
	// timer handle gets ID "5" and the 25 minute sleep timer gets ID "6".
	testEvents := []*historypb.HistoryEvent{
		timerRemainingTestEvent(createTestEventWorkflowExecutionStarted(1, &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{Name: "timerRemainingReplayWorkflow"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:        testEncodeFunctionArgs(converter.GetDefaultDataConverter()),
		}), t0),
		timerRemainingTestEvent(createTestEventWorkflowTaskScheduled(2, &historypb.WorkflowTaskScheduledEventAttributes{}), t0),
		timerRemainingTestEvent(createTestEventWorkflowTaskStarted(3), t0),
		timerRemainingTestEvent(createTestEventWorkflowTaskCompleted(4, &historypb.WorkflowTaskCompletedEventAttributes{}), t0),
		timerRemainingTestEvent(createTestEventTimerStarted(5, 5), t0),
		timerRemainingTestEvent(createTestEventTimerStarted(6, 6), t0),
		timerRemainingTestEvent(createTestEventTimerFired(7, 6), t25),
		timerRemainingTestEvent(createTestEventWorkflowTaskScheduled(8, &historypb.WorkflowTaskScheduledEventAttributes{}), t25),
		timerRemainingTestEvent(createTestEventWorkflowTaskStarted(9), t25),
		timerRemainingTestEvent(createTestEventWorkflowTaskCompleted(10, &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: 8,
			StartedEventId:   9,
		}), t25),
		timerRemainingTestEvent(createTestEventTimerFired(11, 5), t60),
		timerRemainingTestEvent(createTestEventWorkflowTaskScheduled(12, &historypb.WorkflowTaskScheduledEventAttributes{}), t60),
		timerRemainingTestEvent(createTestEventWorkflowTaskStarted(13), t60),
		timerRemainingTestEvent(createTestEventWorkflowTaskCompleted(14, &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: 12,
			StartedEventId:   13,
		}), t60),
		timerRemainingTestEvent(createTestEventWorkflowExecutionCompleted(15, &historypb.WorkflowExecutionCompletedEventAttributes{
			WorkflowTaskCompletedEventId: 14,
		}), t60),
	}

	replayer, err := NewWorkflowReplayer(WorkflowReplayerOptions{})
	require.NoError(t, err)
	replayer.RegisterWorkflowWithOptions(replayWorkflow, RegisterWorkflowOptions{Name: "timerRemainingReplayWorkflow"})
	require.NoError(t, replayer.ReplayWorkflowHistory(getLogger(), &historypb.History{Events: testEvents}))
	require.Equal(t, []time.Duration{time.Hour, 35 * time.Minute, 0}, observed)
}
