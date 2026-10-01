package internal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"

	"go.temporal.io/sdk/internal/common/metrics"
	ilog "go.temporal.io/sdk/internal/log"
)

// sensitiveToken is a fake secret that must never appear in a retry summary,
// even though it is present in the activity error message and details.
const sensitiveToken = "super-secret-token-4242"

// TestActivityRetrySummaryClassify covers the stable classification table:
// retryable, non-retryable and cancellation failures, plus timeouts, panics
// and unknown errors.
func TestActivityRetrySummaryClassify(t *testing.T) {
	cases := []struct {
		name            string
		err             error
		attempt         int32
		nonRetryTypes   []string
		wantReason      ActivityFailureReason
		wantRetryable   bool
		wantTimeoutType enumspb.TimeoutType
	}{
		{
			name:          "retryable application error",
			err:           NewApplicationError("retry me "+sensitiveToken, "MyRetryableError", false, nil),
			attempt:       2,
			wantReason:    ActivityFailureReasonApplication,
			wantRetryable: true,
		},
		{
			name:          "retryable generic go error",
			err:           errors.New("boom " + sensitiveToken),
			attempt:       3,
			wantReason:    ActivityFailureReasonUnknown,
			wantRetryable: true,
		},
		{
			name:          "explicit non-retryable application error",
			err:           NewApplicationError("fatal "+sensitiveToken, "MyFatalError", true, nil),
			attempt:       1,
			wantReason:    ActivityFailureReasonNonRetryableApplication,
			wantRetryable: false,
		},
		{
			name:          "retryable type blocked by retry policy",
			err:           NewApplicationError("blocked "+sensitiveToken, "BlockedType", false, nil),
			attempt:       2,
			nonRetryTypes: []string{"BlockedType"},
			wantReason:    ActivityFailureReasonApplication,
			wantRetryable: false,
		},
		{
			name:            "start to close timeout is retryable",
			err:             NewTimeoutError("slow "+sensitiveToken, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil),
			attempt:         4,
			wantReason:      ActivityFailureReasonTimeout,
			wantRetryable:   true,
			wantTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		{
			name:            "heartbeat timeout is retryable",
			err:             NewHeartbeatTimeoutError("hb"),
			attempt:         2,
			wantReason:      ActivityFailureReasonTimeout,
			wantRetryable:   true,
			wantTimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
		},
		{
			name:            "schedule to close timeout is not retryable",
			err:             NewTimeoutError("schedule timeout", enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, nil),
			attempt:         1,
			wantReason:      ActivityFailureReasonTimeout,
			wantRetryable:   false,
			wantTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		},
		{
			name:          "cancellation is never retryable",
			err:           NewCanceledError("cancel details " + sensitiveToken),
			attempt:       5,
			wantReason:    ActivityFailureReasonCancellation,
			wantRetryable: false,
		},
		{
			name:          "wrapped cancellation is still cancellation",
			err:           errors.Join(NewCanceledError("secret-cancel-details"), errors.New("wrapping "+sensitiveToken)),
			attempt:       2,
			wantReason:    ActivityFailureReasonCancellation,
			wantRetryable: false,
		},
		{
			// Activity panics keep the existing IsRetryable decision (only
			// *workflowPanicError is force-non-retryable): the server may still
			// retry an activity panic under its retry policy. The summary mirrors
			// that decision rather than inventing a new one.
			name:          "panic keeps existing retry decision",
			err:           newPanicError("panic "+sensitiveToken, "stack"),
			attempt:       1,
			wantReason:    ActivityFailureReasonPanic,
			wantRetryable: true,
		},
		{
			name:          "nil failure stays unknown and non-retryable",
			err:           nil,
			attempt:       1,
			wantReason:    ActivityFailureReasonUnknown,
			wantRetryable: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			summary := summarizeActivityFailure(tc.err, tc.attempt, tc.nonRetryTypes)
			require.Equal(t, tc.attempt, summary.Attempt, "attempt must be carried through verbatim")
			require.Equal(t, tc.wantReason, summary.Reason)
			require.Equal(t, tc.wantTimeoutType, summary.TimeoutType)
			require.Equal(t, tc.wantRetryable, summary.Retryable, "summary retry flag must match IsRetryable")

			// The summary must agree with the existing single retry decision.
			require.Equal(t, IsRetryable(tc.err, tc.nonRetryTypes), summary.Retryable,
				"summary retryable must match IsRetryable decision")

			rendered := summary.String()
			require.Contains(t, rendered, "attempt=")
			require.Contains(t, rendered, "reason=")
			require.Contains(t, rendered, "retryable=")
			require.NotContains(t, rendered, sensitiveToken,
				"sensitive error content must not enter the summary: %s", rendered)
		})
	}
}

// TestActivityRetrySummaryConsistentWithRetryBackoff cross-checks the summary
// against the local activity retry decision (getRetryBackoffWithNowTime),
// which is what actually schedules retries.
func TestActivityRetrySummaryConsistentWithRetryBackoff(t *testing.T) {
	policy := &RetryPolicy{
		InitialInterval:    time.Millisecond,
		BackoffCoefficient: 1,
		MaximumInterval:    time.Millisecond,
	}
	now := time.Now()

	retryableErr := NewApplicationError("retryable "+sensitiveToken, "RetryableType", false, nil)
	summary := summarizeActivityFailure(retryableErr, 1, policy.NonRetryableErrorTypes)
	backoff := getRetryBackoffWithNowTime(policy, 1, retryableErr, now, time.Time{})
	require.True(t, summary.Retryable)
	require.Greater(t, backoff, time.Duration(0), "retryable error must yield a positive backoff")

	nonRetryableErr := NewApplicationError("fatal "+sensitiveToken, "FatalType", true, nil)
	summary = summarizeActivityFailure(nonRetryableErr, 1, policy.NonRetryableErrorTypes)
	backoff = getRetryBackoffWithNowTime(policy, 1, nonRetryableErr, now, time.Time{})
	require.False(t, summary.Retryable)
	require.Equal(t, noRetryBackoff, backoff, "non-retryable error must not be retried")

	canceledErr := NewCanceledError()
	summary = summarizeActivityFailure(canceledErr, 1, policy.NonRetryableErrorTypes)
	backoff = getRetryBackoffWithNowTime(policy, 1, canceledErr, now, time.Time{})
	require.False(t, summary.Retryable)
	require.Equal(t, noRetryBackoff, backoff, "cancellation must not be retried")
	require.Equal(t, ActivityFailureReasonCancellation, summary.Reason)
}

type retrySummaryTestSuite struct {
	suite.Suite
}

func TestActivityRetrySummarySuite(t *testing.T) {
	suite.Run(t, &retrySummaryTestSuite{})
}

func (s *retrySummaryTestSuite) newActivityPoll(attempt int32, activityName string) *workflowservice.PollActivityTaskQueueResponse {
	now := time.Now()
	return &workflowservice.PollActivityTaskQueueResponse{
		Attempt:   attempt,
		TaskToken: []byte("token-" + activityName),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "wID",
			RunId:      "rID",
		},
		ActivityType:           &commonpb.ActivityType{Name: activityName},
		ActivityId:             uuid.NewString(),
		ScheduledTime:          timestamppb.New(now),
		ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
		StartedTime:            timestamppb.New(now),
		StartToCloseTimeout:    durationpb.New(30 * time.Second),
		WorkflowType:           &commonpb.WorkflowType{Name: "wType"},
		WorkflowNamespace:      testNamespace,
	}
}

// retryableSummaryActivity fails with a retryable application error.
func retryableSummaryActivity() error {
	return NewApplicationError("retryable failure "+sensitiveToken, "RetryableSummaryType", false, nil,
		"secret-details")
}

// nonRetryableSummaryActivity fails with an explicitly non-retryable error.
func nonRetryableSummaryActivity() error {
	return NewApplicationError("permanent failure "+sensitiveToken, "NonRetryableSummaryType", true, nil,
		"secret-details")
}

// canceledSummaryActivity triggers cancellation through its invoker and waits
// for its context to be canceled, exactly like a real activity honoring cancel.
func (s *retrySummaryTestSuite) canceledSummaryActivity(ctx context.Context) error {
	env := getActivityEnv(ctx)
	invoker, ok := env.serviceInvoker.(*temporalInvoker)
	s.Require().True(ok)
	invoker.cancelHandler(NewCanceledError("secret-cancel-details"))
	<-ctx.Done()
	return ctx.Err()
}

// TestActivityHandlerRetrySummary runs the real activityTaskHandlerImpl Execute
// path for retryable, non-retryable and canceled activities and verifies both
// the respond request type and the attached sanitized summary.
func (s *retrySummaryTestSuite) TestActivityHandlerRetrySummary() {
	r := newRegistry()
	r.RegisterActivityWithOptions(retryableSummaryActivity,
		RegisterActivityOptions{Name: "retryableSummaryActivity"})
	r.RegisterActivityWithOptions(nonRetryableSummaryActivity,
		RegisterActivityOptions{Name: "nonRetryableSummaryActivity"})
	r.RegisterActivityWithOptions(s.canceledSummaryActivity,
		RegisterActivityOptions{Name: "canceledSummaryActivity"})

	mockCtrl := gomock.NewController(s.T())
	mockService := workflowservicemock.NewMockWorkflowServiceClient(mockCtrl)
	client := &WorkflowClient{workflowService: mockService}
	params := workerExecutionParameters{
		TaskQueue:        taskqueue,
		Namespace:        testNamespace,
		Identity:         "test-id",
		MetricsHandler:   metrics.NopHandler,
		Logger:           ilog.NewDefaultLogger(),
		FailureConverter: GetDefaultFailureConverter(),
	}
	handler := newActivityTaskHandler(client, params, r)

	s.Run("retryable failure", func() {
		pats := s.newActivityPoll(3, "retryableSummaryActivity")
		result, err := handler.Execute(taskqueue, pats)
		s.Require().NoError(err)
		s.Require().IsType(&workflowservice.RespondActivityTaskFailedRequest{}, result.response)
		s.Require().NotNil(result.retrySummary)
		s.Equal(int32(3), result.retrySummary.Attempt)
		s.Equal(ActivityFailureReasonApplication, result.retrySummary.Reason)
		s.True(result.retrySummary.Retryable)
		s.False(strings.Contains(result.retrySummary.String(), sensitiveToken))
	})

	s.Run("non-retryable failure", func() {
		pats := s.newActivityPoll(1, "nonRetryableSummaryActivity")
		result, err := handler.Execute(taskqueue, pats)
		s.Require().NoError(err)
		s.Require().IsType(&workflowservice.RespondActivityTaskFailedRequest{}, result.response)
		s.Require().NotNil(result.retrySummary)
		s.Equal(int32(1), result.retrySummary.Attempt)
		s.Equal(ActivityFailureReasonNonRetryableApplication, result.retrySummary.Reason)
		s.False(result.retrySummary.Retryable)
		s.False(strings.Contains(result.retrySummary.String(), sensitiveToken))
	})

	s.Run("canceled", func() {
		pats := s.newActivityPoll(2, "canceledSummaryActivity")
		result, err := handler.Execute(taskqueue, pats)
		s.Require().NoError(err)
		s.Require().IsType(&workflowservice.RespondActivityTaskCanceledRequest{}, result.response)
		s.Require().NotNil(result.retrySummary, "canceled result must carry a summary too")
		s.Equal(int32(2), result.retrySummary.Attempt)
		s.Equal(ActivityFailureReasonCancellation, result.retrySummary.Reason)
		s.False(result.retrySummary.Retryable, "cancellation must never be retryable")
		s.False(strings.Contains(result.retrySummary.String(), sensitiveToken))
	})

	s.Run("successful activity has no summary", func() {
		r.RegisterActivityWithOptions(func() error { return nil },
			RegisterActivityOptions{Name: "okSummaryActivity"})
		pats := s.newActivityPoll(1, "okSummaryActivity")
		result, err := handler.Execute(taskqueue, pats)
		s.Require().NoError(err)
		s.Require().IsType(&workflowservice.RespondActivityTaskCompletedRequest{}, result.response)
		s.Nil(result.retrySummary)
	})
}

// TestHistoryFailureDecodeCompatibility ensures that failures produced for
// history (the full failure, separate from the sanitized summary) still
// round-trip, and that legacy/history events with only standard Failure
// payloads decode exactly as before: into *ActivityError with the cause and
// retry state preserved.
func TestHistoryFailureDecodeCompatibility(t *testing.T) {
	fc := GetDefaultFailureConverter()

	// A failure containing sensitive content round-trips through the failure
	// converter unchanged; the summary feature must not alter this path.
	original := NewApplicationError("fatal "+sensitiveToken, "SecretType", true, nil, "secret-details")
	failure := fc.ErrorToFailure(original)
	decoded := fc.FailureToError(failure)
	var appErr *ApplicationError
	require.ErrorAs(t, decoded, &appErr)
	require.Equal(t, "SecretType", appErr.Type())
	require.True(t, appErr.NonRetryable())
	require.Contains(t, decoded.Error(), sensitiveToken,
		"history failure path remains lossless and independent of the summary")

	// Decode an ActivityTaskFailed failure the same way handleActivityTaskFailed
	// does. History events carry no summary field, and decoding must keep
	// working unchanged (backward compatibility).
	activityErr := NewActivityError(
		5, 7, "worker-1", &commonpb.ActivityType{Name: "SomeActivity"}, "act-1",
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, fc.FailureToError(failure),
	)
	require.Equal(t, enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, activityErr.RetryState())
	require.ErrorAs(t, activityErr, &appErr)
	require.Equal(t, "SecretType", appErr.Type())

	// A legacy canceled failure decodes to a canceled cause.
	canceledFailure := fc.ErrorToFailure(NewCanceledError())
	legacyCanceledErr := NewActivityError(1, 2, "", &commonpb.ActivityType{Name: "C"}, "act-2",
		enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, fc.FailureToError(canceledFailure))
	require.True(t, IsCanceledError(legacyCanceledErr))
	require.False(t, IsRetryable(legacyCanceledErr, nil))

	// The summary can be derived from the decoded history error identically to
	// a fresh error, and still contains no sensitive content.
	summary := summarizeActivityFailure(legacyCanceledErr, 6, nil)
	require.Equal(t, ActivityFailureReasonCancellation, summary.Reason)
	require.False(t, summary.Retryable)
	require.NotContains(t, summary.String(), sensitiveToken)
}

// TestActivityFailedHistoryEventShapeOnly constructs an ActivityError from a
// plain ActivityTaskFailedEventAttributes exactly like the history event
// handler does, guarding against accidental signature/field changes while
// wiring the summary, and confirms the summary works over the decoded error.
func TestActivityFailedHistoryEventShapeOnly(t *testing.T) {
	fc := GetDefaultFailureConverter()
	failure := fc.ErrorToFailure(NewApplicationError("e "+sensitiveToken, "T", false, nil))
	event := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: 10,
				StartedEventId:   11,
				Identity:         "worker",
				RetryState:       enumspb.RETRY_STATE_IN_PROGRESS,
				Failure:          failure,
			},
		},
	}
	attrs := event.GetActivityTaskFailedEventAttributes()
	activityErr := NewActivityError(
		attrs.GetScheduledEventId(),
		attrs.GetStartedEventId(),
		attrs.GetIdentity(),
		&commonpb.ActivityType{Name: "SomeActivity"},
		"activity-id",
		attrs.GetRetryState(),
		fc.FailureToError(attrs.GetFailure()),
	)
	summary := summarizeActivityFailure(activityErr, 2, nil)
	require.Equal(t, int32(2), summary.Attempt)
	require.Equal(t, ActivityFailureReasonApplication, summary.Reason)
	require.True(t, summary.Retryable)
	require.NotContains(t, summary.String(), sensitiveToken)
}
