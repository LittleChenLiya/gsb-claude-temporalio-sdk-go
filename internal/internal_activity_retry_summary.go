package internal

// All code in this file is private to the package.

import (
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
)

// ActivityFailureReason is the stable, coarse-grained category of the final
// reason an activity attempt ended. It deliberately carries no error message,
// error type name, details or stack trace, so a summary built from it never
// leaks sensitive failure contents.
//
// Exposed as: [go.temporal.io/sdk/temporal.ActivityFailureReason]
type ActivityFailureReason string

const (
	// ActivityFailureReasonApplication is a user/application failure reported
	// from activity code (an *ApplicationError or an arbitrary Go error).
	ActivityFailureReasonApplication ActivityFailureReason = "application"
	// ActivityFailureReasonNonRetryableApplication is an application failure
	// explicitly marked non-retryable (NonRetryableError or matching a policy
	// entry).
	ActivityFailureReasonNonRetryableApplication ActivityFailureReason = "application_non_retryable"
	// ActivityFailureReasonTimeout is an activity timeout. Only start-to-close
	// and heartbeat timeouts are retried; schedule timeouts are terminal.
	ActivityFailureReasonTimeout ActivityFailureReason = "timeout"
	// ActivityFailureReasonCancellation is an activity cancellation, which is
	// never retried.
	ActivityFailureReasonCancellation ActivityFailureReason = "cancellation"
	// ActivityFailureReasonPanic is a failure caused by an activity panic.
	ActivityFailureReasonPanic ActivityFailureReason = "panic"
	// ActivityFailureReasonTermination is a failure caused by termination.
	ActivityFailureReasonTermination ActivityFailureReason = "termination"
	// ActivityFailureReasonServer is a (typically non-retryable) server error.
	ActivityFailureReasonServer ActivityFailureReason = "server"
	// ActivityFailureReasonUnknown is any failure that does not fit a known
	// category.
	ActivityFailureReasonUnknown ActivityFailureReason = "unknown"
)

// ActivityRetrySummary is a stable, sanitized summary of why an activity
// attempt finished and whether it is eligible for another attempt.
//
// It is produced on the worker when the activity result is converted into a
// respond request. It is intentionally derived only from the error's category
// and the attempt number; it never embeds error messages, error type names,
// failure details, last-heartbeat payloads or stack traces. It is not written
// into history failures, so adding or populating it keeps decoding of existing
// history events unchanged.
//
// Exposed as: [go.temporal.io/sdk/temporal.ActivityRetrySummary]
type ActivityRetrySummary struct {
	// Attempt is the 1-based server attempt number of the activity task, as
	// reported by PollActivityTaskQueue.
	Attempt int32
	// Reason is the stable, coarse-grained category of the final failure.
	Reason ActivityFailureReason
	// TimeoutType is populated only when Reason is ActivityFailureReasonTimeout.
	TimeoutType enumspb.TimeoutType
	// Retryable reports whether the worker considers the failure eligible for
	// another attempt under the activity's retry policy. It mirrors the
	// existing IsRetryable decision and is false for cancellations.
	Retryable bool
}

// String renders the summary as a stable, single-line, key=value string. The
// format and its keys are part of the contract; the timeout type is only
// present for timeout failures. No failure content is included.
func (s ActivityRetrySummary) String() string {
	if s.Reason == ActivityFailureReasonTimeout && s.TimeoutType != enumspb.TIMEOUT_TYPE_UNSPECIFIED {
		return fmt.Sprintf("attempt=%d reason=%s timeout_type=%s retryable=%t",
			s.Attempt, s.Reason, s.TimeoutType.String(), s.Retryable)
	}
	return fmt.Sprintf("attempt=%d reason=%s retryable=%t", s.Attempt, s.Reason, s.Retryable)
}

// summarizeActivityFailure builds a sanitized ActivityRetrySummary for an
// activity attempt failure. attempt is the 1-based attempt number from the
// polled task and nonRetryableTypes is the NonRetryableErrorTypes list of the
// activity's effective retry policy (nil when no policy applies). Only the
// category is extracted from err; err.Error() and its details are never read
// into the summary.
func summarizeActivityFailure(err error, attempt int32, nonRetryableTypes []string) ActivityRetrySummary {
	reason, timeoutType := classifyActivityFailure(err)
	summary := ActivityRetrySummary{
		Attempt:     attempt,
		Reason:      reason,
		TimeoutType: timeoutType,
	}
	// Reuse the single existing retry decision so the summary can never
	// disagree with the actual retry behavior.
	summary.Retryable = IsRetryable(err, nonRetryableTypes)
	return summary
}

// classifyActivityFailure maps an activity error to its stable reason category.
// It inspects only error types and, for application errors, the non-retryable
// marker; it never reads error messages or payloads.
func classifyActivityFailure(err error) (ActivityFailureReason, enumspb.TimeoutType) {
	if err == nil {
		return ActivityFailureReasonUnknown, enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}

	var canceledErr *CanceledError
	if errors.As(err, &canceledErr) {
		return ActivityFailureReasonCancellation, enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}

	var timeoutErr *TimeoutError
	if errors.As(err, &timeoutErr) {
		return ActivityFailureReasonTimeout, timeoutErr.TimeoutType()
	}

	var panicErr *PanicError
	if errors.As(err, &panicErr) {
		return ActivityFailureReasonPanic, enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}

	var terminatedErr *TerminatedError
	if errors.As(err, &terminatedErr) {
		return ActivityFailureReasonTermination, enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}

	var serverErr *ServerError
	if errors.As(err, &serverErr) {
		return ActivityFailureReasonServer, enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}

	var applicationErr *ApplicationError
	if errors.As(err, &applicationErr) {
		if applicationErr.NonRetryable() {
			return ActivityFailureReasonNonRetryableApplication, enumspb.TIMEOUT_TYPE_UNSPECIFIED
		}
		return ActivityFailureReasonApplication, enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}

	return ActivityFailureReasonUnknown, enumspb.TIMEOUT_TYPE_UNSPECIFIED
}
