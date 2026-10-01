package internal

// All code in this file is private to the package.

import "sync"

// TaskDrainOutcome categorizes how an in-flight task ended while the worker
// was draining during a graceful stop. The outcome mirrors the terminal
// callback the worker actually sent (or, for TimedOut, did not send), so that
// drain reports never disagree with the real task lifecycle.
type TaskDrainOutcome int

const (
	// DrainOutcomeCompleted is for tasks that sent a successful completion
	// callback before the worker finished draining.
	DrainOutcomeCompleted TaskDrainOutcome = iota
	// DrainOutcomeCanceled is for tasks whose terminal callback reported the
	// task as canceled.
	DrainOutcomeCanceled
	// DrainOutcomeFailed is for tasks whose terminal callback reported the
	// task as failed.
	DrainOutcomeFailed
	// DrainOutcomeTimedOut is for tasks that were still in flight when the
	// worker stop timeout elapsed, or that otherwise could not report a
	// terminal callback (e.g. the completion RPC failed). The server will
	// time these out and redispatch them.
	DrainOutcomeTimedOut
)

// WorkerStopDrainReport summarizes what happened to in-flight tasks while the
// worker drained during a graceful stop. Counts describe only the tasks that
// were executing when the drain began (plus tasks that started afterwards but
// finished with a terminal callback during the drain), not tasks completed
// earlier in the worker's lifetime.
//
// Exposed as: [go.temporal.io/sdk/worker.StopDrainReport]
type WorkerStopDrainReport struct {
	// Completed is the number of tasks that completed successfully during the drain.
	Completed int
	// Canceled is the number of tasks that reported cancellation during the drain.
	Canceled int
	// Failed is the number of tasks that reported failure during the drain.
	Failed int
	// TimedOut is the number of tasks still in flight when WorkerStopTimeout
	// elapsed (or whose terminal callback could not be delivered).
	TimedOut int
	// Graceful is true when all in-flight tasks settled before the stop timeout.
	Graceful bool
}

// workerDrainTracker counts how in-flight tasks settle while a worker drains
// on stop. A task is tracked for its whole execution; only tasks that were
// executing (or started and finished) after the drain began contribute to the
// final report. All methods are safe for concurrent use and the tracker is a
// no-op before beginDrain is called.
type workerDrainTracker struct {
	mu sync.Mutex

	draining bool
	finished bool
	report   WorkerStopDrainReport

	// inFlight maps a task's unique key to true while the task is executing.
	inFlight map[string]bool
	// drainInFlight is the set of tasks that were in flight when the drain
	// began (and tasks that started during the drain).
	drainInFlight map[string]bool
}

func newWorkerDrainTracker() *workerDrainTracker {
	return &workerDrainTracker{
		inFlight:      make(map[string]bool),
		drainInFlight: make(map[string]bool),
	}
}

// taskStarted records that a task with the given key began executing.
func (t *workerDrainTracker) taskStarted(key string) {
	if t == nil || key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight[key] = true
	if t.draining {
		t.drainInFlight[key] = true
	}
}

// taskFinished records the terminal outcome of a task. If the task was part of
// the drain, its outcome is counted once and it is removed from the drain set.
// Calling taskFinished more than once for the same key only counts the first
// terminal outcome.
func (t *workerDrainTracker) taskFinished(key string, outcome TaskDrainOutcome) {
	if t == nil || key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inFlight, key)
	if !t.draining || t.finished {
		return
	}
	if !t.drainInFlight[key] {
		return
	}
	delete(t.drainInFlight, key)
	switch outcome {
	case DrainOutcomeCompleted:
		t.report.Completed++
	case DrainOutcomeCanceled:
		t.report.Canceled++
	case DrainOutcomeFailed:
		t.report.Failed++
	case DrainOutcomeTimedOut:
		t.report.TimedOut++
	}
}

// beginDrain marks the start of the graceful-stop drain and snapshots the set
// of currently in-flight tasks. It is a no-op if draining already started, so
// a repeated Stop cannot double-count.
func (t *workerDrainTracker) beginDrain() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining {
		return
	}
	t.draining = true
	for key := range t.inFlight {
		t.drainInFlight[key] = true
	}
}

// finishDrain finalizes and returns the drain report. Any tasks still in the
// drain set are counted as timed out. graceful indicates whether any task
// timed out. Repeated calls return the same report and never recount.
func (t *workerDrainTracker) finishDrain(graceful bool) WorkerStopDrainReport {
	if t == nil {
		return WorkerStopDrainReport{Graceful: graceful}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return t.report
	}
	t.finished = true
	t.report.TimedOut += len(t.drainInFlight)
	for key := range t.drainInFlight {
		delete(t.drainInFlight, key)
	}
	t.report.Graceful = graceful
	return t.report
}
