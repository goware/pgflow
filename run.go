package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/goware/workflow/store"
)

// None is the output type of a task that records no output.
type None = struct{}

type taskRunCore struct {
	log     *slog.Logger
	runID   uuid.UUID
	seq     int16
	input   json.RawMessage
	outputs map[string]json.RawMessage
	attempt int
	budget  int
}

// TaskRun is the handler's view of one task run. Execution is at-least-once:
// a handler must respect ctx cancellation and be idempotent.
type TaskRun[In any] struct {
	Input In
	Log   *slog.Logger

	core taskRunCore
}

// NewTaskRun builds a single-attempt TaskRun for unit-testing a handler
// without a Manager. Key outputs by TaskDef.Name.
func NewTaskRun[In any](input In, outputs map[string]any) (*TaskRun[In], error) {
	raw := make(map[string]json.RawMessage, len(outputs))
	for name, out := range outputs {
		b, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		raw[name] = b
	}
	log := slog.New(slog.DiscardHandler)
	return &TaskRun[In]{Input: input, Log: log, core: taskRunCore{log: log, outputs: raw, attempt: 1, budget: 1}}, nil
}

// Output decodes a prior task's recorded output; it errors when there is none.
func Output[In, Out any](run *TaskRun[In], def TaskDef[Out]) (Out, error) {
	var out Out
	raw, ok := run.core.outputs[def.name]
	if !ok {
		return out, fmt.Errorf("workflow: no output recorded for task %q", def.name)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("workflow: decode output for task %q: %w", def.name, err)
	}
	return out, nil
}

// IdempotencyKey is stable across retries and crash-resumes of this task.
// Provider dedup windows are finite, so a run parked for weeks can still re-emit.
func (r *TaskRun[In]) IdempotencyKey() string {
	return fmt.Sprintf("%s:%d", r.core.runID, r.core.seq)
}

// LastAttempt reports whether a failure now exhausts the attempt budget.
// A permanent failure parks earlier regardless.
func (r *TaskRun[In]) LastAttempt() bool {
	return r.core.attempt >= r.core.budget
}

// Status is a workflow run's lifecycle state, aliased from the persisted values.
type Status = store.WorkflowStatus

const (
	StatusRunning   Status = store.WorkflowStatusRunning
	StatusSucceeded Status = store.WorkflowStatusSucceeded
	StatusStuck     Status = store.WorkflowStatusStuck
	// StatusCancelled is reserved; no code path produces it.
	StatusCancelled Status = store.WorkflowStatusCancelled
)

// WorkflowRun is a handle to one started workflow run.
type WorkflowRun struct {
	id  uuid.UUID
	mgr *Manager
}

// ID returns the run's identifier.
func (r *WorkflowRun) ID() uuid.UUID {
	return r.id
}

// Await blocks until the run is terminal, or returns StatusRunning when ctx
// expires first while the run keeps going in the background.
func (r *WorkflowRun) Await(ctx context.Context) (Status, error) {
	return r.mgr.await(ctx, r.id)
}

// Status returns the run's persisted lifecycle state.
func (r *WorkflowRun) Status(ctx context.Context) (Status, error) {
	return r.mgr.status(ctx, r.id)
}

// Failure is why a run parked: the task that gave up and its last error.
type Failure struct {
	Task string
	// Error is persisted; handlers must keep it PII-free.
	Error string
	// Code is the WithCode tag, empty when none was set or the engine parked the run.
	Code     string
	Attempts int
}

// Failure returns why the run parked, or nil when the run is not stuck.
func (r *WorkflowRun) Failure(ctx context.Context) (*Failure, error) {
	return r.mgr.failure(ctx, r.id)
}
