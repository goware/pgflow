package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
)

// WorkflowDef is a workflow's durable name paired with its input type.
type WorkflowDef[In any] struct {
	name string
}

// NewWorkflow declares a workflow whose tasks and Start all take input In.
func NewWorkflow[In any](name string) WorkflowDef[In] {
	return WorkflowDef[In]{name: name}
}

// Name returns the workflow's durable name.
func (d WorkflowDef[In]) Name() string { return d.name }

// Workflow is a named, ordered set of tasks built by Define.
type Workflow struct {
	name  string
	tasks []taskSpecCore
}

// Define assembles a workflow's ordered tasks. Registry.Register validates it.
func Define[In any](def WorkflowDef[In], tasks ...TaskSpec[In]) Workflow {
	wf := Workflow{name: def.name, tasks: make([]taskSpecCore, len(tasks))}
	for i, spec := range tasks {
		wf.tasks[i] = spec.core
	}
	return wf
}

// Name returns the workflow's name.
func (w Workflow) Name() string { return w.name }

// Registry holds registered workflows. Register every workflow before
// NewManager, which freezes the registry.
type Registry struct {
	workflows map[string]*workflowDef
	outTypes  map[string]reflect.Type
	frozen    bool
}

// NewRegistry returns an empty, mutable registry.
func NewRegistry() *Registry {
	return &Registry{
		workflows: make(map[string]*workflowDef),
		outTypes:  make(map[string]reflect.Type),
	}
}

type workflowDef struct {
	tasks     []*taskDef
	byHandler map[string]*taskDef
}

type taskDef struct {
	handlerName string
	handler     erasedHandler
	cfg         taskConfig
}

// TaskDef is a task's durable name paired with the type of output it records.
type TaskDef[Out any] struct {
	name string
}

// NewTask declares a task whose handler returns Out; use None to record no output.
func NewTask[Out any](name string) TaskDef[Out] {
	return TaskDef[Out]{name: name}
}

// Name returns the task's durable handler name.
func (d TaskDef[Out]) Name() string { return d.name }

type erasedHandler func(ctx context.Context, core taskRunCore) (output any, err error)

// TaskSpec is one task, produced by Handle and consumed by Define.
type TaskSpec[In any] struct {
	core taskSpecCore
}

// taskSpecCore keeps outType so Register can reject one task name recording
// two different output types.
type taskSpecCore struct {
	handlerName string
	handler     erasedHandler
	outType     reflect.Type
	cfg         taskConfig
}

// Handle binds a typed handler to a task definition. A handler whose input or
// output type disagrees with the definition is a compile error.
func Handle[In, Out any](def TaskDef[Out], h func(context.Context, *TaskRun[In]) (Out, error), opts ...TaskOption) TaskSpec[In] {
	var cfg taskConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return TaskSpec[In]{core: taskSpecCore{
		handlerName: def.name,
		outType:     reflect.TypeOf((*Out)(nil)).Elem(),
		cfg:         cfg,
		handler: func(ctx context.Context, core taskRunCore) (any, error) {
			var in In
			if err := json.Unmarshal(core.input, &in); err != nil {
				// The run's input never changes, so a retry cannot fix it.
				return nil, Permanent(fmt.Errorf("workflow: decode input for task %q: %w", def.name, err))
			}
			out, err := h(ctx, &TaskRun[In]{Input: in, Log: core.log, core: core})
			if err != nil {
				return nil, err
			}
			if _, isNone := any(out).(None); isNone {
				return nil, nil
			}
			return out, nil
		},
	}}
}

// Register validates and indexes a workflow. It fails after NewManager.
func (r *Registry) Register(wf Workflow) error {
	if r.frozen {
		return fmt.Errorf("workflow: registry frozen, cannot register workflow %q", wf.name)
	}
	if _, exists := r.workflows[wf.name]; exists {
		return fmt.Errorf("workflow: workflow %q already registered", wf.name)
	}
	if len(wf.tasks) == 0 {
		return fmt.Errorf("workflow: workflow %q has no tasks", wf.name)
	}
	// A task ordinal is an int16 end to end, so one more task would wrap the
	// advance to a negative index.
	if len(wf.tasks) > math.MaxInt16 {
		return fmt.Errorf("workflow: workflow %q has %d tasks, over the %d limit", wf.name, len(wf.tasks), math.MaxInt16)
	}

	def := &workflowDef{byHandler: make(map[string]*taskDef, len(wf.tasks))}
	for _, spec := range wf.tasks {
		if spec.handlerName == "" {
			return fmt.Errorf("workflow: workflow %q has a task with an empty name", wf.name)
		}
		if _, exists := def.byHandler[spec.handlerName]; exists {
			return fmt.Errorf("workflow: workflow %q has duplicate handler %q", wf.name, spec.handlerName)
		}
		// Output resolves a TaskDef by name alone, so two output types under one
		// name would silently mis-decode another workflow's output.
		if prior, ok := r.outTypes[spec.handlerName]; ok && prior != spec.outType {
			return fmt.Errorf("workflow: task %q output type %v conflicts with %v from an earlier workflow", spec.handlerName, spec.outType, prior)
		}
		if err := validateBackoff(wf.name, spec); err != nil {
			return err
		}
		task := &taskDef{
			handlerName: spec.handlerName,
			handler:     spec.handler,
			cfg:         spec.cfg,
		}
		def.tasks = append(def.tasks, task)
		def.byHandler[spec.handlerName] = task
	}
	r.workflows[wf.name] = def
	for _, spec := range wf.tasks {
		r.outTypes[spec.handlerName] = spec.outType
	}
	return nil
}

// validateBackoff rejects negative schedule entries because they put
// next_retry_at in the past and spin on immediate re-claims.
func validateBackoff(workflowName string, spec taskSpecCore) error {
	if spec.cfg.backoffSchedule == nil {
		return nil
	}
	if spec.cfg.backoffBase != 0 || spec.cfg.backoffMax != 0 {
		return fmt.Errorf("workflow: workflow %q task %q sets both Backoff and BackoffSchedule", workflowName, spec.handlerName)
	}
	if len(spec.cfg.backoffSchedule) == 0 {
		return fmt.Errorf("workflow: workflow %q task %q has an empty BackoffSchedule", workflowName, spec.handlerName)
	}
	for i, d := range spec.cfg.backoffSchedule {
		if d < 0 {
			return fmt.Errorf("workflow: workflow %q task %q has a negative BackoffSchedule entry at index %d", workflowName, spec.handlerName, i)
		}
	}
	return nil
}

func (r *Registry) freeze() {
	r.frozen = true
}

func (r *Registry) names() []string {
	return slices.Sorted(maps.Keys(r.workflows))
}

// resolve returns ok=false on version skew: an unknown workflow or handler name.
func (r *Registry) resolve(workflowName, handlerName string) (erasedHandler, taskConfig, bool) {
	wf, ok := r.workflows[workflowName]
	if !ok {
		return nil, taskConfig{}, false
	}
	task, ok := wf.byHandler[handlerName]
	if !ok {
		return nil, taskConfig{}, false
	}
	return task.handler, task.cfg, true
}

type taskSnapshot struct {
	handlerName string
	skipped     bool
}

func (r *Registry) snapshot(workflowName string, input json.RawMessage) (name string, tasks []taskSnapshot, ok bool, err error) {
	wf, ok := r.workflows[workflowName]
	if !ok {
		return "", nil, false, nil
	}

	rows := make([]taskSnapshot, len(wf.tasks))
	for i, task := range wf.tasks {
		var skipped bool
		if task.cfg.skip != nil {
			skipped, err = task.cfg.skip(input)
			if err != nil {
				return "", nil, false, fmt.Errorf("workflow: evaluate SkipIf for %q: %w", task.handlerName, err)
			}
		}
		rows[i] = taskSnapshot{handlerName: task.handlerName, skipped: skipped}
	}
	return workflowName, rows, true, nil
}
