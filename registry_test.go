package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func noopTask(name string, opts ...TaskOption) TaskSpec[testInput] {
	return Handle(NewTask[None](name), func(context.Context, *TaskRun[testInput]) (None, error) {
		return None{}, nil
	}, opts...)
}

func mapTask(name string, out map[string]string, opts ...TaskOption) TaskSpec[testInput] {
	return Handle(NewTask[map[string]string](name),
		func(context.Context, *TaskRun[testInput]) (map[string]string, error) {
			return out, nil
		}, opts...)
}

func TestRegistry(t *testing.T) {
	t.Run("Define registers a plan of ordered stages", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first"),
			noopTask("test.second"),
		)))
	})

	t.Run("Define errors on a duplicate plan name", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"), noopTask("test.first"))))
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"), noopTask("test.first"))))
	})

	t.Run("Define errors on zero stages", func(t *testing.T) {
		reg := NewRegistry()
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"))))
	})

	t.Run("Register rejects more stages than an int16 ordinal can address", func(t *testing.T) {
		tasks := make([]taskSpecCore, math.MaxInt16+1)
		for i := range tasks {
			tasks[i] = taskSpecCore{handlerName: fmt.Sprintf("test.stage-%d", i)}
		}

		reg := NewRegistry()
		err := reg.Register(Workflow{name: "test.toomany", tasks: tasks})
		require.EqualError(t, err,
			fmt.Sprintf("workflow: workflow %q has %d tasks, over the %d limit", "test.toomany", len(tasks), math.MaxInt16))

		regAtLimit := NewRegistry()
		require.NoError(t, regAtLimit.Register(Workflow{name: "test.atlimit", tasks: tasks[:math.MaxInt16]}))
	})

	t.Run("Define errors on a duplicate handler name within a plan", func(t *testing.T) {
		reg := NewRegistry()
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first"),
			noopTask("test.first"),
		)))
	})

	t.Run("Define errors on an empty task name", func(t *testing.T) {
		reg := NewRegistry()
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"), noopTask(""))))
	})

	t.Run("Register rejects a task name reused with a different output type", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.a"), mapTask("test.shared", nil))))
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.b"), noopTask("test.shared"))),
			"test.shared records map[string]string in test.a and None in test.b")
	})

	t.Run("Register allows a task name reused with the same output type", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.a"), noopTask("test.shared"))))
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.b"), noopTask("test.shared"))))
	})

	t.Run("Register rejects a task setting both Backoff and BackoffSchedule", func(t *testing.T) {
		reg := NewRegistry()
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first", Backoff(time.Second, time.Minute), BackoffSchedule([]time.Duration{time.Second})),
		)))
	})

	t.Run("Register rejects an empty BackoffSchedule", func(t *testing.T) {
		reg := NewRegistry()
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.empty"),
			noopTask("test.first", BackoffSchedule([]time.Duration{})),
		)))
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.nil"),
			noopTask("test.first", BackoffSchedule(nil)),
		)))
	})

	t.Run("Register rejects a negative BackoffSchedule entry", func(t *testing.T) {
		reg := NewRegistry()
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first", BackoffSchedule([]time.Duration{time.Second, -time.Second})),
		)))
	})

	t.Run("Register accepts a BackoffSchedule on its own", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first", BackoffSchedule([]time.Duration{0, time.Second})),
		)))
	})

	t.Run("Define errors after freeze", func(t *testing.T) {
		reg := NewRegistry()
		reg.freeze()
		require.Error(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"), noopTask("test.first"))))
	})

	t.Run("resolve returns the handler and cfg for a known stage", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first", MaxAttempts(3)),
		)))

		h, cfg, ok := reg.resolve("test.plan", "test.first")
		require.True(t, ok)
		require.NotNil(t, h)
		require.Equal(t, 3, cfg.maxAttempts)
	})

	t.Run("resolve reports ok=false for an unknown plan name", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"), noopTask("test.first"))))
		_, _, ok := reg.resolve("test.missing", "test.first")
		require.False(t, ok)
	})

	t.Run("resolve reports ok=false for an unknown handler name", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"), noopTask("test.first"))))
		_, _, ok := reg.resolve("test.plan", "test.missing")
		require.False(t, ok)
	})

	t.Run("snapshot produces ordered rows with skipped set where SkipIf is true", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first"),
			noopTask("test.second").SkipIf(func(in testInput) bool { return in.Flag }),
			noopTask("test.third"),
		)))

		name, stages, ok, err := reg.snapshot("test.plan", json.RawMessage(`{"flag":true}`))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "test.plan", name)
		require.Equal(t, []taskSnapshot{
			{handlerName: "test.first", skipped: false},
			{handlerName: "test.second", skipped: true},
			{handlerName: "test.third", skipped: false},
		}, stages)
	})

	t.Run("snapshot reports ok=false for an unknown plan name", func(t *testing.T) {
		reg := NewRegistry()
		_, _, ok, err := reg.snapshot("test.missing", json.RawMessage(`{}`))
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("snapshot surfaces a SkipIf decode error", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first").SkipIf(func(in testInput) bool { return in.Flag }),
		)))

		_, _, _, err := reg.snapshot("test.plan", json.RawMessage(`not json`))
		require.Error(t, err)
	})

	t.Run("maxTaskTimeout returns the largest explicit override", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"),
			noopTask("test.first"),
			noopTask("test.second", Timeout(90*time.Second)),
			noopTask("test.third", Timeout(30*time.Second)),
		)))

		cfg := Config{DefaultTaskTimeout: 60 * time.Second}
		require.Equal(t, 90*time.Second, reg.maxTaskTimeout(cfg))
	})

	t.Run("maxTaskTimeout falls back to the default with no overrides", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.plan"), noopTask("test.first"))))

		cfg := Config{DefaultTaskTimeout: 60 * time.Second}
		require.Equal(t, 60*time.Second, reg.maxTaskTimeout(cfg))
	})
}

func TestHandle(t *testing.T) {
	t.Run("decodes the run input into the typed TaskRun", func(t *testing.T) {
		var got testInput
		spec := Handle(NewTask[None]("test.decode"),
			func(_ context.Context, run *TaskRun[testInput]) (None, error) {
				got = run.Input
				return None{}, nil
			})

		_, err := spec.core.handler(t.Context(), taskRunCore{input: json.RawMessage(`{"flag":true}`)})
		require.NoError(t, err)
		require.True(t, got.Flag)
	})

	t.Run("an undecodable input is permanent", func(t *testing.T) {
		called := false
		spec := Handle(NewTask[None]("test.baddecode"),
			func(context.Context, *TaskRun[testInput]) (None, error) {
				called = true
				return None{}, nil
			})

		_, err := spec.core.handler(t.Context(), taskRunCore{input: json.RawMessage(`not json`)})
		require.Error(t, err)
		_, permanent := errors.AsType[*PermanentError](err)
		require.True(t, permanent, "want a permanent error, got %v", err)
		require.False(t, called, "the handler must not run on an undecodable input")
	})

	t.Run("a None result records no output", func(t *testing.T) {
		spec := noopTask("test.none")

		out, err := spec.core.handler(t.Context(), taskRunCore{input: json.RawMessage(`{}`)})
		require.NoError(t, err)
		require.Nil(t, out, "None must persist as null")
	})

	t.Run("a typed result is returned for persistence", func(t *testing.T) {
		type payload struct {
			Token string `json:"token"`
		}
		spec := Handle(NewTask[payload]("test.payload"),
			func(context.Context, *TaskRun[testInput]) (payload, error) {
				return payload{Token: "abc-1"}, nil
			})

		out, err := spec.core.handler(t.Context(), taskRunCore{input: json.RawMessage(`{}`)})
		require.NoError(t, err)
		require.Equal(t, payload{Token: "abc-1"}, out)
	})
}
