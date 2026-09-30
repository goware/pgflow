package workflow

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestTaskRun(t *testing.T) {
	t.Run("Input carries the decoded plan input", func(t *testing.T) {
		run, err := NewTaskRun(testInput{Flag: true}, nil)
		require.NoError(t, err)
		require.True(t, run.Input.Flag)
	})

	t.Run("IdempotencyKey is exactly runID:seq", func(t *testing.T) {
		runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
		run := &TaskRun[testInput]{core: taskRunCore{runID: runID, seq: 2}}
		require.Equal(t, "11111111-1111-1111-1111-111111111111:2", run.IdempotencyKey())
	})
}

func TestOutput(t *testing.T) {
	type payload struct {
		Token   string `json:"token"`
		Missing string `json:"missing"`
	}
	first := NewTask[payload]("test.first")
	missing := NewTask[payload]("test.missing")

	t.Run("decodes a prior stage output through its definition", func(t *testing.T) {
		run := &TaskRun[testInput]{core: taskRunCore{outputs: map[string]json.RawMessage{
			"test.first": json.RawMessage(`{"token":"abc-1"}`),
		}}}

		out, err := Output(run, first)
		require.NoError(t, err)
		require.Equal(t, "abc-1", out.Token)
	})

	t.Run("errors on an absent task name", func(t *testing.T) {
		run := &TaskRun[testInput]{core: taskRunCore{outputs: map[string]json.RawMessage{}}}
		_, err := Output(run, missing)
		require.Error(t, err)
	})

	t.Run("tolerates a shape-drifted snapshot", func(t *testing.T) {
		run := &TaskRun[testInput]{core: taskRunCore{outputs: map[string]json.RawMessage{
			"test.first": json.RawMessage(`{"token":"abc-1","unknown_field":"unexpected"}`),
		}}}

		out, err := Output(run, first)
		require.NoError(t, err)
		require.Equal(t, "abc-1", out.Token)
		require.Empty(t, out.Missing)
	})

	t.Run("NewTaskRun seeds outputs keyed by task name", func(t *testing.T) {
		run, err := NewTaskRun(testInput{}, map[string]any{
			first.Name(): payload{Token: "seeded"},
		})
		require.NoError(t, err)

		out, err := Output(run, first)
		require.NoError(t, err)
		require.Equal(t, "seeded", out.Token)
	})
}
