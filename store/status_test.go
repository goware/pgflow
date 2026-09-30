package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowStatus(t *testing.T) {
	t.Run("persisted values are stable", func(t *testing.T) {
		require.EqualValues(t, 1, WorkflowStatusRunning)
		require.EqualValues(t, 2, WorkflowStatusSucceeded)
		require.EqualValues(t, 3, WorkflowStatusStuck)
		require.EqualValues(t, 4, WorkflowStatusCancelled)
	})

	t.Run("String renders the durable name", func(t *testing.T) {
		require.Equal(t, "running", WorkflowStatusRunning.String())
		require.Equal(t, "succeeded", WorkflowStatusSucceeded.String())
		require.Equal(t, "stuck", WorkflowStatusStuck.String())
		require.Equal(t, "cancelled", WorkflowStatusCancelled.String())
	})

	t.Run("String names the type for a value outside the set", func(t *testing.T) {
		require.Equal(t, "WorkflowStatus(0)", WorkflowStatus(0).String())
		require.Equal(t, "WorkflowStatus(9)", WorkflowStatus(9).String())
	})
}

func TestTaskStatus(t *testing.T) {
	t.Run("persisted values are stable", func(t *testing.T) {
		require.EqualValues(t, 1, TaskStatusPending)
		require.EqualValues(t, 2, TaskStatusRunning)
		require.EqualValues(t, 3, TaskStatusSucceeded)
		require.EqualValues(t, 4, TaskStatusStuck)
		require.EqualValues(t, 5, TaskStatusSkipped)
	})

	t.Run("String renders the durable name", func(t *testing.T) {
		require.Equal(t, "pending", TaskStatusPending.String())
		require.Equal(t, "running", TaskStatusRunning.String())
		require.Equal(t, "succeeded", TaskStatusSucceeded.String())
		require.Equal(t, "stuck", TaskStatusStuck.String())
		require.Equal(t, "skipped", TaskStatusSkipped.String())
	})

	t.Run("String names the type for a value outside the set", func(t *testing.T) {
		require.Equal(t, "TaskStatus(0)", TaskStatus(0).String())
		require.Equal(t, "TaskStatus(9)", TaskStatus(9).String())
	})
}
