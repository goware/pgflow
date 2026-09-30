package workflow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSkipIf(t *testing.T) {
	t.Run("decodes input and evaluates the predicate", func(t *testing.T) {
		spec := noopTask("test.skip").SkipIf(func(in testInput) bool { return in.Flag })

		skipped, err := spec.core.cfg.skip(json.RawMessage(`{"flag":true}`))
		require.NoError(t, err)
		require.True(t, skipped)

		skipped, err = spec.core.cfg.skip(json.RawMessage(`{"flag":false}`))
		require.NoError(t, err)
		require.False(t, skipped)
	})

	t.Run("malformed input surfaces the decode error", func(t *testing.T) {
		spec := noopTask("test.skip").SkipIf(func(in testInput) bool { return in.Flag })
		_, err := spec.core.cfg.skip(json.RawMessage(`not json`))
		require.Error(t, err)
	})

	t.Run("nil skip when SkipIf is not called", func(t *testing.T) {
		require.Nil(t, noopTask("test.skip", MaxAttempts(3)).core.cfg.skip)
	})

	t.Run("a true predicate marks the snapshot skipped", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.skip"),
			noopTask("test.always"),
			noopTask("test.conditional").SkipIf(func(in testInput) bool { return in.Flag }),
		)))

		_, stages, ok, err := reg.snapshot("test.skip", json.RawMessage(`{"flag":true}`))
		require.NoError(t, err)
		require.True(t, ok)
		require.False(t, stages[0].skipped)
		require.True(t, stages[1].skipped)
	})
}
