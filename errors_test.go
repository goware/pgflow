package workflow

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPermanent(t *testing.T) {
	t.Run("nil in nil out", func(t *testing.T) {
		require.NoError(t, Permanent(nil))
	})

	t.Run("matches via errors.AsType and unwraps to the cause", func(t *testing.T) {
		cause := errors.New("boom")
		err := Permanent(cause)

		permErr, ok := errors.AsType[*PermanentError](err)
		require.True(t, ok)
		require.ErrorIs(t, permErr.Unwrap(), cause)
	})

	t.Run("matches through a wrapped error", func(t *testing.T) {
		cause := errors.New("boom")
		wrapped := fmt.Errorf("context: %w", Permanent(cause))

		permErr, ok := errors.AsType[*PermanentError](wrapped)
		require.True(t, ok)
		require.ErrorIs(t, permErr.Unwrap(), cause)
	})
}

func TestWithCode(t *testing.T) {
	cause := errors.New("boom")

	t.Run("nil in nil out", func(t *testing.T) {
		require.NoError(t, WithCode("rejected", nil))
	})

	t.Run("keeps the cause matchable", func(t *testing.T) {
		require.ErrorIs(t, WithCode("rejected", cause), cause)
	})

	t.Run("codeOf reads the code back", func(t *testing.T) {
		require.Equal(t, "rejected", codeOf(WithCode("rejected", cause)))
	})

	t.Run("codeOf is empty for an untagged error", func(t *testing.T) {
		require.Empty(t, codeOf(cause))
		require.Empty(t, codeOf(Permanent(cause)))
	})

	t.Run("codeOf reads through a wrapped error", func(t *testing.T) {
		wrapped := fmt.Errorf("context: %w", WithCode("rejected", cause))

		require.Equal(t, "rejected", codeOf(wrapped))
	})

	t.Run("composes with Permanent wrapping WithCode", func(t *testing.T) {
		err := Permanent(WithCode("rejected", cause))

		require.Equal(t, "rejected", codeOf(err))
		_, permanent := errors.AsType[*PermanentError](err)
		require.True(t, permanent)
		require.ErrorIs(t, err, cause)
	})

	t.Run("composes with WithCode wrapping Permanent", func(t *testing.T) {
		err := WithCode("rejected", Permanent(cause))

		require.Equal(t, "rejected", codeOf(err))
		_, permanent := errors.AsType[*PermanentError](err)
		require.True(t, permanent)
		require.ErrorIs(t, err, cause)
	})
}
