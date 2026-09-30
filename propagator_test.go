package workflow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goware/workflow/store"
)

// base64Cipher proves the engine seals and opens without a real key.
type base64Cipher struct{}

func (base64Cipher) Encrypt(_ context.Context, plaintext string) (string, error) {
	return base64.StdEncoding.EncodeToString([]byte(plaintext)), nil
}

func (base64Cipher) Decrypt(_ context.Context, token string) (string, error) {
	plain, err := base64.StdEncoding.DecodeString(token)
	return string(plain), err
}

type markerKey struct{}

type markerPropagator struct{}

func (markerPropagator) Capture(ctx context.Context) (string, error) {
	marker, _ := ctx.Value(markerKey{}).(string)
	return marker, nil
}

func (markerPropagator) Restore(ctx context.Context, marker string) (context.Context, error) {
	return context.WithValue(ctx, markerKey{}, marker), nil
}

func markerRegistry(t *testing.T, name string, seen chan<- string) *Registry {
	t.Helper()
	reg := NewRegistry()
	require.NoError(t, reg.Register(Define(NewWorkflow[testInput](name),
		Handle(NewTask[None]("test.marker"), func(ctx context.Context, _ *TaskRun[testInput]) (None, error) {
			marker, _ := ctx.Value(markerKey{}).(string)
			seen <- marker
			return None{}, nil
		}),
	)))
	return reg
}

func TestSeal(t *testing.T) {
	ctx := t.Context()

	t.Run("round-trips a document", func(t *testing.T) {
		mgr := &Manager{cfg: Config{Cipher: base64Cipher{}}}
		stored, err := mgr.seal(ctx, json.RawMessage(`{"seed":"secret"}`))
		require.NoError(t, err)
		assert.NotContains(t, string(stored), "secret")

		opened, err := mgr.open(ctx, stored)
		require.NoError(t, err)
		assert.JSONEq(t, `{"seed":"secret"}`, string(opened))
	})

	t.Run("leaves an unsealed document untouched", func(t *testing.T) {
		mgr := &Manager{cfg: Config{Cipher: base64Cipher{}}}
		opened, err := mgr.open(ctx, json.RawMessage(`{"seed":"plain"}`))
		require.NoError(t, err)
		assert.JSONEq(t, `{"seed":"plain"}`, string(opened))
	})

	t.Run("is permanent when no cipher can open it", func(t *testing.T) {
		sealer := &Manager{cfg: Config{Cipher: base64Cipher{}}}
		stored, err := sealer.seal(ctx, json.RawMessage(`{"seed":"secret"}`))
		require.NoError(t, err)

		_, err = (&Manager{}).open(ctx, stored)
		var permanent *PermanentError
		require.ErrorAs(t, err, &permanent)
	})
}

func TestPropagator(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	t.Run("restores the calling context for a task running off the fast path", func(t *testing.T) {
		seen := make(chan string, 1)
		mgr := newManager(t, db, markerRegistry(t, "test.propagate", seen),
			func(c *Config) { c.Context = Propagate(markerPropagator{}) })

		run := seedPlan(t, db, context.WithValue(ctx, markerKey{}, "caller-42"), mgr, "test.propagate", testInput{Seed: "x"})
		// A bare context stands in for the worker: it carries no caller of its own.
		require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, claim(t, db, ctx, run.ID)))
		assert.Equal(t, "caller-42", <-seen)
	})

	t.Run("seals the calling context and the input at rest", func(t *testing.T) {
		seen := make(chan string, 1)
		mgr := newManager(t, db, markerRegistry(t, "test.propagate.sealed", seen),
			func(c *Config) { c.Context = Propagate(markerPropagator{}); c.Cipher = base64Cipher{} })

		run := seedPlan(t, db, context.WithValue(ctx, markerKey{}, "zz-secret-caller"), mgr,
			"test.propagate.sealed", testInput{Seed: "zz-secret-seed"})

		stored, err := db.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.NotContains(t, string(stored.Input), "zz-secret-seed")
		assert.NotContains(t, string(stored.CallContext), "zz-secret-caller")

		require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, claim(t, db, ctx, run.ID)))
		assert.Equal(t, "zz-secret-caller", <-seen)
	})

	t.Run("parks a run whose input cannot be opened without spending an attempt", func(t *testing.T) {
		seen := make(chan string, 1)
		reg := markerRegistry(t, "test.propagate.unopenable", seen)
		sealer := newManager(t, db, reg, func(c *Config) { c.Cipher = base64Cipher{} })
		run := seedPlan(t, db, ctx, sealer, "test.propagate.unopenable", testInput{Seed: "x"})

		// The reader has no cipher, so the sealed input never opens.
		reader := newManager(t, db, reg)
		require.Equal(t, StatusStuck, reader.executePlan(ctx, claim(t, db, ctx, run.ID)))

		tasks, err := db.GetTasks(ctx, run.ID)
		require.NoError(t, err)
		require.NotEmpty(t, tasks)
		assert.Equal(t, store.TaskStatusStuck, tasks[0].Status)
		assert.Equal(t, 0, tasks[0].AttemptCount)
	})

	t.Run("parks a run whose call context does not decode without spending an attempt", func(t *testing.T) {
		seen := make(chan string, 1)
		mgr := newManager(t, db, markerRegistry(t, "test.propagate.undecodable", seen),
			func(c *Config) { c.Context = Propagate(markerPropagator{}) })
		run := seedPlan(t, db, ctx, mgr, "test.propagate.undecodable", testInput{Seed: "x"})
		_, err := db.DB.Conn.Exec(ctx, "UPDATE "+store.TableRuns+" SET call_context = '[1,2]' WHERE id = $1", run.ID)
		require.NoError(t, err)

		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, run.ID)))

		tasks, err := db.GetTasks(ctx, run.ID)
		require.NoError(t, err)
		require.NotEmpty(t, tasks)
		assert.Equal(t, store.TaskStatusStuck, tasks[0].Status)
		assert.Equal(t, 0, tasks[0].AttemptCount)
	})
}

func TestAwaitPollFailure(t *testing.T) {
	sentinel := errors.New("boom")

	t.Run("surfaces a live failure", func(t *testing.T) {
		status, err := awaitPollFailure(t.Context(), sentinel)
		require.ErrorIs(t, err, sentinel)
		assert.Equal(t, Status(0), status)
	})

	t.Run("reports a budget expiry as still running", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		status, err := awaitPollFailure(ctx, sentinel)
		require.NoError(t, err)
		assert.Equal(t, StatusRunning, status)
	})
}

func TestPropagate(t *testing.T) {
	ctx := t.Context()
	p := Propagate(markerPropagator{})

	t.Run("encodes the captured value as JSON and decodes it on restore", func(t *testing.T) {
		snapshot, err := p.Capture(context.WithValue(ctx, markerKey{}, "caller-7"))
		require.NoError(t, err)
		assert.JSONEq(t, `"caller-7"`, string(snapshot))

		restored, err := p.Restore(ctx, snapshot)
		require.NoError(t, err)
		assert.Equal(t, "caller-7", restored.Value(markerKey{}))
	})

	t.Run("is permanent when the snapshot does not decode", func(t *testing.T) {
		_, err := p.Restore(ctx, json.RawMessage(`[1,2]`))
		var permanent *PermanentError
		require.ErrorAs(t, err, &permanent)
	})
}
