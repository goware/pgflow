package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/goware/workflow/store"
)

// Propagator captures the caller's context when a run starts and restores it
// around each handler, so a run resumed in another process sees the same
// values. Implement it for your own snapshot type and pass it through
// Propagate; the engine stores the snapshot as JSON.
type Propagator[T any] interface {
	Capture(ctx context.Context) (T, error)
	Restore(ctx context.Context, snapshot T) (context.Context, error)
}

// Propagate adapts p to the JSON form Config.Context takes. A snapshot that
// does not decode into T parks the run.
func Propagate[T any](p Propagator[T]) Propagator[json.RawMessage] {
	return jsonPropagator[T]{p: p}
}

type jsonPropagator[T any] struct {
	p Propagator[T]
}

func (j jsonPropagator[T]) Capture(ctx context.Context) (json.RawMessage, error) {
	v, err := j.p.Capture(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func (j jsonPropagator[T]) Restore(ctx context.Context, snapshot json.RawMessage) (context.Context, error) {
	var v T
	if err := json.Unmarshal(snapshot, &v); err != nil {
		return nil, Permanent(fmt.Errorf("workflow: decode call context: %w", err))
	}
	return j.p.Restore(ctx, v)
}

// Cipher seals a run's input and call context at rest.
type Cipher interface {
	Encrypt(ctx context.Context, plaintext string) (string, error)
	Decrypt(ctx context.Context, token string) (string, error)
}

// sealed wraps the ciphertext so open can tell a sealed document from a plain
// one without a schema change or a per-column flag.
type sealed struct {
	Token string `json:"$enc"`
}

func (m *Manager) seal(ctx context.Context, doc json.RawMessage) (json.RawMessage, error) {
	if m.cfg.Cipher == nil || len(doc) == 0 {
		return doc, nil
	}
	token, err := m.cfg.Cipher.Encrypt(ctx, string(doc))
	if err != nil {
		return nil, fmt.Errorf("workflow: seal document: %w", err)
	}
	return json.Marshal(sealed{Token: token})
}

// open leaves an unsealed document untouched, so a run written before a Cipher
// was configured still decodes.
func (m *Manager) open(ctx context.Context, doc json.RawMessage) (json.RawMessage, error) {
	var env sealed
	if err := json.Unmarshal(doc, &env); err != nil || env.Token == "" {
		return doc, nil
	}
	if m.cfg.Cipher == nil {
		return nil, Permanent(errors.New("workflow: sealed document with no cipher configured"))
	}
	plain, err := m.cfg.Cipher.Decrypt(ctx, env.Token)
	if err != nil {
		return nil, Permanent(fmt.Errorf("workflow: open document: %w", err))
	}
	return json.RawMessage(plain), nil
}

func (m *Manager) captureContext(ctx context.Context) (json.RawMessage, error) {
	if m.cfg.Context == nil {
		return nil, nil
	}
	snapshot, err := m.cfg.Context.Capture(ctx)
	if err != nil {
		return nil, fmt.Errorf("workflow: capture call context: %w", err)
	}
	return m.seal(ctx, snapshot)
}

// restoreContext wraps only the context a handler runs under, never the
// executor's own bookkeeping writes, so engine logs stay engine-scoped.
func (m *Manager) restoreContext(ctx context.Context, snapshot json.RawMessage) (context.Context, error) {
	if m.cfg.Context == nil || len(snapshot) == 0 {
		return ctx, nil
	}
	return m.cfg.Context.Restore(ctx, snapshot)
}

// openRun unseals the run's documents and restores its calling context.
func (m *Manager) openRun(ctx context.Context, run *store.WorkflowRun) (input json.RawMessage, handlerCtx context.Context, err error) {
	input, err = m.open(ctx, run.Input)
	if err != nil {
		return nil, nil, err
	}
	callCtx, err := m.open(ctx, run.CallContext)
	if err != nil {
		return nil, nil, err
	}
	handlerCtx, err = m.restoreContext(ctx, callCtx)
	if err != nil {
		return nil, nil, err
	}
	return input, handlerCtx, nil
}
