package workflow

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0xsequence/runnable"
	"github.com/0xsequence/runnable/adapters"
	"github.com/google/uuid"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"

	"github.com/goware/workflow/store"
)

// awaitPollInterval is Await's fallback poll cadence. Correctness rests on
// this poll; the in-memory completion channel is only a latency win.
const awaitPollInterval = time.Second

// LevelAlert marks a run the engine parked and will not retry on its own.
// It is above slog.LevelError and equals go-libs alert.LevelAlert.
const LevelAlert = slog.Level(16)

// Manager starts, executes, and observes workflow runs backed by the store.
type Manager struct {
	db            *store.Store
	reg           *Registry
	workflowNames []string
	cfg           Config
	log           *slog.Logger

	sem      chan struct{}
	wg       sync.WaitGroup
	drainMu  sync.RWMutex
	draining atomic.Bool

	mu      sync.Mutex
	waiters map[uuid.UUID]*completion
}

// completion signals one in-flight run's terminal status. kickOnce makes
// concurrent Awaits on a StartTx run launch runLocal at most once.
type completion struct {
	done     chan struct{}
	status   Status
	once     sync.Once
	kickOnce sync.Once
}

// NewManager freezes reg and returns a Manager over db. It fails unless the
// widest task timeout plus MaxInProcessBackoff is below SweepMaxAge, so the
// sweeper never reclaims a run inside a live task attempt.
func NewManager(ctx context.Context, db *pgkit.DB, reg *Registry, cfg Config, log *slog.Logger) (*Manager, error) {
	cfg = withDefaults(cfg)
	reg.freeze()

	maxTimeout := reg.maxTaskTimeout(cfg)
	if window := maxTimeout + cfg.MaxInProcessBackoff; window >= cfg.SweepMaxAge {
		return nil, fmt.Errorf(
			"workflow: max task timeout (%s) + max in-process backoff (%s) must be < sweep max age (%s)",
			maxTimeout, cfg.MaxInProcessBackoff, cfg.SweepMaxAge,
		)
	}

	st := store.New(db)
	version, err := st.SchemaVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("workflow: %w", err)
	}
	if version < store.SchemaVersion {
		return nil, fmt.Errorf("workflow: %w: %s at version %d, code expects %d",
			store.ErrSchemaOutdated, store.VersionTable, version, store.SchemaVersion)
	}

	return &Manager{
		db:            st,
		reg:           reg,
		workflowNames: reg.names(),
		cfg:           cfg,
		log:           log,
		sem:           make(chan struct{}, cfg.MaxLocalRuns),
		waiters:       make(map[uuid.UUID]*completion),
	}, nil
}

func withDefaults(cfg Config) Config {
	cfg.DefaultTaskTimeout = cmp.Or(cfg.DefaultTaskTimeout, 60*time.Second)
	cfg.DefaultMaxAttempts = cmp.Or(cfg.DefaultMaxAttempts, 8)
	cfg.BackoffBase = cmp.Or(cfg.BackoffBase, time.Second)
	cfg.BackoffMax = cmp.Or(cfg.BackoffMax, 5*time.Minute)
	cfg.MaxInProcessBackoff = cmp.Or(cfg.MaxInProcessBackoff, 5*time.Second)
	cfg.SweepMaxAge = cmp.Or(cfg.SweepMaxAge, 10*time.Minute)
	cfg.MaxLocalRuns = cmp.Or(cfg.MaxLocalRuns, 64)
	return cfg
}

// Draining reports whether the Manager is shutting down.
func (m *Manager) Draining() bool {
	return m.draining.Load()
}

// Start durably enqueues a run of the workflow and launches it in-process
// when a local slot is free; otherwise the poller drives it.
func (d WorkflowDef[In]) Start(ctx context.Context, m *Manager, input In, opts ...StartOption) (*WorkflowRun, error) {
	return m.start(ctx, d.name, input, opts...)
}

// StartTx inserts the run inside tx, visible once the caller commits. It does
// not launch the run: the Manager cannot observe the commit, so the first
// Await does.
func (d WorkflowDef[In]) StartTx(ctx context.Context, m *Manager, tx pgx.Tx, input In, opts ...StartOption) (*WorkflowRun, error) {
	return m.startTx(ctx, tx, d.name, input, opts...)
}

// Input returns the input run was started with. After a duplicate start it is
// the first start's input, not the caller's.
func (d WorkflowDef[In]) Input(ctx context.Context, run *WorkflowRun) (in In, err error) {
	row, err := run.mgr.db.GetRun(ctx, run.id)
	if err != nil {
		return in, fmt.Errorf("workflow: get run %s: %w", run.id, err)
	}
	if row.Name != d.name {
		return in, fmt.Errorf("workflow: run %s is %q, not %q", run.id, row.Name, d.name)
	}
	raw, err := run.mgr.open(ctx, row.Input)
	if err != nil {
		return in, fmt.Errorf("workflow: open input for run %s: %w", run.id, err)
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return in, fmt.Errorf("workflow: decode input for run %s: %w", run.id, err)
	}
	return in, nil
}

func (m *Manager) start(ctx context.Context, workflowName string, input any, opts ...StartOption) (*WorkflowRun, error) {
	cfg := resolveStart(opts)
	run, tasks, err := m.buildRun(ctx, workflowName, input, cfg.idempotencyKey)
	if err != nil {
		return nil, err
	}

	runID := run.ID
	if err := pgx.BeginFunc(ctx, m.db.DB.Conn, func(tx pgx.Tx) error {
		if cfg.idempotencyKey == "" {
			return m.db.WithTx(tx).InsertRun(ctx, run, tasks)
		}
		id, _, err := m.db.WithTx(tx).UpsertRun(ctx, run, tasks)
		runID = id
		return err
	}); err != nil {
		return nil, fmt.Errorf("workflow: insert run %q: %w", workflowName, err)
	}

	c := m.register(runID)
	m.kick(ctx, runID, c)
	return &WorkflowRun{id: runID, mgr: m}, nil
}

func (m *Manager) startTx(ctx context.Context, tx pgx.Tx, workflowName string, input any, opts ...StartOption) (*WorkflowRun, error) {
	cfg := resolveStart(opts)
	run, tasks, err := m.buildRun(ctx, workflowName, input, cfg.idempotencyKey)
	if err != nil {
		return nil, err
	}

	runID := run.ID
	if cfg.idempotencyKey == "" {
		if err := m.db.WithTx(tx).InsertRun(ctx, run, tasks); err != nil {
			return nil, fmt.Errorf("workflow: insert run %q: %w", workflowName, err)
		}
	} else {
		id, _, err := m.db.WithTx(tx).UpsertRun(ctx, run, tasks)
		if err != nil {
			return nil, fmt.Errorf("workflow: insert run %q: %w", workflowName, err)
		}
		runID = id
	}

	m.register(runID)
	return &WorkflowRun{id: runID, mgr: m}, nil
}

func (m *Manager) buildRun(ctx context.Context, workflowName string, input any, idempotencyKey string) (*store.WorkflowRun, []*store.TaskRun, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow: marshal input for run %q: %w", workflowName, err)
	}

	name, snap, ok, err := m.reg.snapshot(workflowName, raw)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("workflow: unknown run %q", workflowName)
	}

	// Seal after snapshot: SkipIf predicates read the plaintext input.
	callCtx, err := m.captureContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	stored, err := m.seal(ctx, raw)
	if err != nil {
		return nil, nil, err
	}

	runID := uuid.New()
	run := &store.WorkflowRun{
		ID:          runID,
		Name:        name,
		Input:       stored,
		CallContext: callCtx,
		Status:      StatusRunning,
	}
	if idempotencyKey != "" {
		run.IdempotencyKey = &idempotencyKey
	}

	tasks := make([]*store.TaskRun, len(snap))
	for i, s := range snap {
		status := store.TaskStatusPending
		if s.skipped {
			status = store.TaskStatusSkipped
		}
		tasks[i] = &store.TaskRun{
			RunID:   runID,
			Seq:     int16(i),
			Handler: s.handlerName,
			Status:  status,
		}
	}
	return run, tasks, nil
}

func (m *Manager) register(runID uuid.UUID) *completion {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.waiters[runID]; ok {
		return c
	}
	c := &completion{done: make(chan struct{})}
	m.waiters[runID] = c
	return c
}

// kick is shared by Start and a StartTx run's first Await, so a run launches
// once whichever fires first.
func (m *Manager) kick(ctx context.Context, runID uuid.UUID, c *completion) {
	c.kickOnce.Do(func() { m.launch(ctx, runID) })
}

// launch skips when the semaphore is full or a drain has started; the poller
// covers the run either way. It holds drainMu's read lock from the draining
// check through wg.Go, so a launch is either registered with the WaitGroup
// before startDrain takes the write lock, and therefore waited for, or it sees
// draining and refuses. It can never slip in between.
func (m *Manager) launch(ctx context.Context, runID uuid.UUID) {
	select {
	case m.sem <- struct{}{}:
	default:
		return
	}

	m.drainMu.RLock()
	defer m.drainMu.RUnlock()

	if m.draining.Load() {
		<-m.sem
		return
	}
	runCtx := context.WithoutCancel(ctx)
	m.wg.Go(func() {
		defer func() { <-m.sem }()
		m.runLocal(runCtx, runID)
	})
}

// startDrain must precede every m.wg.Wait: its write lock waits out every
// in-flight launch's wg.Add, and every later launch is refused.
func (m *Manager) startDrain() {
	m.drainMu.Lock()
	defer m.drainMu.Unlock()
	m.draining.Store(true)
}

// runLocal treats a missed claim as a no-op: another executor or the poller
// owns the run.
func (m *Manager) runLocal(ctx context.Context, runID uuid.UUID) {
	claimed, ok, err := m.db.ClaimRunByID(ctx, runID)
	if err != nil {
		m.log.ErrorContext(ctx, "workflow: fast-path claim failed",
			slog.String("run_id", runID.String()), slog.Any("error", err))
		return
	}
	if !ok {
		return
	}
	m.executePlan(ctx, claimed)
}

func (m *Manager) signal(runID uuid.UUID, s Status) {
	m.mu.Lock()
	c, ok := m.waiters[runID]
	if ok {
		delete(m.waiters, runID)
	}
	m.mu.Unlock()
	if !ok {
		return
	}
	c.once.Do(func() {
		c.status = s
		close(c.done)
	})
}

// await returns StatusRunning when ctx expires before the run is terminal;
// the run keeps going in the background.
func (m *Manager) await(ctx context.Context, runID uuid.UUID) (Status, error) {
	m.mu.Lock()
	c := m.waiters[runID]
	m.mu.Unlock()

	var done <-chan struct{}
	if c != nil {
		done = c.done
		// A StartTx run launches here; for a Start run kickOnce makes this a no-op.
		m.kick(ctx, runID, c)
	}

	ticker := time.NewTicker(awaitPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return c.status, nil
		case <-ticker.C:
			run, err := m.db.GetRun(ctx, runID)
			if err != nil {
				return awaitPollFailure(ctx, err)
			}
			if run.Status != StatusRunning {
				return run.Status, nil
			}
		case <-ctx.Done():
			return StatusRunning, nil
		}
	}
}

// awaitPollFailure treats a poll cancelled by its own deadline as still
// running rather than as a caller error.
func awaitPollFailure(ctx context.Context, err error) (Status, error) {
	if ctx.Err() != nil {
		return StatusRunning, nil
	}
	return 0, err
}

// status reads the database only, never in-memory state.
func (m *Manager) status(ctx context.Context, runID uuid.UUID) (Status, error) {
	run, err := m.db.GetRun(ctx, runID)
	if err != nil {
		return 0, err
	}
	return run.Status, nil
}

func (m *Manager) failure(ctx context.Context, runID uuid.UUID) (*Failure, error) {
	run, err := m.db.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.Status != StatusStuck {
		return nil, nil
	}
	tasks, err := m.db.GetTasks(ctx, runID)
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		if task.Status != store.TaskStatusStuck {
			continue
		}
		f := &Failure{Task: task.Handler, Attempts: task.AttemptCount}
		if task.LastError != nil {
			f.Error = *task.LastError
		}
		if task.LastErrorCode != nil {
			f.Code = *task.LastErrorCode
		}
		return f, nil
	}
	return nil, nil
}

// tickDrainBudget bounds one poll or sweep tick, not the executions it
// dispatches: those drain through m.wg with no deadline.
const tickDrainBudget = 5 * time.Second

func withRunnerDefaults(cfg RunnerConfig) RunnerConfig {
	cfg.PollInterval = cmp.Or(cfg.PollInterval, 30*time.Second)
	cfg.BatchSize = cmp.Or(cfg.BatchSize, 10)
	cfg.Concurrency = cmp.Or(cfg.Concurrency, 4)
	cfg.SweepInterval = cmp.Or(cfg.SweepInterval, time.Minute)
	cfg.ClaimStrategy = cmp.Or(cfg.ClaimStrategy, store.ClaimStrategyFIFO)
	return cfg
}

// NewRunnable returns the poll loop and, when cfg.SweepEnabled, the sweep loop.
// On shutdown it waits for every in-flight execution before returning.
func (m *Manager) NewRunnable(cfg RunnerConfig) runnable.Runnable {
	cfg = withRunnerDefaults(cfg)

	return runnable.New(func(ctx context.Context) error {
		// Fast-path runs use context.WithoutCancel and observe shutdown only
		// through Draining, so flip it as soon as ctx is done.
		stop := context.AfterFunc(ctx, m.startDrain)
		defer stop()

		runners := []runnable.Runnable{
			runnable.New(func(ctx context.Context) error {
				if err := m.pollOnce(ctx, cfg); err != nil {
					if ctx.Err() != nil {
						return err
					}
					m.log.ErrorContext(ctx, "workflow: poll tick failed", slog.Any("error", err))
				}
				return nil
			}, runnable.WithAdapters(adapters.Draining(tickDrainBudget), adapters.Ticker(cfg.PollInterval))),
		}
		if cfg.SweepEnabled {
			runners = append(runners, runnable.New(func(ctx context.Context) error {
				if err := m.sweepOnce(ctx); err != nil {
					if ctx.Err() != nil {
						return err
					}
					m.log.ErrorContext(ctx, "workflow: sweep tick failed", slog.Any("error", err))
				}
				return nil
			}, runnable.WithAdapters(adapters.Draining(tickDrainBudget), adapters.Ticker(cfg.SweepInterval))))
		}

		err := runnable.NewGroup(runners...).Run(ctx)
		m.log.InfoContext(ctx, "workflow: shutting down — draining in-flight executions")
		// The startDrain above ran on the AfterFunc goroutine. Calling it again
		// on this goroutine is what guarantees every fast-path launch is
		// registered before this wg.Wait.
		m.startDrain()
		m.wg.Wait()
		m.log.InfoContext(ctx, "workflow: all executions finished")
		return err
	})
}

// pollOnce runs executions under ctx as-is: unlike the fast path's request
// context, it is the runnable's lifecycle context, so its cancellation is the
// shutdown signal executePlan observes. The semaphore is per tick, so long
// runs can overlap the next tick's batch.
func (m *Manager) pollOnce(ctx context.Context, cfg RunnerConfig) error {
	claimed, err := m.db.ClaimRuns(ctx, cfg.ClaimStrategy, m.workflowNames, cfg.BatchSize)
	if err != nil {
		return err
	}

	sem := make(chan struct{}, cfg.Concurrency)
	for _, c := range claimed {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return nil
		}
		m.wg.Go(func() {
			defer func() { <-sem }()
			m.executePlan(ctx, c)
		})
	}
	return nil
}

// sweepOnce releases claims older than Config.SweepMaxAge, not
// RunnerConfig.SweepInterval, which is only the tick cadence. It is safe across
// replicas: SweepStuck is a guarded UPDATE on the database clock.
func (m *Manager) sweepOnce(ctx context.Context) error {
	n, err := m.db.SweepStuck(ctx, m.cfg.SweepMaxAge)
	if err != nil {
		return err
	}
	if n > 0 {
		m.log.InfoContext(ctx, "workflow: swept stale claims", slog.Int64("count", n))
	}
	return nil
}
