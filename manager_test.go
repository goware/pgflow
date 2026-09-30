package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/goware/pgkit/v2/pgerr"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/goware/workflow/internal/pgtest"
	"github.com/goware/workflow/store"
)

type testInput struct {
	Seed string `json:"seed"`
	Flag bool   `json:"flag"`
}

func testDB(t *testing.T) *store.Store {
	t.Helper()
	db := pgtest.Scratch(t)
	require.NoError(t, store.Migrate(t.Context(), pgtest.SQL(t, db)))
	return store.New(db)
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func newManager(t *testing.T, db *store.Store, reg *Registry, overrides ...func(*Config)) *Manager {
	t.Helper()
	var cfg Config
	for _, o := range overrides {
		o(&cfg)
	}
	mgr, err := NewManager(t.Context(), db.DB, reg, cfg, testLogger())
	require.NoError(t, err)
	return mgr
}

func twoStageRegistry(t *testing.T) *Registry {
	t.Helper()
	reg := NewRegistry()
	err := reg.Register(Define(NewWorkflow[testInput]("test.twostage"),
		mapTask("test.first", map[string]string{"v": "1"}),
		mapTask("test.second", map[string]string{"v": "2"}),
	))
	require.NoError(t, err)
	return reg
}

// trackedTask records peak handler concurrency and holds its slot long enough
// that an unbounded fan-out overlaps rather than serialising by accident.
func trackedTask(name string, inFlight, peak *atomic.Int64) TaskSpec[testInput] {
	return Handle(NewTask[None](name), func(context.Context, *TaskRun[testInput]) (None, error) {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			high := peak.Load()
			if cur <= high || peak.CompareAndSwap(high, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		return None{}, nil
	})
}

// reopenLaunches lets one Manager be drained repeatedly; production drains once.
func reopenLaunches(m *Manager) {
	m.drainMu.Lock()
	defer m.drainMu.Unlock()
	m.draining.Store(false)
}

func TestManager(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	t.Run("NewManager rejects a stage timeout that would outlive the sweep", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.slow"),
			noopTask("test.slow", Timeout(11*time.Minute)),
		)))
		_, err := NewManager(t.Context(), db.DB, reg, Config{SweepMaxAge: 10 * time.Minute}, testLogger())
		require.Error(t, err)
	})

	t.Run("NewManager refuses a database with no store schema", func(t *testing.T) {
		reg := NewRegistry()
		_, err := NewManager(t.Context(), pgtest.Scratch(t), reg, Config{}, testLogger())
		require.ErrorIs(t, err, store.ErrSchemaOutdated)
		require.ErrorContains(t, err, "at version 0, code expects 1")
	})

	t.Run("NewManager refuses a database at an older store version", func(t *testing.T) {
		db := pgtest.Scratch(t)
		sqlDB := pgtest.SQL(t, db)
		require.NoError(t, store.Migrate(t.Context(), sqlDB))
		_, err := db.Conn.Exec(t.Context(), "DELETE FROM "+store.VersionTable+" WHERE version_id = 1")
		require.NoError(t, err)

		_, err = NewManager(t.Context(), db, NewRegistry(), Config{}, testLogger())
		require.ErrorIs(t, err, store.ErrSchemaOutdated)
		require.ErrorContains(t, err, "at version 0, code expects 1")
	})

	t.Run("NewManager fills defaults and freezes the registry", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.noop"),
			noopTask("test.noop"),
		)))
		mgr, err := NewManager(t.Context(), db.DB, reg, Config{}, testLogger())
		require.NoError(t, err)
		require.Equal(t, 64, mgr.cfg.MaxLocalRuns)
		require.Equal(t, 60*time.Second, mgr.cfg.DefaultTaskTimeout)
		require.Equal(t, 64, cap(mgr.sem))

		err = reg.Register(Define(NewWorkflow[testInput]("test.late"),
			noopTask("test.late"),
		))
		require.Error(t, err)
	})

	t.Run("withRunnerDefaults uses FIFO claim order", func(t *testing.T) {
		cfg := withRunnerDefaults(RunnerConfig{})

		require.Equal(t, store.ClaimStrategyFIFO, cfg.ClaimStrategy)
	})

	t.Run("pollOnce claims with an unset strategy filled by the default", func(t *testing.T) {
		mgr := newManager(t, db, NewRegistry())
		_, err := db.DB.Conn.Exec(ctx, "TRUNCATE workflow_runs, workflow_tasks")
		require.NoError(t, err)
		cfg := withRunnerDefaults(RunnerConfig{BatchSize: 1, Concurrency: 1})

		// An unset strategy surviving withRunnerDefaults would surface here as an error.
		require.NoError(t, mgr.pollOnce(ctx, cfg))
	})

	t.Run("NewRunnable mounts inert with an empty registry and leaves a due run alone", func(t *testing.T) {
		plan := seedPlan(t, db, ctx, newManager(t, db, twoStageRegistry(t)), "test.twostage", testInput{Seed: "inert"})

		r := newManager(t, db, NewRegistry()).NewRunnable(RunnerConfig{
			PollInterval:  10 * time.Millisecond,
			SweepEnabled:  true,
			SweepInterval: 10 * time.Millisecond,
		})
		runCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		require.NoError(t, r.Run(runCtx))

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusRunning, got.Status)
		require.Nil(t, got.ClaimedAt)
		require.Nil(t, got.ClaimID)
	})

	t.Run("StartTx rows are invisible until the caller commits", func(t *testing.T) {
		mgr := newManager(t, db, twoStageRegistry(t))

		var runID uuid.UUID
		err := pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			p, err := mgr.startTx(ctx, tx, "test.twostage", testInput{Seed: "abc"})
			if err != nil {
				return err
			}
			runID = p.ID()
			// A pool-scoped read runs on a separate connection, so it cannot
			// see the still-uncommitted plan row.
			_, err = db.GetRun(ctx, runID)
			require.Error(t, err)
			return nil
		})
		require.NoError(t, err)

		got, err := db.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, StatusRunning, got.Status)
	})

	t.Run("StartTx idempotent enqueue rolls back with the caller", func(t *testing.T) {
		mgr := newManager(t, db, twoStageRegistry(t))
		tx, err := db.DB.Conn.Begin(ctx)
		require.NoError(t, err)

		p, err := mgr.startTx(ctx, tx, "test.twostage", testInput{Seed: "abc"},
			WithIdempotencyKey("rollback:"+uuid.NewString()))
		require.NoError(t, err)
		require.NoError(t, tx.Rollback(ctx))

		_, err = db.GetRun(ctx, p.ID())
		require.True(t, pgerr.IsErrorNoRows(err))
	})

	t.Run("StartTx on a duplicate key leaves a parked run parked", func(t *testing.T) {
		mgr := newManager(t, db, twoStageRegistry(t))
		key := "parked:" + uuid.NewString()
		var plan *WorkflowRun
		err := pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			var err error
			plan, err = mgr.startTx(
				ctx,
				tx,
				"test.twostage",
				testInput{Seed: "abc"},
				WithIdempotencyKey(key),
			)
			return err
		})
		require.NoError(t, err)
		_, err = db.DB.Query.Exec(ctx, db.DB.SQL.Update(store.TableRuns).
			Set("status", StatusStuck).
			Where(squirrel.Eq{"id": plan.ID()}))
		require.NoError(t, err)
		_, err = db.DB.Query.Exec(ctx, db.DB.SQL.Update(store.TableTasks).
			Set("status", store.TaskStatusStuck).
			Set("attempt_count", 8).
			Set("last_error", "boom").
			Where(squirrel.Eq{"run_id": plan.ID()}))
		require.NoError(t, err)

		var dup *WorkflowRun
		err = pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			var err error
			dup, err = mgr.startTx(ctx, tx, "test.twostage", testInput{Seed: "abc"}, WithIdempotencyKey(key))
			return err
		})
		require.NoError(t, err)
		require.Equal(t, plan.ID(), dup.ID())

		gotPlan, err := db.GetRun(ctx, plan.ID())
		require.NoError(t, err)
		require.Equal(t, StatusStuck, gotPlan.Status)
		stages, err := db.GetTasks(ctx, plan.ID())
		require.NoError(t, err)
		require.Len(t, stages, 2)
		for _, stage := range stages {
			require.Equal(t, store.TaskStatusStuck, stage.Status)
			require.Equal(t, 8, stage.AttemptCount)
			require.NotNil(t, stage.LastError)
			require.Equal(t, "boom", *stage.LastError)
		}
	})

	t.Run("Start enqueues a durable running plan", func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		signalStarted := sync.OnceFunc(func() { close(started) })
		releaseHandler := sync.OnceFunc(func() { close(release) })
		// Registered before any assertion can fail: the handler's ctx is
		// WithoutCancel, so nothing else would unblock it.
		t.Cleanup(releaseHandler)
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.gated"),
			Handle(NewTask[None]("test.gatedfirst"), func(ctx context.Context, _ *TaskRun[testInput]) (None, error) {
				signalStarted()
				select {
				case <-release:
					return None{}, nil
				case <-ctx.Done():
					return None{}, ctx.Err()
				}
			}),
			mapTask("test.gatedsecond", map[string]string{"v": "2"}),
		)))
		mgr := newManager(t, db, reg)

		p, err := mgr.start(ctx, "test.gated", testInput{Seed: "abc"})
		require.NoError(t, err)

		// Read the row while the first handler is provably mid-flight, so it
		// shows the enqueued state rather than one the runner already left.
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			require.Fail(t, "handler never signaled started; fast-path launch was not taken")
		}
		got, err := db.GetRun(ctx, p.ID())
		require.NoError(t, err)
		require.Equal(t, StatusRunning, got.Status)
		require.Equal(t, int16(0), got.CurrentTask)

		releaseHandler()
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		status, err := p.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, status)
	})

	t.Run("a Start burst above MaxLocalRuns launches no unbounded goroutines", func(t *testing.T) {
		var inFlight, peak atomic.Int64
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.twostage"),
			trackedTask("test.first", &inFlight, &peak),
			trackedTask("test.second", &inFlight, &peak),
		)))
		mgr := newManager(t, db, reg, func(c *Config) { c.MaxLocalRuns = 1 })

		const n = 20
		ids := make([]uuid.UUID, 0, n)
		for i := range n {
			p, err := mgr.start(ctx, "test.twostage", testInput{Seed: fmt.Sprintf("s%d", i)})
			require.NoError(t, err)
			ids = append(ids, p.ID())
		}

		mgr.startDrain()
		mgr.wg.Wait()

		require.Equal(t, 1, cap(mgr.sem))
		require.Positive(t, peak.Load(), "the burst must have executed at least one handler")
		require.LessOrEqualf(t, peak.Load(), int64(mgr.cfg.MaxLocalRuns),
			"peak concurrent handler executions must stay within MaxLocalRuns=%d", mgr.cfg.MaxLocalRuns)

		// A plan that got a slot succeeds; the rest stay running for a poller.
		for _, id := range ids {
			got, err := db.GetRun(ctx, id)
			require.NoError(t, err)
			require.Contains(t, []Status{StatusRunning, StatusSucceeded}, got.Status)
		}
	})

	t.Run("a launch after the drain barrier is refused", func(t *testing.T) {
		mgr := newManager(t, db, twoStageRegistry(t))
		run, tasks, err := mgr.buildRun(ctx, "test.twostage", testInput{Seed: "abc"}, "")
		require.NoError(t, err)
		require.NoError(t, pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			return db.InsertRun(ctx, run, tasks)
		}))

		mgr.startDrain()
		mgr.launch(ctx, run.ID)
		mgr.wg.Wait()

		require.Empty(t, mgr.sem, "a refused launch must not hold a semaphore slot")
		_, claimable, err := db.ClaimRunByID(ctx, run.ID)
		require.NoError(t, err)
		require.True(t, claimable, "a refused launch must leave the run unclaimed for the poller")
	})

	t.Run("launches racing a drain never race the wait", func(t *testing.T) {
		mgr := newManager(t, db, NewRegistry())

		var stop atomic.Bool
		var hammers sync.WaitGroup
		for range 8 {
			hammers.Go(func() {
				for !stop.Load() {
					// Each launch of an unknown ID is one short claim attempt,
					// so the WaitGroup keeps returning to zero while drains
					// run. That is the race this test exercises.
					mgr.launch(ctx, uuid.New())
				}
			})
		}

		// Deadline-bound so a slow CI database can't balloon the runtime.
		const minCycles = 200
		deadline := time.Now().Add(4 * time.Second)
		for i := 0; i < minCycles || time.Now().Before(deadline); i++ {
			mgr.startDrain()
			mgr.wg.Wait()
			reopenLaunches(mgr)
		}

		stop.Store(true)
		hammers.Wait()
		mgr.startDrain()
		mgr.wg.Wait()

		require.Empty(t, mgr.sem, "every drained launch must have released its slot")
	})

	t.Run("a freshly-inserted plan is immediately claimable", func(t *testing.T) {
		mgr := newManager(t, db, twoStageRegistry(t))

		// Claim at the data layer so no runLocal goroutine races it: a fresh
		// row's default next_retry_at must not land in the future.
		for i := range 25 {
			plan, stages, err := mgr.buildRun(ctx, "test.twostage", testInput{Seed: fmt.Sprintf("s%d", i)}, "")
			require.NoError(t, err)
			require.NoError(t, pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
				return db.InsertRun(ctx, plan, stages)
			}))

			_, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.Truef(t, ok, "iteration %d: a freshly-inserted plan must be immediately claimable", i)
		}
	})

	t.Run("Await returns Succeeded for a same-process run", func(t *testing.T) {
		mgr := newManager(t, db, twoStageRegistry(t))
		p, err := mgr.start(ctx, "test.twostage", testInput{Seed: "abc"})
		require.NoError(t, err)

		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		status, err := p.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, status)
	})

	t.Run("Start collapses a duplicate idempotency key onto one plan", func(t *testing.T) {
		mgr := newManager(t, db, twoStageRegistry(t))
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		p1, err := mgr.start(ctx, "test.twostage", testInput{Seed: "abc"}, WithIdempotencyKey("dup:1"))
		require.NoError(t, err)
		status, err := p1.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, status)

		p2, err := mgr.start(ctx, "test.twostage", testInput{Seed: "abc"}, WithIdempotencyKey("dup:1"))
		require.NoError(t, err)
		require.Equal(t, p1.ID(), p2.ID())
	})

	t.Run("Await returns Running when the ctx budget expires", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.slow"),
			Handle(NewTask[map[string]string]("test.slow"),
				func(ctx context.Context, _ *TaskRun[testInput]) (map[string]string, error) {
					select {
					case <-time.After(200 * time.Millisecond):
						return map[string]string{"v": "slow"}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}),
		)))
		mgr := newManager(t, db, reg)
		p, err := mgr.start(ctx, "test.slow", testInput{Seed: "abc"})
		require.NoError(t, err)

		shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		status, err := p.Await(shortCtx)
		require.NoError(t, err)
		require.Equal(t, StatusRunning, status)

		longCtx, cancel2 := context.WithTimeout(ctx, 10*time.Second)
		defer cancel2()
		status, err = p.Await(longCtx)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, status)
	})

	t.Run("Await wakes on a Stuck plan", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.park"),
			Handle(NewTask[None]("test.park"), func(context.Context, *TaskRun[testInput]) (None, error) {
				return None{}, Permanent(errors.New("boom"))
			}),
		)))
		mgr := newManager(t, db, reg)
		p, err := mgr.start(ctx, "test.park", testInput{Seed: "abc"})
		require.NoError(t, err)

		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		status, err := p.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusStuck, status)
	})
}

func TestWorkflowDef(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	t.Run("Start runs a workflow through its definition", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.typedstart")
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(def,
			mapTask("test.typedonly", map[string]string{"v": "typed"}),
		)))
		mgr := newManager(t, db, reg)

		p, err := def.Start(ctx, mgr, testInput{Seed: "abc"})
		require.NoError(t, err)

		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		status, err := p.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, status)
	})

	t.Run("StartTx enqueues inside the caller transaction", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.typedstarttx")
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(def,
			mapTask("test.typedtxonly", map[string]string{"v": "typed"}),
		)))
		mgr := newManager(t, db, reg)

		var runID uuid.UUID
		require.NoError(t, pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			p, err := def.StartTx(ctx, mgr, tx, testInput{Seed: "abc"})
			if err != nil {
				return err
			}
			runID = p.ID()
			return nil
		}))

		got, err := db.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, StatusRunning, got.Status)
	})

	t.Run("Start errors when the workflow is not registered", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.unregistered")
		mgr := newManager(t, db, NewRegistry())

		_, err := def.Start(ctx, mgr, testInput{Seed: "abc"})
		require.Error(t, err)
	})

	t.Run("Input returns the input the first start stored", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.typedinput")
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(def,
			mapTask("test.typedinputonly", map[string]string{"v": "typed"}),
		)))
		mgr := newManager(t, db, reg, func(c *Config) { c.Cipher = base64Cipher{} })
		key := WithIdempotencyKey("input:" + uuid.NewString())

		var dup *WorkflowRun
		require.NoError(t, pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			_, err := def.StartTx(ctx, mgr, tx, testInput{Seed: "first"}, key)
			return err
		}))
		require.NoError(t, pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			var err error
			dup, err = def.StartTx(ctx, mgr, tx, testInput{Seed: "second"}, key)
			return err
		}))

		got, err := def.Input(ctx, dup)
		require.NoError(t, err)
		require.Equal(t, testInput{Seed: "first"}, got)
	})

	t.Run("Input rejects a run of another workflow", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.inputowner")
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(def,
			mapTask("test.inputowneronly", map[string]string{"v": "typed"}),
		)))
		mgr := newManager(t, db, reg)

		var run *WorkflowRun
		require.NoError(t, pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			var err error
			run, err = def.StartTx(ctx, mgr, tx, testInput{Seed: "abc"})
			return err
		}))

		_, err := NewWorkflow[testInput]("test.inputstranger").Input(ctx, run)
		require.Error(t, err)
	})

	t.Run("Input errors for a run that does not exist", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.inputmissing")
		mgr := newManager(t, db, NewRegistry())

		_, err := def.Input(ctx, &WorkflowRun{id: uuid.New(), mgr: mgr})
		require.Error(t, err)
	})

	t.Run("Input errors when the stored input does not decode", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.inputcorrupt")
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(def,
			mapTask("test.inputcorruptonly", map[string]string{"v": "typed"}),
		)))
		mgr := newManager(t, db, reg)

		var run *WorkflowRun
		require.NoError(t, pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
			var err error
			run, err = def.StartTx(ctx, mgr, tx, testInput{Seed: "abc"})
			return err
		}))
		_, err := db.DB.Query.Exec(ctx, db.DB.SQL.Update(store.TableRuns).
			Set("input", squirrel.Expr("'[1,2,3]'::jsonb")).
			Where(squirrel.Eq{"id": run.ID()}))
		require.NoError(t, err)

		_, err = def.Input(ctx, run)
		require.Error(t, err)
	})

	t.Run("Name returns the durable workflow name", func(t *testing.T) {
		require.Equal(t, "test.named", NewWorkflow[testInput]("test.named").Name())
	})
}

func TestWorkflowRunFailure(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	t.Run("reports the parked task and its error", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.failreason")
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(def,
			Handle(NewTask[None]("test.boom"), func(context.Context, *TaskRun[testInput]) (None, error) {
				return None{}, Permanent(errors.New("business rejected"))
			}),
		)))
		mgr := newManager(t, db, reg)

		p, err := def.Start(ctx, mgr, testInput{Seed: "abc"})
		require.NoError(t, err)
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		status, err := p.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusStuck, status)

		f, err := p.Failure(ctx)
		require.NoError(t, err)
		require.NotNil(t, f)
		require.Equal(t, "test.boom", f.Task)
		require.Equal(t, "business rejected", f.Error)
		require.Equal(t, 1, f.Attempts, "the handler ran once before the permanent error")
		require.Empty(t, f.Code, "the handler tagged no code")
	})

	t.Run("reports the code the handler tagged the failure with", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.failcode")
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(def,
			Handle(NewTask[None]("test.codedboom"), func(context.Context, *TaskRun[testInput]) (None, error) {
				return None{}, Permanent(WithCode("invalid_input", errors.New("business rejected")))
			}),
		)))
		mgr := newManager(t, db, reg)

		p, err := def.Start(ctx, mgr, testInput{Seed: "abc"})
		require.NoError(t, err)
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		status, err := p.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusStuck, status)

		f, err := p.Failure(ctx)
		require.NoError(t, err)
		require.NotNil(t, f)
		require.Equal(t, "invalid_input", f.Code)
		require.Equal(t, "business rejected", f.Error)
	})

	t.Run("leaves the code empty when the engine parks the run", func(t *testing.T) {
		// Same workflow name in both registries, so the claim filter admits the
		// run, but the seeded task has no handler in the second.
		seeded := NewRegistry()
		require.NoError(t, seeded.Register(Define(NewWorkflow[testInput]("test.skewcode"),
			Handle(NewTask[None]("test.skew.seeded"), func(context.Context, *TaskRun[testInput]) (None, error) {
				return None{}, nil
			}),
		)))
		skewed := NewRegistry()
		require.NoError(t, skewed.Register(Define(NewWorkflow[testInput]("test.skewcode"),
			Handle(NewTask[None]("test.skew.other"), func(context.Context, *TaskRun[testInput]) (None, error) {
				return None{}, nil
			}),
		)))

		plan := seedPlan(t, db, ctx, newManager(t, db, seeded), "test.skewcode", testInput{Seed: "abc"})
		mgr := newManager(t, db, skewed)
		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))

		f, err := mgr.failure(ctx, plan.ID)
		require.NoError(t, err)
		require.NotNil(t, f)
		require.Empty(t, f.Code)
		require.Contains(t, f.Error, "version skew")
	})

	t.Run("is nil for a run that succeeded", func(t *testing.T) {
		def := NewWorkflow[testInput]("test.failnone")
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(def,
			mapTask("test.failok", map[string]string{"v": "ok"}),
		)))
		mgr := newManager(t, db, reg)

		p, err := def.Start(ctx, mgr, testInput{Seed: "abc"})
		require.NoError(t, err)
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		status, err := p.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, status)

		f, err := p.Failure(ctx)
		require.NoError(t, err)
		require.Nil(t, f)
	})
}
