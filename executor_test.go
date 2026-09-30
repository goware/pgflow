package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/goware/workflow/store"
)

type firstOut struct {
	Token string `json:"token"`
}

type secondOut struct {
	Result string `json:"result"`
}

func makeDue(t *testing.T, db *store.Store, ctx context.Context, runID uuid.UUID) {
	t.Helper()
	q := db.DB.SQL.Update(store.TableRuns).
		Set("next_retry_at", squirrel.Expr("now() - interval '1 second'")).
		Where(squirrel.Eq{"id": runID})
	_, err := db.DB.Query.Exec(ctx, q)
	require.NoError(t, err)
}

func ageClaim(t *testing.T, db *store.Store, ctx context.Context, runID, claimID uuid.UUID, age time.Duration) {
	t.Helper()
	q := db.DB.SQL.Update(store.TableRuns).
		Set("claimed_at", squirrel.Expr("now() - make_interval(secs => ?)", age.Seconds())).
		Where(squirrel.Eq{"id": runID, "claim_id": claimID})
	res, err := db.DB.Query.Exec(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsAffected())
}

// backOffRun stands in for PersistTaskFailure's backoff so a release visibly
// preserves it.
func backOffRun(t *testing.T, db *store.Store, ctx context.Context, runID uuid.UUID) {
	t.Helper()
	q := db.DB.SQL.Update(store.TableRuns).
		Set("next_retry_at", squirrel.Expr("now() + interval '1 minute'")).
		Where(squirrel.Eq{"id": runID})
	res, err := db.DB.Query.Exec(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsAffected())
}

// failHeartbeats uses a trigger on UPDATE OF claimed_at that skips claims and
// releases, so only Heartbeat trips it and the release that follows still lands.
func failHeartbeats(t *testing.T, db *store.Store, ctx context.Context, runName string) func() {
	t.Helper()
	drop := func() {
		_, err := db.DB.Conn.Exec(ctx, `DROP TRIGGER IF EXISTS test_fail_heartbeat ON workflow_runs`)
		require.NoError(t, err)
		_, err = db.DB.Conn.Exec(ctx, `DROP FUNCTION IF EXISTS test_fail_heartbeat()`)
		require.NoError(t, err)
	}
	_, err := db.DB.Conn.Exec(ctx, `
		CREATE OR REPLACE FUNCTION test_fail_heartbeat() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.name = TG_ARGV[0] AND NEW.claimed_at IS NOT NULL
				AND OLD.claim_id IS NOT NULL AND NEW.claim_id = OLD.claim_id THEN
				RAISE EXCEPTION 'injected heartbeat failure';
			END IF;
			RETURN NEW;
		END;
		$$`)
	require.NoError(t, err)
	_, err = db.DB.Conn.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER test_fail_heartbeat
		BEFORE UPDATE OF claimed_at ON workflow_runs
		FOR EACH ROW EXECUTE FUNCTION test_fail_heartbeat('%s')`, runName))
	require.NoError(t, err)
	t.Cleanup(drop)
	return drop
}

// failTaskTransition uses a trigger so the pool stays healthy and later writes
// land. pending to running trips MarkTaskRunning; running to stuck trips a park.
func failTaskTransition(t *testing.T, db *store.Store, ctx context.Context, runName string, from, to store.TaskStatus) func() {
	t.Helper()
	drop := func() {
		_, err := db.DB.Conn.Exec(ctx, `DROP TRIGGER IF EXISTS test_fail_task_transition ON workflow_tasks`)
		require.NoError(t, err)
		_, err = db.DB.Conn.Exec(ctx, `DROP FUNCTION IF EXISTS test_fail_task_transition()`)
		require.NoError(t, err)
	}
	_, err := db.DB.Conn.Exec(ctx, `
		CREATE OR REPLACE FUNCTION test_fail_task_transition() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.status = TG_ARGV[1]::smallint AND NEW.status = TG_ARGV[2]::smallint
				AND (SELECT name FROM workflow_runs WHERE id = NEW.run_id) = TG_ARGV[0] THEN
				RAISE EXCEPTION 'injected task transition failure';
			END IF;
			RETURN NEW;
		END;
		$$`)
	require.NoError(t, err)
	_, err = db.DB.Conn.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER test_fail_task_transition
		BEFORE UPDATE OF status ON workflow_tasks
		FOR EACH ROW EXECUTE FUNCTION test_fail_task_transition('%s', '%d', '%d')`, runName, from, to))
	require.NoError(t, err)
	t.Cleanup(drop)
	return drop
}

// requireAttemptLedger asserts attempt_count equals the ledger length: the two
// must never drift.
func requireAttemptLedger(t *testing.T, task *store.TaskRun, want int) {
	t.Helper()
	var attempts []struct {
		N int `json:"n"`
	}
	require.NoError(t, json.Unmarshal(task.Attempts, &attempts))
	require.Equal(t, want, task.AttemptCount)
	require.Len(t, attempts, want, "attempt_count must equal jsonb_array_length(attempts)")
	for i, attempt := range attempts {
		require.Equal(t, i+1, attempt.N, "ledger entries must be numbered 1..attempt_count")
	}
}

// seedPlan backdates next_retry_at so an immediate claim in the same test sees it.
func seedPlan(t *testing.T, db *store.Store, ctx context.Context, mgr *Manager, workflowName string, input any) *store.WorkflowRun {
	t.Helper()
	plan, stages, err := mgr.buildRun(ctx, workflowName, input, "")
	require.NoError(t, err)
	require.NoError(t, db.InsertRun(ctx, plan, stages))
	makeDue(t, db, ctx, plan.ID)
	return plan
}

func claim(t *testing.T, db *store.Store, ctx context.Context, runID uuid.UUID) *store.ClaimedRun {
	t.Helper()
	claimed, ok, err := db.ClaimRunByID(ctx, runID)
	require.NoError(t, err)
	require.True(t, ok)
	return claimed
}

// recordingHandler tells failure branches apart when they leave identical rows.
type recordingHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

func (h *recordingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.msgs)
}

func TestNextBackoff(t *testing.T) {
	cfg := Config{BackoffBase: time.Second, BackoffMax: time.Minute}
	schedule := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}

	t.Run("an explicit schedule is indexed by the failed attempt", func(t *testing.T) {
		sc := taskConfig{backoffSchedule: schedule}

		require.Equal(t, time.Second, nextBackoff(sc, cfg, 1))
		require.Equal(t, 5*time.Second, nextBackoff(sc, cfg, 2))
		require.Equal(t, 30*time.Second, nextBackoff(sc, cfg, 3))
	})

	t.Run("attempts past the end clamp to the last entry", func(t *testing.T) {
		sc := taskConfig{backoffSchedule: schedule}

		require.Equal(t, 30*time.Second, nextBackoff(sc, cfg, 4))
		require.Equal(t, 30*time.Second, nextBackoff(sc, cfg, 99))
	})

	t.Run("an attempt below one reads the first entry", func(t *testing.T) {
		sc := taskConfig{backoffSchedule: schedule}

		require.Equal(t, time.Second, nextBackoff(sc, cfg, 0))
		require.Equal(t, time.Second, nextBackoff(sc, cfg, -1))
	})

	t.Run("no schedule falls through to exponential backoff", func(t *testing.T) {
		require.Equal(t, time.Second, nextBackoff(taskConfig{}, cfg, 1))
		require.Equal(t, 4*time.Second, nextBackoff(taskConfig{}, cfg, 3))
		require.Equal(t, time.Minute, nextBackoff(taskConfig{}, cfg, 20))
	})

	t.Run("a task's own exponential bounds win over the config's", func(t *testing.T) {
		sc := taskConfig{backoffBase: 10 * time.Millisecond, backoffMax: 20 * time.Millisecond}

		require.Equal(t, 10*time.Millisecond, nextBackoff(sc, cfg, 1))
		require.Equal(t, 20*time.Millisecond, nextBackoff(sc, cfg, 2))
	})
}

func TestExecutor(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	t.Run("runs a two-stage plan to succeeded and stage two reads stage one's output", func(t *testing.T) {
		reg := NewRegistry()
		firstTask := NewTask[firstOut]("test.first")
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.io"),
			Handle(firstTask, func(_ context.Context, run *TaskRun[testInput]) (firstOut, error) {
				return firstOut{Token: run.Input.Seed + "-1"}, nil
			}),
			Handle(NewTask[secondOut]("test.second"), func(_ context.Context, run *TaskRun[testInput]) (secondOut, error) {
				prev, err := Output(run, firstTask)
				if err != nil {
					return secondOut{}, err
				}
				return secondOut{Result: prev.Token + "-2"}, nil
			}),
		)))
		mgr := newManager(t, db, reg)

		p, err := mgr.start(ctx, "test.io", testInput{Seed: "abc"})
		require.NoError(t, err)

		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		status, err := p.Await(awaitCtx)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, status)

		stages, err := db.GetTasks(ctx, p.ID())
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusSucceeded, stages[0].Status)
		require.Equal(t, store.TaskStatusSucceeded, stages[1].Status)
		var got secondOut
		require.NoError(t, json.Unmarshal(stages[1].Output, &got))
		require.Equal(t, "abc-1-2", got.Result)
	})

	t.Run("crash-resume from every stage boundary completes exactly once", func(t *testing.T) {
		const stageCount = 3
		for b := range stageCount {
			t.Run(fmt.Sprintf("boundary %d", b), func(t *testing.T) {
				reg := NewRegistry()
				counters := make([]*atomic.Int64, stageCount)
				specs := make([]TaskSpec[testInput], stageCount)
				for i := range stageCount {
					counters[i] = new(atomic.Int64)
					ctr := counters[i]
					idx := i
					specs[i] = Handle(NewTask[map[string]int](fmt.Sprintf("test.s%d", i)),
						func(context.Context, *TaskRun[testInput]) (map[string]int, error) {
							ctr.Add(1)
							return map[string]int{"stage": idx}, nil
						})
				}
				require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.multi"), specs...)))
				mgr := newManager(t, db, reg)

				var p *WorkflowRun
				require.NoError(t, pgx.BeginFunc(ctx, db.DB.Conn, func(tx pgx.Tx) error {
					var e error
					p, e = mgr.startTx(ctx, tx, "test.multi", testInput{Seed: "x"})
					return e
				}))
				makeDue(t, db, ctx, p.ID())

				first := claim(t, db, ctx, p.ID())
				for s := range b {
					ok, err := db.MarkTaskRunning(ctx, p.ID(), int16(s), first.ClaimID)
					require.NoError(t, err)
					require.True(t, ok)
					ok, err = db.PersistTaskSuccess(ctx, p.ID(), int16(s), json.RawMessage(`{}`), first.ClaimID)
					require.NoError(t, err)
					require.True(t, ok)
				}

				ageClaim(t, db, ctx, p.ID(), first.ClaimID, time.Hour)
				_, err := db.SweepStuck(ctx, time.Minute)
				require.NoError(t, err)
				released, err := db.GetRun(ctx, p.ID())
				require.NoError(t, err)
				require.Nil(t, released.ClaimedAt)
				makeDue(t, db, ctx, p.ID())

				resume := claim(t, db, ctx, p.ID())
				require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, resume))

				got, err := db.GetRun(ctx, p.ID())
				require.NoError(t, err)
				require.Equal(t, StatusSucceeded, got.Status)
				for s := range b {
					require.EqualValues(t, 0, counters[s].Load(), "driven stage %d must not run on resume", s)
				}
				for s := b; s < stageCount; s++ {
					require.EqualValues(t, 1, counters[s].Load(), "resumed stage %d must run exactly once", s)
				}
			})
		}
	})

	t.Run("crash after final success before CompleteRun still completes", func(t *testing.T) {
		reg := NewRegistry()
		var ran atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.one"),
			Handle(NewTask[any]("test.only"), func(context.Context, *TaskRun[testInput]) (any, error) {
				ran.Add(1)
				return map[string]string{"v": "ok"}, nil
			}),
		)))
		mgr := newManager(t, db, reg)

		plan := seedPlan(t, db, ctx, mgr, "test.one", testInput{Seed: "x"})
		first := claim(t, db, ctx, plan.ID)
		ok, err := db.MarkTaskRunning(ctx, plan.ID, 0, first.ClaimID)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = db.PersistTaskSuccess(ctx, plan.ID, 0, json.RawMessage(`{"v":"ok"}`), first.ClaimID)
		require.NoError(t, err)
		require.True(t, ok)

		ageClaim(t, db, ctx, plan.ID, first.ClaimID, time.Hour)
		_, err = db.SweepStuck(ctx, time.Minute)
		require.NoError(t, err)
		makeDue(t, db, ctx, plan.ID)

		resume := claim(t, db, ctx, plan.ID)
		require.Equal(t, int16(1), resume.Run.CurrentTask)
		require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, resume))

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, got.Status)
		require.NotNil(t, got.CompletedAt)
		require.EqualValues(t, 0, ran.Load())
	})

	t.Run("sweep preserves a skipped current stage", func(t *testing.T) {
		reg := NewRegistry()
		var skippedRan, secondRan atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.skipfirst"),
			Handle(NewTask[any]("test.skipme"), func(context.Context, *TaskRun[testInput]) (any, error) {
				skippedRan.Add(1)
				return nil, nil
			}).SkipIf(func(in testInput) bool { return in.Seed == "skip" }),
			Handle(NewTask[any]("test.run"), func(context.Context, *TaskRun[testInput]) (any, error) {
				secondRan.Add(1)
				return map[string]string{"v": "ok"}, nil
			}),
		)))
		mgr := newManager(t, db, reg)

		plan := seedPlan(t, db, ctx, mgr, "test.skipfirst", testInput{Seed: "skip"})
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusSkipped, stages[0].Status)

		first := claim(t, db, ctx, plan.ID)
		ageClaim(t, db, ctx, plan.ID, first.ClaimID, time.Hour)
		_, err = db.SweepStuck(ctx, time.Minute)
		require.NoError(t, err)

		swept, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusSkipped, swept[0].Status)
		require.Equal(t, 0, swept[0].AttemptCount)
		makeDue(t, db, ctx, plan.ID)

		resume := claim(t, db, ctx, plan.ID)
		require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, resume))

		final, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusSkipped, final[0].Status)
		require.Equal(t, store.TaskStatusSucceeded, final[1].Status)
		require.EqualValues(t, 0, skippedRan.Load())
		require.EqualValues(t, 1, secondRan.Load())
	})

	t.Run("sweep preserves a pending current stage", func(t *testing.T) {
		reg := NewRegistry()
		var firstRan, secondRan atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.twopending"),
			Handle(NewTask[any]("test.p0"), func(context.Context, *TaskRun[testInput]) (any, error) {
				firstRan.Add(1)
				return map[string]string{"v": "0"}, nil
			}),
			Handle(NewTask[any]("test.p1"), func(context.Context, *TaskRun[testInput]) (any, error) {
				secondRan.Add(1)
				return map[string]string{"v": "1"}, nil
			}),
		)))
		mgr := newManager(t, db, reg)

		plan := seedPlan(t, db, ctx, mgr, "test.twopending", testInput{Seed: "x"})
		// Crash at the boundary: claimed but never MarkTaskRunning'd.
		first := claim(t, db, ctx, plan.ID)
		ageClaim(t, db, ctx, plan.ID, first.ClaimID, time.Hour)
		_, err := db.SweepStuck(ctx, time.Minute)
		require.NoError(t, err)

		swept, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusPending, swept[0].Status)
		require.Equal(t, 0, swept[0].AttemptCount)
		makeDue(t, db, ctx, plan.ID)

		resume := claim(t, db, ctx, plan.ID)
		require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, resume))
		require.EqualValues(t, 1, firstRan.Load())
		require.EqualValues(t, 1, secondRan.Load())
	})

	t.Run("shutdown mid-stage does not burn an attempt and drains fast", func(t *testing.T) {
		newBlockingMgr := func(ret func() (any, error), started, proceed chan struct{}, counter *atomic.Int64) (*Manager, *store.WorkflowRun) {
			reg := NewRegistry()
			require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.block"),
				Handle(NewTask[any]("test.block"), func(context.Context, *TaskRun[testInput]) (any, error) {
					if counter != nil {
						counter.Add(1)
					}
					close(started)
					<-proceed
					return ret()
				}),
				Handle(NewTask[any]("test.after"), func(context.Context, *TaskRun[testInput]) (any, error) {
					return map[string]string{"v": "after"}, nil
				}),
			)))
			mgr := newManager(t, db, reg)
			plan := seedPlan(t, db, ctx, mgr, "test.block", testInput{Seed: "x"})
			return mgr, plan
		}

		t.Run("interrupted handler is not attempted", func(t *testing.T) {
			started, proceed := make(chan struct{}), make(chan struct{})
			mgr, plan := newBlockingMgr(func() (any, error) { return nil, errors.New("interrupted") }, started, proceed, nil)
			claimed := claim(t, db, ctx, plan.ID)

			done := make(chan Status, 1)
			go func() { done <- mgr.executePlan(ctx, claimed) }()
			<-started
			mgr.draining.Store(true)
			close(proceed)
			require.Equal(t, StatusRunning, <-done)

			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			require.Equal(t, int16(0), got.CurrentTask)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusPending, stages[0].Status)
			require.Equal(t, 0, stages[0].AttemptCount)

			makeDue(t, db, ctx, plan.ID)
			_, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
		})

		t.Run("a just-succeeded handler persists then releases for immediate re-claim", func(t *testing.T) {
			started, proceed := make(chan struct{}), make(chan struct{})
			mgr, plan := newBlockingMgr(func() (any, error) { return map[string]string{"v": "done"}, nil }, started, proceed, nil)
			claimed := claim(t, db, ctx, plan.ID)

			done := make(chan Status, 1)
			go func() { done <- mgr.executePlan(ctx, claimed) }()
			<-started
			mgr.draining.Store(true)
			close(proceed)
			require.Equal(t, StatusRunning, <-done)

			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, int16(1), got.CurrentTask)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			require.WithinDuration(t, time.Now(), got.NextRetryAt, 10*time.Second)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusSucceeded, stages[0].Status)
			require.NotNil(t, stages[0].Output)
		})

		t.Run("drain observed at a boundary starts no further unit", func(t *testing.T) {
			reg := NewRegistry()
			var ran atomic.Int64
			require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.boundary"),
				Handle(NewTask[any]("test.boundary"), func(context.Context, *TaskRun[testInput]) (any, error) {
					ran.Add(1)
					return nil, nil
				}),
			)))
			mgr := newManager(t, db, reg)
			plan := seedPlan(t, db, ctx, mgr, "test.boundary", testInput{Seed: "x"})
			claimed := claim(t, db, ctx, plan.ID)

			mgr.draining.Store(true)
			require.Equal(t, StatusRunning, mgr.executePlan(ctx, claimed))
			require.EqualValues(t, 0, ran.Load())

			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusPending, stages[0].Status)
		})
	})

	t.Run("a cancelled poller context still lands the drain-path writes", func(t *testing.T) {
		t.Run("a handler that finished as the poller stopped is persisted and released", func(t *testing.T) {
			reg := NewRegistry()
			started := make(chan struct{})
			require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.polldrain"),
				Handle(NewTask[any]("test.polldrain.first"), func(taskCtx context.Context, _ *TaskRun[testInput]) (any, error) {
					close(started)
					// Respects cancellation and returns work it already finished.
					<-taskCtx.Done()
					return map[string]string{"v": "done"}, nil
				}),
				Handle(NewTask[any]("test.polldrain.second"), func(context.Context, *TaskRun[testInput]) (any, error) {
					return map[string]string{"v": "after"}, nil
				}),
			)))
			mgr := newManager(t, db, reg)

			plan := seedPlan(t, db, ctx, mgr, "test.polldrain", testInput{Seed: "x"})

			runnerCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			stopped := make(chan error, 1)
			go func() {
				stopped <- mgr.NewRunnable(RunnerConfig{PollInterval: 10 * time.Millisecond}).Run(runnerCtx)
			}()
			<-started
			cancel()
			require.NoError(t, <-stopped)

			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusSucceeded, stages[0].Status)
			require.NotNil(t, stages[0].Output)
			require.Equal(t, 0, stages[0].AttemptCount)
			require.Equal(t, store.TaskStatusPending, stages[1].Status)
			require.Nil(t, stages[1].Output)

			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			require.Equal(t, int16(1), got.CurrentTask)
		})

		t.Run("a drain observed at a task boundary releases the claim", func(t *testing.T) {
			reg := NewRegistry()
			var ran atomic.Int64
			require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.cancelboundary"),
				Handle(NewTask[any]("test.cancelboundary"), func(context.Context, *TaskRun[testInput]) (any, error) {
					ran.Add(1)
					return nil, nil
				}),
			)))
			mgr := newManager(t, db, reg)
			plan := seedPlan(t, db, ctx, mgr, "test.cancelboundary", testInput{Seed: "x"})
			claimed := claim(t, db, ctx, plan.ID)

			runCtx, cancel := context.WithCancel(ctx)
			cancel()
			require.Equal(t, StatusRunning, mgr.executePlan(runCtx, claimed))
			require.EqualValues(t, 0, ran.Load())

			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusPending, stages[0].Status)
		})

		t.Run("a heartbeat that cannot land releases the claim", func(t *testing.T) {
			mgr := newManager(t, db, twoStageRegistry(t))
			plan := seedPlan(t, db, ctx, mgr, "test.twostage", testInput{Seed: "x"})
			claimed := claim(t, db, ctx, plan.ID)
			running, err := db.MarkTaskRunning(ctx, plan.ID, 0, claimed.ClaimID)
			require.NoError(t, err)
			require.True(t, running)

			runCtx, cancel := context.WithCancel(ctx)
			cancel()
			require.False(t, mgr.heartbeat(runCtx, plan.ID, 0, claimed.ClaimID))

			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusPending, stages[0].Status)
		})

		t.Run("a cancellation after the last task still completes the run", func(t *testing.T) {
			reg := NewRegistry()
			var ran atomic.Int64
			require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.cancelcomplete"),
				Handle(NewTask[any]("test.cancelcomplete"), func(context.Context, *TaskRun[testInput]) (any, error) {
					ran.Add(1)
					return nil, nil
				}),
			)))
			mgr := newManager(t, db, reg)
			plan := seedPlan(t, db, ctx, mgr, "test.cancelcomplete", testInput{Seed: "x"})

			// Stands in for an executor that persisted the last task and advanced
			// past it, so only the completion write is owed.
			claimed := claim(t, db, ctx, plan.ID)
			advanced, err := db.PersistTaskSuccess(ctx, plan.ID, 0, json.RawMessage(`{"v":"done"}`), claimed.ClaimID)
			require.NoError(t, err)
			require.True(t, advanced)
			require.NoError(t, db.ReleaseClaim(ctx, plan.ID, 0, claimed.ClaimID))
			makeDue(t, db, ctx, plan.ID)

			resumed := claim(t, db, ctx, plan.ID)
			require.Equal(t, int16(1), resumed.Run.CurrentTask)

			runCtx, cancel := context.WithCancel(ctx)
			cancel()
			require.Equal(t, StatusSucceeded, mgr.executePlan(runCtx, resumed))
			require.EqualValues(t, 0, ran.Load())

			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, StatusSucceeded, got.Status)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusSucceeded, stages[0].Status)
		})
	})

	t.Run("retries with backoff then succeeds", func(t *testing.T) {
		reg := NewRegistry()
		var calls atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.retry"),
			Handle(NewTask[any]("test.retry"), func(context.Context, *TaskRun[testInput]) (any, error) {
				if calls.Add(1) <= 2 {
					return nil, errors.New("transient")
				}
				return map[string]string{"v": "ok"}, nil
			}, Backoff(10*time.Millisecond, 50*time.Millisecond), MaxAttempts(5)),
		)))
		mgr := newManager(t, db, reg, func(c *Config) { c.MaxInProcessBackoff = time.Second })

		plan := seedPlan(t, db, ctx, mgr, "test.retry", testInput{Seed: "x"})
		require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 3, calls.Load())

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, got.Status)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusSucceeded, stages[0].Status)
		require.Equal(t, 2, stages[0].AttemptCount)
	})

	t.Run("a long backoff releases the claim with a DB-computed next_retry_at", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.longbackoff"),
			Handle(NewTask[any]("test.longbackoff"), func(context.Context, *TaskRun[testInput]) (any, error) {
				return nil, errors.New("transient")
			}, Backoff(20*time.Second, time.Minute), MaxAttempts(5)),
		)))
		mgr := newManager(t, db, reg)

		plan := seedPlan(t, db, ctx, mgr, "test.longbackoff", testInput{Seed: "x"})
		before := time.Now()
		require.Equal(t, StatusRunning, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusRunning, got.Status)
		require.Nil(t, got.ClaimedAt)
		require.Nil(t, got.ClaimID)
		require.WithinDuration(t, before.Add(20*time.Second), got.NextRetryAt, 5*time.Second)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusPending, stages[0].Status)
		require.Equal(t, 1, stages[0].AttemptCount)
	})

	t.Run("a shutdown during an in-process backoff keeps the recorded delay", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.backoffdrain"),
			Handle(NewTask[any]("test.backoffdrain"), func(context.Context, *TaskRun[testInput]) (any, error) {
				return nil, errors.New("transient")
			}, Backoff(30*time.Second, time.Minute), MaxAttempts(3)),
		)))
		mgr := newManager(t, db, reg, func(c *Config) { c.MaxInProcessBackoff = time.Minute })
		plan := seedPlan(t, db, ctx, mgr, "test.backoffdrain", testInput{Seed: "x"})
		claimed := claim(t, db, ctx, plan.ID)

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan Status, 1)
		go func() { done <- mgr.executePlan(runCtx, claimed) }()
		// Cancel once the failure is recorded so shutdown lands inside the sleep.
		require.Eventually(t, func() bool {
			stages, err := db.GetTasks(ctx, plan.ID)
			return err == nil && stages[0].AttemptCount == 1
		}, 5*time.Second, 10*time.Millisecond)
		cancel()
		require.Equal(t, StatusRunning, <-done)

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Nil(t, got.ClaimedAt)
		require.Nil(t, got.ClaimID)
		require.WithinDuration(t, time.Now().Add(30*time.Second), got.NextRetryAt, 5*time.Second)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusPending, stages[0].Status)
		require.Equal(t, 1, stages[0].AttemptCount)
	})

	t.Run("a BackoffSchedule delay above MaxInProcessBackoff releases the claim", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.schedrelease"),
			Handle(NewTask[any]("test.schedrelease"), func(context.Context, *TaskRun[testInput]) (any, error) {
				return nil, errors.New("transient")
			}, BackoffSchedule([]time.Duration{30 * time.Second, time.Minute}), MaxAttempts(5)),
		)))
		mgr := newManager(t, db, reg, func(c *Config) { c.MaxInProcessBackoff = time.Second })

		plan := seedPlan(t, db, ctx, mgr, "test.schedrelease", testInput{Seed: "x"})
		before := time.Now()
		require.Equal(t, StatusRunning, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Nil(t, got.ClaimedAt)
		require.Nil(t, got.ClaimID)
		require.WithinDuration(t, before.Add(30*time.Second), got.NextRetryAt, 5*time.Second)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusPending, stages[0].Status)
		require.Equal(t, 1, stages[0].AttemptCount)
	})

	t.Run("MaxAttempts caps a task whose BackoffSchedule is shorter than its budget", func(t *testing.T) {
		reg := NewRegistry()
		var calls atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.schedclamp"),
			Handle(NewTask[any]("test.schedclamp"), func(context.Context, *TaskRun[testInput]) (any, error) {
				calls.Add(1)
				return nil, errors.New("always fails")
			}, BackoffSchedule([]time.Duration{5 * time.Millisecond}), MaxAttempts(4)),
		)))
		mgr := newManager(t, db, reg, func(c *Config) { c.MaxInProcessBackoff = time.Second })

		plan := seedPlan(t, db, ctx, mgr, "test.schedclamp", testInput{Seed: "x"})
		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 4, calls.Load(), "the schedule's length must not cap attempts")

		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusStuck, stages[0].Status)
		requireAttemptLedger(t, stages[0], 4)
	})

	t.Run("heartbeat", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.heartbeat"),
			Handle(NewTask[any]("test.heartbeat"), func(context.Context, *TaskRun[testInput]) (any, error) {
				return nil, nil
			}),
		)))

		t.Run("a lost claim takes the release path, not the error path", func(t *testing.T) {
			rec := new(recordingHandler)
			mgr, err := NewManager(t.Context(), db.DB, reg, Config{}, slog.New(rec))
			require.NoError(t, err)
			plan := seedPlan(t, db, ctx, mgr, "test.heartbeat", testInput{Seed: "x"})
			claim(t, db, ctx, plan.ID)

			require.False(t, mgr.heartbeat(ctx, plan.ID, 0, uuid.New()))
			require.Empty(t, rec.messages(), "a stolen claim is not a DB failure")
		})

		t.Run("a DB error releases the claim and keeps the persisted retry time", func(t *testing.T) {
			rec := new(recordingHandler)
			mgr, err := NewManager(t.Context(), db.DB, reg, Config{}, slog.New(rec))
			require.NoError(t, err)
			plan := seedPlan(t, db, ctx, mgr, "test.heartbeat", testInput{Seed: "x"})
			claimed := claim(t, db, ctx, plan.ID)
			ok, err := db.MarkTaskRunning(ctx, plan.ID, 0, claimed.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			backOffRun(t, db, ctx, plan.ID)
			drop := failHeartbeats(t, db, ctx, "test.heartbeat")

			require.False(t, mgr.heartbeat(ctx, plan.ID, 0, claimed.ClaimID))
			drop()

			require.Contains(t, rec.messages(), "workflow: heartbeat failed")
			require.NotContains(t, rec.messages(), "workflow: release claim failed")
			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			require.WithinDuration(t, time.Now().Add(time.Minute), got.NextRetryAt, 5*time.Second)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusPending, stages[0].Status)
		})
	})

	t.Run("a MarkTaskRunning DB error releases the claim", func(t *testing.T) {
		reg := NewRegistry()
		var ran atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.markfail"),
			Handle(NewTask[any]("test.markfail"), func(context.Context, *TaskRun[testInput]) (any, error) {
				ran.Add(1)
				return nil, nil
			}),
		)))
		rec := new(recordingHandler)
		mgr, err := NewManager(t.Context(), db.DB, reg, Config{}, slog.New(rec))
		require.NoError(t, err)
		plan := seedPlan(t, db, ctx, mgr, "test.markfail", testInput{Seed: "x"})
		claimed := claim(t, db, ctx, plan.ID)
		drop := failTaskTransition(t, db, ctx, "test.markfail", store.TaskStatusPending, store.TaskStatusRunning)

		require.Equal(t, StatusRunning, mgr.executePlan(ctx, claimed))
		drop()

		require.EqualValues(t, 0, ran.Load())
		require.Contains(t, rec.messages(), "workflow: mark task running failed")
		require.NotContains(t, rec.messages(), "workflow: release claim failed")
		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Nil(t, got.ClaimedAt)
		require.Nil(t, got.ClaimID)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusPending, stages[0].Status)
		require.Equal(t, 0, stages[0].AttemptCount)

		// Claimable now, not after SweepMaxAge.
		_, ok, err := db.ClaimRunByID(ctx, plan.ID)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("a PersistTaskSuccess DB error keeps the claim so the sweep charges the attempt", func(t *testing.T) {
		reg := NewRegistry()
		var ran atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.persistfail"),
			Handle(NewTask[any]("test.persistfail"), func(context.Context, *TaskRun[testInput]) (any, error) {
				ran.Add(1)
				return map[string]string{"v": "done"}, nil
			}, MaxAttempts(1)),
		)))
		rec := new(recordingHandler)
		mgr, err := NewManager(t.Context(), db.DB, reg, Config{}, slog.New(rec))
		require.NoError(t, err)
		plan := seedPlan(t, db, ctx, mgr, "test.persistfail", testInput{Seed: "x"})
		claimed := claim(t, db, ctx, plan.ID)
		drop := failTaskTransition(t, db, ctx, "test.persistfail", store.TaskStatusRunning, store.TaskStatusSucceeded)

		require.Equal(t, StatusRunning, mgr.executePlan(ctx, claimed))
		drop()

		require.EqualValues(t, 1, ran.Load())
		require.Contains(t, rec.messages(), "workflow: persist task success failed")
		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.NotNil(t, got.ClaimID)
		require.Equal(t, claimed.ClaimID, *got.ClaimID)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusRunning, stages[0].Status)
		require.Equal(t, 0, stages[0].AttemptCount)

		// Not claimable until the sweeper has counted the attempt that ran.
		_, ok, err := db.ClaimRunByID(ctx, plan.ID)
		require.NoError(t, err)
		require.False(t, ok)
		ageClaim(t, db, ctx, plan.ID, claimed.ClaimID, time.Hour)
		_, err = db.SweepStuck(ctx, time.Minute)
		require.NoError(t, err)
		stages, err = db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, 1, stages[0].AttemptCount)

		// The budget, not the failing write, decides whether the handler runs again.
		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 1, ran.Load())
	})

	t.Run("a ParkRunAfterAttempt DB error keeps the claim so the sweep charges the attempt", func(t *testing.T) {
		reg := NewRegistry()
		var ran atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.parkfail"),
			Handle(NewTask[any]("test.parkfail"), func(context.Context, *TaskRun[testInput]) (any, error) {
				ran.Add(1)
				return nil, Permanent(errors.New("rejected"))
			}, MaxAttempts(1)),
		)))
		rec := new(recordingHandler)
		mgr, err := NewManager(t.Context(), db.DB, reg, Config{}, slog.New(rec))
		require.NoError(t, err)
		plan := seedPlan(t, db, ctx, mgr, "test.parkfail", testInput{Seed: "x"})
		claimed := claim(t, db, ctx, plan.ID)
		drop := failTaskTransition(t, db, ctx, "test.parkfail", store.TaskStatusRunning, store.TaskStatusStuck)

		require.Equal(t, StatusRunning, mgr.executePlan(ctx, claimed))
		drop()

		require.EqualValues(t, 1, ran.Load())
		require.Contains(t, rec.messages(), "workflow: park run failed")
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusRunning, stages[0].Status)
		require.Equal(t, 0, stages[0].AttemptCount)
		_, ok, err := db.ClaimRunByID(ctx, plan.ID)
		require.NoError(t, err)
		require.False(t, ok)

		ageClaim(t, db, ctx, plan.ID, claimed.ClaimID, time.Hour)
		_, err = db.SweepStuck(ctx, time.Minute)
		require.NoError(t, err)
		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 1, ran.Load())
		stages, err = db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusStuck, stages[0].Status)
		require.Equal(t, 1, stages[0].AttemptCount)
	})

	t.Run("a heartbeat DB error at the failure boundary", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.hbfailure"),
			Handle(NewTask[any]("test.hbfailure"), func(context.Context, *TaskRun[testInput]) (any, error) {
				return nil, errors.New("transient")
			}, Backoff(10*time.Millisecond, 50*time.Millisecond), MaxAttempts(5)),
		)))

		failOnce := func(t *testing.T) (*store.WorkflowRun, uuid.UUID) {
			t.Helper()
			mgr := newManager(t, db, reg, func(c *Config) { c.MaxInProcessBackoff = time.Second })
			plan := seedPlan(t, db, ctx, mgr, "test.hbfailure", testInput{Seed: "x"})
			claimed := claim(t, db, ctx, plan.ID)
			drop := failHeartbeats(t, db, ctx, "test.hbfailure")
			require.Equal(t, StatusRunning, mgr.executePlan(ctx, claimed))
			drop()
			return plan, claimed.ClaimID
		}

		t.Run("releases the claim instead of stalling until the sweep", func(t *testing.T) {
			plan, _ := failOnce(t)

			got, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, got.ClaimedAt)
			require.Nil(t, got.ClaimID)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusPending, stages[0].Status)
		})

		t.Run("leaves SweepStuck nothing to double-bump attempt_count on", func(t *testing.T) {
			plan, claimID := failOnce(t)

			// Age any surviving claim so the sweep is decisive.
			_, err := db.DB.Query.Exec(ctx, db.DB.SQL.Update(store.TableRuns).
				Set("claimed_at", squirrel.Expr("now() - interval '1 hour'")).
				Where(squirrel.Eq{"id": plan.ID, "claim_id": claimID}))
			require.NoError(t, err)
			_, err = db.SweepStuck(ctx, time.Minute)
			require.NoError(t, err)

			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, 1, stages[0].AttemptCount,
				"the handler took one attempt; a reclaim must not spend a second")
		})
	})

	t.Run("parks at MaxAttempts", func(t *testing.T) {
		reg := NewRegistry()
		var calls atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.park"),
			Handle(NewTask[any]("test.park"), func(context.Context, *TaskRun[testInput]) (any, error) {
				calls.Add(1)
				return nil, errors.New("always fails")
			}, Backoff(5*time.Millisecond, 20*time.Millisecond), MaxAttempts(3)),
		)))
		mgr := newManager(t, db, reg, func(c *Config) { c.MaxInProcessBackoff = time.Second })

		plan := seedPlan(t, db, ctx, mgr, "test.park", testInput{Seed: "x"})
		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 3, calls.Load())

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusStuck, got.Status)
		require.Nil(t, got.ClaimedAt)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusStuck, stages[0].Status)
		requireAttemptLedger(t, stages[0], 3)
		require.NotNil(t, stages[0].LastError)
		require.Equal(t, "always fails", *stages[0].LastError)
	})

	t.Run("LastAttempt is true only on the attempt the budget ends at", func(t *testing.T) {
		reg := NewRegistry()
		var mu sync.Mutex
		var seen []bool
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.lastattempt"),
			Handle(NewTask[any]("test.lastattempt"), func(_ context.Context, run *TaskRun[testInput]) (any, error) {
				mu.Lock()
				seen = append(seen, run.LastAttempt())
				mu.Unlock()
				return nil, errors.New("always fails")
			}, Backoff(5*time.Millisecond, 20*time.Millisecond), MaxAttempts(3)),
		)))
		mgr := newManager(t, db, reg, func(c *Config) { c.MaxInProcessBackoff = time.Second })

		plan := seedPlan(t, db, ctx, mgr, "test.lastattempt", testInput{Seed: "x"})
		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.Equal(t, []bool{false, false, true}, seen)
	})

	t.Run("SkipIf snapshots skipped, the handler never runs, and the loop does not spin", func(t *testing.T) {
		reg := NewRegistry()
		var skippedRan, ran atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.snapshot"),
			Handle(NewTask[any]("test.maybe"), func(context.Context, *TaskRun[testInput]) (any, error) {
				skippedRan.Add(1)
				return nil, nil
			}).SkipIf(func(in testInput) bool { return in.Seed == "skip" }),
			Handle(NewTask[any]("test.always"), func(context.Context, *TaskRun[testInput]) (any, error) {
				ran.Add(1)
				return map[string]string{"v": "ok"}, nil
			}),
		)))
		mgr := newManager(t, db, reg)

		plan := seedPlan(t, db, ctx, mgr, "test.snapshot", testInput{Seed: "skip"})
		require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 0, skippedRan.Load())
		require.EqualValues(t, 1, ran.Load())

		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusSkipped, stages[0].Status)
		require.Equal(t, store.TaskStatusSucceeded, stages[1].Status)
	})

	t.Run("an interrupted stage re-runs idempotently with no duplicate side effect", func(t *testing.T) {
		reg := NewRegistry()
		var mu sync.Mutex
		var calls atomic.Int64
		sideEffects := 0
		applied := map[string]bool{}
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.idem"),
			Handle(NewTask[any]("test.idem"), func(_ context.Context, run *TaskRun[testInput]) (any, error) {
				calls.Add(1)
				mu.Lock()
				if key := run.IdempotencyKey(); !applied[key] {
					applied[key] = true
					sideEffects++
				}
				mu.Unlock()
				return map[string]string{"v": "ok"}, nil
			}),
		)))
		mgr := newManager(t, db, reg)

		plan := seedPlan(t, db, ctx, mgr, "test.idem", testInput{Seed: "x"})
		first := claim(t, db, ctx, plan.ID)
		ok, err := db.MarkTaskRunning(ctx, plan.ID, 0, first.ClaimID)
		require.NoError(t, err)
		require.True(t, ok)

		// Crash after the side effect but before PersistTaskSuccess: invoke the
		// handler directly and leave the task running.
		_, err = reg.workflows["test.idem"].tasks[0].handler(ctx, taskRunCore{
			log: testLogger(), runID: plan.ID, seq: 0, input: plan.Input,
		})
		require.NoError(t, err)

		ageClaim(t, db, ctx, plan.ID, first.ClaimID, time.Hour)
		_, err = db.SweepStuck(ctx, time.Minute)
		require.NoError(t, err)
		makeDue(t, db, ctx, plan.ID)

		require.Equal(t, StatusSucceeded, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 2, calls.Load())
		mu.Lock()
		require.Equal(t, 1, sideEffects)
		require.True(t, applied[fmt.Sprintf("%s:0", plan.ID)])
		mu.Unlock()
	})

	t.Run("a resumed task whose budget is already spent parks without invoking", func(t *testing.T) {
		reg := NewRegistry()
		var ran atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.exhausted"),
			Handle(NewTask[any]("test.exhausted"), func(context.Context, *TaskRun[testInput]) (any, error) {
				ran.Add(1)
				return nil, nil
			}, MaxAttempts(3)),
		)))
		mgr := newManager(t, db, reg)
		plan := seedPlan(t, db, ctx, mgr, "test.exhausted", testInput{Seed: "x"})

		_, err := db.DB.Query.Exec(ctx, db.DB.SQL.Update(store.TableTasks).
			Set("attempt_count", 3).
			Set("attempts", squirrel.Expr(`'[{"n":1},{"n":2},{"n":3}]'::jsonb`)).
			Where(squirrel.Eq{"run_id": plan.ID, "seq": 0}))
		require.NoError(t, err)

		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 0, ran.Load(), "the handler must not run with its budget spent")

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusStuck, got.Status)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusStuck, stages[0].Status)
		requireAttemptLedger(t, stages[0], 3)
		require.NotNil(t, stages[0].LastError)
		require.Contains(t, *stages[0].LastError, "attempt budget exhausted")
	})

	t.Run("a real sweep-then-park sequence agrees with the ledger invariant", func(t *testing.T) {
		reg := NewRegistry()
		var ran atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.sweepthenpark"),
			Handle(NewTask[any]("test.sweepthenpark"), func(context.Context, *TaskRun[testInput]) (any, error) {
				ran.Add(1)
				return nil, nil
			}, MaxAttempts(2)),
		)))
		mgr := newManager(t, db, reg)
		plan := seedPlan(t, db, ctx, mgr, "test.sweepthenpark", testInput{Seed: "x"})

		claimed := claim(t, db, ctx, plan.ID)
		ok, err := db.MarkTaskRunning(ctx, plan.ID, 0, claimed.ClaimID)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = db.PersistTaskFailure(ctx, plan.ID, 0, store.TaskFailure{Message: "boom"}, time.Second, false, claimed.ClaimID)
		require.NoError(t, err)
		require.True(t, ok)

		ageClaim(t, db, ctx, plan.ID, claimed.ClaimID, time.Hour)
		released, err := db.SweepStuck(ctx, time.Minute)
		require.NoError(t, err)
		require.EqualValues(t, 1, released)
		makeDue(t, db, ctx, plan.ID)

		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 0, ran.Load(), "the sweep already spent the last attempt; the handler must not run")

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusStuck, got.Status)
		require.Nil(t, got.ClaimedAt)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusStuck, stages[0].Status)
		requireAttemptLedger(t, stages[0], 2)
		require.NotNil(t, stages[0].LastError)
		require.Contains(t, *stages[0].LastError, "attempt budget exhausted")
	})

	t.Run("Permanent short-circuits to park on the first failure", func(t *testing.T) {
		reg := NewRegistry()
		var calls atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.permanent"),
			Handle(NewTask[any]("test.permanent"), func(context.Context, *TaskRun[testInput]) (any, error) {
				calls.Add(1)
				return nil, Permanent(errors.New("fatal"))
			}, MaxAttempts(8)),
		)))
		mgr := newManager(t, db, reg)

		plan := seedPlan(t, db, ctx, mgr, "test.permanent", testInput{Seed: "x"})
		require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, plan.ID)))
		require.EqualValues(t, 1, calls.Load())

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusStuck, got.Status)
		stages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, store.TaskStatusStuck, stages[0].Status)
		requireAttemptLedger(t, stages[0], 1)
		require.NotNil(t, stages[0].LastError)
		require.Equal(t, "fatal", *stages[0].LastError)
	})

	t.Run("version skew parks with a clear error", func(t *testing.T) {
		reg := NewRegistry()
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.skew"),
			Handle(NewTask[any]("test.known"), func(context.Context, *TaskRun[testInput]) (any, error) { return nil, nil }),
		)))
		mgr := newManager(t, db, reg)

		insertRaw := func(name, handler string) *store.WorkflowRun {
			t.Helper()
			plan := &store.WorkflowRun{ID: uuid.New(), Name: name, Input: json.RawMessage(`{}`), Status: store.WorkflowStatusRunning}
			stages := []*store.TaskRun{{RunID: plan.ID, Seq: 0, Handler: handler, Status: store.TaskStatusPending}}
			require.NoError(t, db.InsertRun(ctx, plan, stages))
			makeDue(t, db, ctx, plan.ID)
			return plan
		}

		assertParked := func(runID uuid.UUID) {
			t.Helper()
			require.Equal(t, StatusStuck, mgr.executePlan(ctx, claim(t, db, ctx, runID)))
			got, err := db.GetRun(ctx, runID)
			require.NoError(t, err)
			require.Equal(t, StatusStuck, got.Status)
			require.Nil(t, got.ClaimedAt)
			stages, err := db.GetTasks(ctx, runID)
			require.NoError(t, err)
			require.Equal(t, store.TaskStatusStuck, stages[0].Status)
			requireAttemptLedger(t, stages[0], 0)
			require.NotNil(t, stages[0].LastError)
			require.Contains(t, *stages[0].LastError, "version skew")
		}

		t.Run("removed handler name", func(t *testing.T) {
			assertParked(insertRaw("test.skew", "test.gone").ID)
		})

		t.Run("removed plan name", func(t *testing.T) {
			assertParked(insertRaw("test.removed", "test.any").ID)
		})
	})

	t.Run("concurrent claim contention runs a plan exactly once", func(t *testing.T) {
		reg := NewRegistry()
		var calls atomic.Int64
		require.NoError(t, reg.Register(Define(NewWorkflow[testInput]("test.contend"),
			Handle(NewTask[any]("test.contend"), func(context.Context, *TaskRun[testInput]) (any, error) {
				calls.Add(1)
				return map[string]string{"v": "ok"}, nil
			}),
		)))
		mgr := newManager(t, db, reg)
		plan := seedPlan(t, db, ctx, mgr, "test.contend", testInput{Seed: "x"})

		// FailNow must run on the test's own goroutine, so claim errors come back
		// through a channel and are asserted after the wait.
		var wg sync.WaitGroup
		claimErrs := make(chan error, 2)
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				claimed, ok, err := db.ClaimRunByID(ctx, plan.ID)
				if err != nil {
					claimErrs <- err
					return
				}
				if ok {
					mgr.executePlan(ctx, claimed)
				}
			}()
		}
		wg.Wait()
		close(claimErrs)
		for err := range claimErrs {
			require.NoError(t, err)
		}

		require.EqualValues(t, 1, calls.Load())
		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, StatusSucceeded, got.Status)
	})
}
