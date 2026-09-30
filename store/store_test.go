package store

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const testWorkflowName = "test.workflows"

func newTestPlan(stageCount int) (*WorkflowRun, []*TaskRun) {
	plan := &WorkflowRun{
		ID:     uuid.New(),
		Name:   testWorkflowName,
		Input:  json.RawMessage(`{"seed":"x"}`),
		Status: WorkflowStatusRunning,
	}
	stages := make([]*TaskRun, stageCount)
	for i := range stages {
		stages[i] = &TaskRun{
			RunID:   plan.ID,
			Seq:     int16(i),
			Handler: "test.stage",
			Status:  TaskStatusPending,
		}
	}
	return plan, stages
}

// truncateWorkflows lets subtests that claim with no id filter avoid picking up
// a row another subtest left behind.
func truncateWorkflows(t *testing.T, db *Store, ctx context.Context) {
	t.Helper()
	_, err := db.DB.Query.Exec(ctx, db.DB.SQL.Delete(TableRuns))
	require.NoError(t, err)
}

// makeDue backdates next_retry_at so an immediate claim sees the plan as due.
func makeDue(t *testing.T, db *Store, ctx context.Context, runID uuid.UUID) {
	t.Helper()
	q := db.DB.SQL.Update(TableRuns).
		Set("next_retry_at", squirrel.Expr("now() - interval '1 second'")).
		Where(squirrel.Eq{"id": runID})
	_, err := db.DB.Query.Exec(ctx, q)
	require.NoError(t, err)
}

// backOffRun stands in for PersistTaskFailure's backoff so a release visibly
// preserves it.
func backOffRun(t *testing.T, db *Store, ctx context.Context, runID uuid.UUID) {
	t.Helper()
	q := db.DB.SQL.Update(TableRuns).
		Set("next_retry_at", squirrel.Expr("now() + interval '1 minute'")).
		Where(squirrel.Eq{"id": runID})
	res, err := db.DB.Query.Exec(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsAffected())
}

func seedDuePlan(t *testing.T, db *Store, ctx context.Context, stageCount int) (*WorkflowRun, []*TaskRun) {
	t.Helper()
	plan, stages := newTestPlan(stageCount)
	require.NoError(t, db.InsertRun(ctx, plan, stages))
	makeDue(t, db, ctx, plan.ID)
	return plan, stages
}

// requireAttemptLedger asserts attempt_count equals the ledger length: the two
// must never drift.
func requireAttemptLedger(t *testing.T, task *TaskRun, want int) {
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

// ageClaim is guarded on the live claim_id so it cannot touch a reclaimed row
// and mask a staleness-detection bug.
func ageClaim(t *testing.T, db *Store, ctx context.Context, runID uuid.UUID, claimID uuid.UUID, age time.Duration) {
	t.Helper()
	q := db.DB.SQL.Update(TableRuns).
		Set("claimed_at", squirrel.Expr("now() - make_interval(secs => ?)", age.Seconds())).
		Where(squirrel.Eq{"id": runID, "claim_id": claimID})
	res, err := db.DB.Query.Exec(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsAffected())
}

func TestWorkflowsTable(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	t.Run("InsertRun round-trips a plan and its stages", func(t *testing.T) {
		plan := &WorkflowRun{
			ID:     uuid.New(),
			Name:   "test.twostage",
			Input:  json.RawMessage(`{"seed":"x"}`),
			Status: WorkflowStatusRunning,
		}
		stages := []*TaskRun{
			{RunID: plan.ID, Seq: 0, Handler: "test.first", Status: TaskStatusPending},
			{RunID: plan.ID, Seq: 1, Handler: "test.second", Status: TaskStatusPending},
		}

		require.NoError(t, db.InsertRun(ctx, plan, stages))

		var gotPlan WorkflowRun
		planQ := db.DB.SQL.Select("*").From(TableRuns).Where(squirrel.Eq{"id": plan.ID})
		require.NoError(t, db.DB.Query.GetOne(ctx, planQ, &gotPlan))
		require.Equal(t, plan.ID, gotPlan.ID)
		require.Equal(t, plan.Name, gotPlan.Name)
		require.JSONEq(t, string(plan.Input), string(gotPlan.Input))
		require.Equal(t, plan.Status, gotPlan.Status)
		require.Equal(t, int16(0), gotPlan.CurrentTask)
		require.Nil(t, gotPlan.ClaimedAt)
		require.Nil(t, gotPlan.ClaimID)

		var gotStages []*TaskRun
		stagesQ := db.DB.SQL.Select("*").From(TableTasks).Where(squirrel.Eq{"run_id": plan.ID}).OrderBy("seq")
		require.NoError(t, db.DB.Query.GetAll(ctx, stagesQ, &gotStages))
		require.Len(t, gotStages, 2)
		require.Equal(t, "test.first", gotStages[0].Handler)
		require.Equal(t, "test.second", gotStages[1].Handler)
		require.Equal(t, TaskStatusPending, gotStages[0].Status)
		require.Equal(t, TaskStatusPending, gotStages[1].Status)
	})

	t.Run("UpsertRun", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)

		t.Run("inserts a new plan and reports inserted", func(t *testing.T) {
			plan, stages := newTestPlan(1)
			plan.IdempotencyKey = new("cust:new")

			id, inserted, err := db.UpsertRun(ctx, plan, stages)
			require.NoError(t, err)
			require.True(t, inserted)
			require.Equal(t, plan.ID, id)

			var got WorkflowRun
			require.NoError(t, db.DB.Query.GetOne(ctx, db.DB.SQL.Select("*").From(TableRuns).Where(squirrel.Eq{"id": id}), &got))
			require.Equal(t, WorkflowStatusRunning, got.Status)
		})

		t.Run("leaves a stuck plan parked", func(t *testing.T) {
			plan, stages := newTestPlan(2)
			plan.IdempotencyKey = new("cust:stuck")
			firstID, inserted, err := db.UpsertRun(ctx, plan, stages)
			require.NoError(t, err)
			require.True(t, inserted)
			_, err = db.DB.Query.Exec(ctx, db.DB.SQL.Update(TableRuns).
				Set("status", WorkflowStatusStuck).
				Set("current_task", 1).
				Where(squirrel.Eq{"id": firstID}))
			require.NoError(t, err)
			_, err = db.DB.Query.Exec(ctx, db.DB.SQL.Update(TableTasks).
				Set("status", TaskStatusSucceeded).
				Set("output", json.RawMessage(`{"result":"first"}`)).
				Where(squirrel.Eq{"run_id": firstID, "seq": 0}))
			require.NoError(t, err)
			_, err = db.DB.Query.Exec(ctx, db.DB.SQL.Update(TableTasks).
				Set("status", TaskStatusStuck).
				Set("attempt_count", 8).
				Set("last_error", "boom").
				Where(squirrel.Eq{"run_id": firstID, "seq": 1}))
			require.NoError(t, err)

			retry, retryStages := newTestPlan(2)
			retry.IdempotencyKey = new("cust:stuck")
			gotID, inserted, err := db.UpsertRun(ctx, retry, retryStages)
			require.NoError(t, err)
			require.False(t, inserted)
			require.Equal(t, firstID, gotID)

			gotPlan, err := db.GetRun(ctx, firstID)
			require.NoError(t, err)
			require.Equal(t, WorkflowStatusStuck, gotPlan.Status)
			require.Equal(t, int16(1), gotPlan.CurrentTask)
			gotStages, err := db.GetTasks(ctx, firstID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusSucceeded, gotStages[0].Status)
			require.JSONEq(t, `{"result":"first"}`, string(gotStages[0].Output))
			require.Equal(t, TaskStatusStuck, gotStages[1].Status)
			require.Equal(t, 8, gotStages[1].AttemptCount)
			require.NotNil(t, gotStages[1].LastError)
			require.Equal(t, "boom", *gotStages[1].LastError)
		})

		t.Run("no-ops when the plan is still running", func(t *testing.T) {
			plan, stages := newTestPlan(1)
			plan.IdempotencyKey = new("cust:running")
			firstID, _, err := db.UpsertRun(ctx, plan, stages)
			require.NoError(t, err)

			plan2, stages2 := newTestPlan(1)
			plan2.IdempotencyKey = new("cust:running")
			gotID, inserted2, err := db.UpsertRun(ctx, plan2, stages2)
			require.NoError(t, err)
			require.False(t, inserted2)
			require.Equal(t, firstID, gotID)

			var count int
			require.NoError(t, db.DB.Query.GetOne(ctx, db.DB.SQL.Select("count(*)").From(TableRuns).Where(squirrel.Eq{"id": plan2.ID}), &count))
			require.Equal(t, 0, count)
		})
	})

	t.Run("ClaimRuns claims a claimable plan and re-claim is empty", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)
		plan, _ := seedDuePlan(t, db, ctx, 2)

		claimed, err := db.ClaimRuns(ctx, ClaimStrategyFIFO, []string{testWorkflowName}, 1)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		got := claimed[0]
		require.Equal(t, plan.ID, got.Run.ID)
		require.NotEqual(t, uuid.Nil, got.ClaimID)
		require.NotNil(t, got.Run.ClaimID)
		require.Equal(t, got.ClaimID, *got.Run.ClaimID)
		require.NotNil(t, got.Run.ClaimedAt)
		require.Len(t, got.Tasks, 2)
		require.Equal(t, int16(0), got.Tasks[0].Seq)
		require.Equal(t, int16(1), got.Tasks[1].Seq)

		reclaimed, err := db.ClaimRuns(ctx, ClaimStrategyFIFO, []string{testWorkflowName}, 1)
		require.NoError(t, err)
		require.Empty(t, reclaimed)
	})

	t.Run("ClaimRuns leaves a due run whose workflow it was not asked for", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)
		plan, _ := seedDuePlan(t, db, ctx, 1)

		claimed, err := db.ClaimRuns(ctx, ClaimStrategyFIFO, []string{"test.other"}, 1)
		require.NoError(t, err)
		require.Empty(t, claimed)

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Nil(t, got.ClaimID)
		require.Nil(t, got.ClaimedAt)
		require.Equal(t, WorkflowStatusRunning, got.Status)
	})

	t.Run("ClaimRuns with no workflow names claims nothing", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)
		plan, _ := seedDuePlan(t, db, ctx, 1)

		claimed, err := db.ClaimRuns(ctx, ClaimStrategyFIFO, nil, 1)
		require.NoError(t, err)
		require.Empty(t, claimed)

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Nil(t, got.ClaimID)
	})

	t.Run("ClaimStrategyFIFO claims the oldest due run first", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)
		older, _ := seedDuePlan(t, db, ctx, 1)
		newer, _ := seedDuePlan(t, db, ctx, 1)
		_, err := db.DB.Query.Exec(ctx, db.DB.SQL.Update(TableRuns).
			Set("next_retry_at", squirrel.Expr("now() - interval '2 seconds'")).
			Where(squirrel.Eq{"id": older.ID}))
		require.NoError(t, err)
		_, err = db.DB.Query.Exec(ctx, db.DB.SQL.Update(TableRuns).
			Set("next_retry_at", squirrel.Expr("now() - interval '1 second'")).
			Where(squirrel.Eq{"id": newer.ID}))
		require.NoError(t, err)

		claimed, err := db.ClaimRuns(ctx, ClaimStrategyFIFO, []string{testWorkflowName}, 1)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.Equal(t, older.ID, claimed[0].Run.ID)
		untouched, err := db.GetRun(ctx, newer.ID)
		require.NoError(t, err)
		require.Nil(t, untouched.ClaimID)
	})

	t.Run("ClaimRuns rejects an unknown claim strategy without claiming", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)
		plan, _ := seedDuePlan(t, db, ctx, 1)

		claimed, err := db.ClaimRuns(ctx, ClaimStrategy(99), []string{testWorkflowName}, 1)
		require.Error(t, err)
		require.Empty(t, claimed)

		got, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Nil(t, got.ClaimID)
		require.Nil(t, got.ClaimedAt)
	})

	t.Run("claim contention lets exactly one claimer win", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)
		seedDuePlan(t, db, ctx, 1)

		var wg sync.WaitGroup
		results := make([][]*ClaimedRun, 2)
		errs := make([]error, 2)
		for i := range 2 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], errs[i] = db.ClaimRuns(ctx, ClaimStrategyFIFO, []string{testWorkflowName}, 1)
			}(i)
		}
		wg.Wait()

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		totalClaimed := len(results[0]) + len(results[1])
		require.Equal(t, 1, totalClaimed, "exactly one claimer must win the plan")
	})

	t.Run("ClaimRunByID claims a claimable plan and re-claim returns false", func(t *testing.T) {
		plan, _ := seedDuePlan(t, db, ctx, 1)

		got, ok, err := db.ClaimRunByID(ctx, plan.ID)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, plan.ID, got.Run.ID)
		require.NotEqual(t, uuid.Nil, got.ClaimID)
		require.NotNil(t, got.Run.ClaimedAt)
		require.Len(t, got.Tasks, 1)

		reclaimed, ok, err := db.ClaimRunByID(ctx, plan.ID)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, reclaimed)
	})

	t.Run("MarkTaskRunning", func(t *testing.T) {
		t.Run("transitions the current stage from pending to running", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 2)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusRunning, gotStages[0].Status)
			require.Equal(t, TaskStatusPending, gotStages[1].Status)
		})

		t.Run("guard fails on wrong claimID", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			_, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, uuid.New())
			require.NoError(t, err)
			require.False(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusPending, gotStages[0].Status)
		})

		t.Run("guard fails on wrong current_task", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 2)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			// current_task is 0; asking to mark seq 1 running must fail.
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 1, got.ClaimID)
			require.NoError(t, err)
			require.False(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusPending, gotStages[0].Status)
			require.Equal(t, TaskStatusPending, gotStages[1].Status)
		})
	})

	t.Run("PersistTaskSuccess", func(t *testing.T) {
		t.Run("advances current_task and writes stage output", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 2)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			output := json.RawMessage(`{"result":"ok"}`)
			ok, err = db.PersistTaskSuccess(ctx, plan.ID, 0, output, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, int16(1), gotPlan.CurrentTask)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusSucceeded, gotStages[0].Status)
			require.JSONEq(t, string(output), string(gotStages[0].Output))
			require.Equal(t, TaskStatusPending, gotStages[1].Status)
		})

		t.Run("false on stale claim and the stage output is left untouched", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.PersistTaskSuccess(ctx, plan.ID, 0, json.RawMessage(`{"result":"stale"}`), uuid.New())
			require.NoError(t, err)
			require.False(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, int16(0), gotPlan.CurrentTask)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusRunning, gotStages[0].Status)
			require.Nil(t, gotStages[0].Output)
		})
	})

	t.Run("PersistTaskFailure", func(t *testing.T) {
		t.Run("release=true clears the claim and resets the stage to pending", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			before := time.Now()
			backoff := 30 * time.Second
			ok, err = db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: "boom"}, backoff, true, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, gotPlan.ClaimedAt)
			require.Nil(t, gotPlan.ClaimID)
			require.WithinDuration(t, before.Add(backoff), gotPlan.NextRetryAt, 5*time.Second)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusPending, gotStages[0].Status)
			require.Equal(t, 1, gotStages[0].AttemptCount)
			require.NotNil(t, gotStages[0].LastError)
			require.Equal(t, "boom", *gotStages[0].LastError)
			require.WithinDuration(t, before.Add(backoff), gotStages[0].NextRetryAt, 5*time.Second)

			var attempts []struct {
				N     int    `json:"n"`
				Error string `json:"error"`
			}
			require.NoError(t, json.Unmarshal(gotStages[0].Attempts, &attempts))
			require.Len(t, attempts, 1)
			require.Equal(t, 1, attempts[0].N)
			require.Equal(t, "boom", attempts[0].Error)
		})

		t.Run("release=false keeps the claim and leaves the stage running", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: "boom again"}, 10*time.Second, false, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.NotNil(t, gotPlan.ClaimedAt)
			require.NotNil(t, gotPlan.ClaimID)
			require.Equal(t, got.ClaimID, *gotPlan.ClaimID)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusRunning, gotStages[0].Status)
			require.Equal(t, 1, gotStages[0].AttemptCount)
			require.NotNil(t, gotStages[0].LastError)
			require.Equal(t, "boom again", *gotStages[0].LastError)
		})

		t.Run("false on stale claim and nothing mutates", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: "zombie"}, 10*time.Second, true, uuid.New())
			require.NoError(t, err)
			require.False(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.NotNil(t, gotPlan.ClaimedAt)
			require.NotNil(t, gotPlan.ClaimID)
			require.Equal(t, got.ClaimID, *gotPlan.ClaimID)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusRunning, gotStages[0].Status)
			require.Equal(t, 0, gotStages[0].AttemptCount)
			require.Nil(t, gotStages[0].LastError)
		})
	})

	t.Run("AdvanceSkippedTask", func(t *testing.T) {
		t.Run("advances current_task and leaves the skipped stage row untouched", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 2)
			skipQ := db.DB.SQL.Update(TableTasks).
				Set("status", TaskStatusSkipped).
				Where(squirrel.Eq{"run_id": plan.ID, "seq": 0})
			_, err := db.DB.Query.Exec(ctx, skipQ)
			require.NoError(t, err)

			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.AdvanceSkippedTask(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, int16(1), gotPlan.CurrentTask)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusSkipped, gotStages[0].Status)
			require.Equal(t, TaskStatusPending, gotStages[1].Status)
		})

		t.Run("false on stale claim", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			_, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.AdvanceSkippedTask(ctx, plan.ID, 0, uuid.New())
			require.NoError(t, err)
			require.False(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, int16(0), gotPlan.CurrentTask)
		})
	})

	t.Run("Heartbeat", func(t *testing.T) {
		t.Run("refreshes claimed_at for the live claim holder", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			// Backdate the fresh claim so the refresh is a strict advance
			// rather than a same-instant tie.
			ageClaim(t, db, ctx, plan.ID, got.ClaimID, 2*time.Second)
			backdated, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.NotNil(t, backdated.ClaimedAt)
			firstClaimedAt := *backdated.ClaimedAt

			ok, err = db.Heartbeat(ctx, plan.ID, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.NotNil(t, gotPlan.ClaimedAt)
			require.Truef(t, gotPlan.ClaimedAt.After(firstClaimedAt),
				"heartbeat must advance claimed_at: got %s, want after %s", gotPlan.ClaimedAt, firstClaimedAt)
		})

		t.Run("false on stale claim", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			_, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.Heartbeat(ctx, plan.ID, uuid.New())
			require.NoError(t, err)
			require.False(t, ok)
		})
	})

	t.Run("CompleteRun", func(t *testing.T) {
		t.Run("marks the plan succeeded and clears the claim", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.CompleteRun(ctx, plan.ID, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, WorkflowStatusSucceeded, gotPlan.Status)
			require.NotNil(t, gotPlan.CompletedAt)
			require.Nil(t, gotPlan.ClaimedAt)
			require.Nil(t, gotPlan.ClaimID)
		})

		t.Run("false on stale claim and the plan is left running", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			_, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.CompleteRun(ctx, plan.ID, uuid.New())
			require.NoError(t, err)
			require.False(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, WorkflowStatusRunning, gotPlan.Status)
			require.Nil(t, gotPlan.CompletedAt)
		})
	})

	t.Run("ParkRunAfterAttempt and ParkRunWithoutAttempt", func(t *testing.T) {
		claimWithFailures := func(t *testing.T, failures int) (*WorkflowRun, uuid.UUID) {
			t.Helper()
			plan, _ := seedDuePlan(t, db, ctx, 1)
			claimed, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, claimed.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			for range failures {
				ok, err = db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: "boom"}, time.Second, false, claimed.ClaimID)
				require.NoError(t, err)
				require.True(t, ok)
			}
			return plan, claimed.ClaimID
		}

		t.Run("marks the plan and current stage stuck and clears the claim", func(t *testing.T) {
			plan, claimID := claimWithFailures(t, 0)

			ok, err := db.ParkRunAfterAttempt(ctx, plan.ID, 0, TaskFailure{Message: "gave up"}, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, WorkflowStatusStuck, gotPlan.Status)
			require.Nil(t, gotPlan.ClaimedAt)
			require.Nil(t, gotPlan.ClaimID)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusStuck, gotStages[0].Status)
			require.NotNil(t, gotStages[0].LastError)
			require.Equal(t, "gave up", *gotStages[0].LastError)
			requireAttemptLedger(t, gotStages[0], 1)
		})

		t.Run("ParkRunAfterAttempt records the attempt it parks on", func(t *testing.T) {
			plan, claimID := claimWithFailures(t, 2)

			ok, err := db.ParkRunAfterAttempt(ctx, plan.ID, 0, TaskFailure{Message: "gave up"}, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			requireAttemptLedger(t, gotStages[0], 3)
		})

		t.Run("ParkRunWithoutAttempt records no attempt", func(t *testing.T) {
			plan, claimID := claimWithFailures(t, 2)

			ok, err := db.ParkRunWithoutAttempt(ctx, plan.ID, 0, TaskFailure{Message: "budget spent"}, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusStuck, gotStages[0].Status)
			requireAttemptLedger(t, gotStages[0], 2)
		})

		t.Run("a tagged failure records its code", func(t *testing.T) {
			plan, claimID := claimWithFailures(t, 1)

			ok, err := db.ParkRunAfterAttempt(ctx,
				plan.ID, 0, TaskFailure{Message: "rejected", Code: "invalid_input"}, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.NotNil(t, gotStages[0].LastErrorCode)
			require.Equal(t, "invalid_input", *gotStages[0].LastErrorCode)
		})

		t.Run("an untagged failure leaves the code null", func(t *testing.T) {
			plan, claimID := claimWithFailures(t, 1)

			ok, err := db.ParkRunAfterAttempt(ctx, plan.ID, 0, TaskFailure{Message: "gave up"}, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, gotStages[0].LastErrorCode)
		})

		t.Run("false on stale claim and nothing mutates", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = db.ParkRunAfterAttempt(ctx, plan.ID, 0, TaskFailure{Message: "zombie park"}, uuid.New())
			require.NoError(t, err)
			require.False(t, ok)

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, WorkflowStatusRunning, gotPlan.Status)
			require.NotNil(t, gotPlan.ClaimedAt)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusRunning, gotStages[0].Status)
			require.Nil(t, gotStages[0].LastError)
		})
	})

	t.Run("task error truncation", func(t *testing.T) {
		// Byte maxErrMsgLen lands inside a multibyte rune, so a byte-boundary
		// cut leaves a partial rune that Postgres refuses to store.
		splitRune := strings.Repeat("a", maxErrMsgLen-2) + strings.Repeat("€", 40)
		require.Greater(t, len(splitRune), maxErrMsgLen)
		require.False(t, utf8.ValidString(splitRune[:maxErrMsgLen]))
		wantSplitRune := strings.Repeat("a", maxErrMsgLen-2)

		asciiOverflow := strings.Repeat("b", 600)
		wantASCII := strings.Repeat("b", maxErrMsgLen)

		claimRunning := func(t *testing.T) (*WorkflowRun, uuid.UUID) {
			t.Helper()
			plan, _ := seedDuePlan(t, db, ctx, 1)
			claimed, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, claimed.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			return plan, claimed.ClaimID
		}

		requireStored := func(t *testing.T, task *TaskRun, want string) {
			t.Helper()
			require.NotNil(t, task.LastError)
			require.True(t, utf8.ValidString(*task.LastError))
			require.LessOrEqual(t, len(*task.LastError), maxErrMsgLen)
			require.Equal(t, want, *task.LastError)

			var attempts []struct {
				Error string `json:"error"`
			}
			require.NoError(t, json.Unmarshal(task.Attempts, &attempts))
			require.NotEmpty(t, attempts)
			last := attempts[len(attempts)-1].Error
			require.True(t, utf8.ValidString(last))
			require.Equal(t, want, last)
		}

		t.Run("PersistTaskFailure records a message cut mid-rune", func(t *testing.T) {
			plan, claimID := claimRunning(t)

			ok, err := db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: splitRune}, time.Second, false, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			requireStored(t, gotStages[0], wantSplitRune)
			requireAttemptLedger(t, gotStages[0], 1)
		})

		t.Run("ParkRunAfterAttempt records a message cut mid-rune", func(t *testing.T) {
			plan, claimID := claimRunning(t)

			ok, err := db.ParkRunAfterAttempt(ctx, plan.ID, 0, TaskFailure{Message: splitRune}, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusStuck, gotStages[0].Status)
			requireStored(t, gotStages[0], wantSplitRune)
			requireAttemptLedger(t, gotStages[0], 1)
		})

		t.Run("PersistTaskFailure truncates an ASCII message to exactly maxErrMsgLen bytes", func(t *testing.T) {
			plan, claimID := claimRunning(t)

			ok, err := db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: asciiOverflow}, time.Second, false, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Len(t, *gotStages[0].LastError, maxErrMsgLen)
			requireStored(t, gotStages[0], wantASCII)
			requireAttemptLedger(t, gotStages[0], 1)
		})

		t.Run("ParkRunAfterAttempt truncates an ASCII message to exactly maxErrMsgLen bytes", func(t *testing.T) {
			plan, claimID := claimRunning(t)

			ok, err := db.ParkRunAfterAttempt(ctx, plan.ID, 0, TaskFailure{Message: asciiOverflow}, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Len(t, *gotStages[0].LastError, maxErrMsgLen)
			requireStored(t, gotStages[0], wantASCII)
			requireAttemptLedger(t, gotStages[0], 1)
		})

		t.Run("PersistTaskFailure strips a NUL byte from the recorded message", func(t *testing.T) {
			plan, claimID := claimRunning(t)
			withNUL := "boom\x00 with an embedded NUL"
			wantNoNUL := strings.ReplaceAll(withNUL, "\x00", "")

			ok, err := db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: withNUL}, time.Second, false, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			requireStored(t, gotStages[0], wantNoNUL)
			requireAttemptLedger(t, gotStages[0], 1)
		})

		t.Run("PersistTaskFailure strips a NUL byte alongside a mid-rune cut past maxErrMsgLen", func(t *testing.T) {
			plan, claimID := claimRunning(t)
			nulSplitRune := "\x00" + splitRune

			ok, err := db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: nulSplitRune}, time.Second, false, claimID)
			require.NoError(t, err)
			require.True(t, ok)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			requireStored(t, gotStages[0], wantSplitRune)
			requireAttemptLedger(t, gotStages[0], 1)
		})
	})

	t.Run("ReleaseClaim", func(t *testing.T) {
		t.Run("clears the claim, resets a running stage to pending, and leaves next_retry_at as persisted", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			backOffRun(t, db, ctx, plan.ID)

			require.NoError(t, db.ReleaseClaim(ctx, plan.ID, 0, got.ClaimID))

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, gotPlan.ClaimedAt)
			require.Nil(t, gotPlan.ClaimID)
			require.WithinDuration(t, time.Now().Add(time.Minute), gotPlan.NextRetryAt, 5*time.Second)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusPending, gotStages[0].Status)
		})

		t.Run("no-op when the current stage is already pending", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)

			require.NoError(t, db.ReleaseClaim(ctx, plan.ID, 0, got.ClaimID))

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, gotPlan.ClaimedAt)
			require.Nil(t, gotPlan.ClaimID)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusPending, gotStages[0].Status)
		})

		t.Run("a reassigned claim cannot wipe the new owner's claim", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)

			require.NoError(t, db.ReleaseClaim(ctx, plan.ID, 0, uuid.New()))

			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.NotNil(t, gotPlan.ClaimedAt)
			require.NotNil(t, gotPlan.ClaimID)
			require.Equal(t, got.ClaimID, *gotPlan.ClaimID)

			gotStages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusRunning, gotStages[0].Status)
		})
	})

	t.Run("ownership writes land on a cancelled context", func(t *testing.T) {
		dead, cancel := context.WithCancel(ctx)
		cancel()

		claimRunning := func(t *testing.T, stageCount int) (*WorkflowRun, *ClaimedRun) {
			t.Helper()
			plan, _ := seedDuePlan(t, db, ctx, stageCount)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			return plan, got
		}

		t.Run("the write context is detached and bounded", func(t *testing.T) {
			writeCtx, cancel := ownershipWrite(dead)
			defer cancel()
			require.NoError(t, writeCtx.Err())
			deadline, ok := writeCtx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(ownershipWriteBudget), deadline, 5*time.Second)
		})

		t.Run("PersistTaskSuccess", func(t *testing.T) {
			plan, got := claimRunning(t, 2)
			ok, err := db.PersistTaskSuccess(dead, plan.ID, 0, json.RawMessage(`{"result":"ok"}`), got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, int16(1), gotPlan.CurrentTask)
		})

		t.Run("PersistTaskFailure", func(t *testing.T) {
			plan, got := claimRunning(t, 1)
			ok, err := db.PersistTaskFailure(dead, plan.ID, 0, TaskFailure{Message: "boom"}, time.Second, false, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, 1, stages[0].AttemptCount)
		})

		t.Run("AdvanceSkippedTask", func(t *testing.T) {
			plan, stages := newTestPlan(2)
			stages[0].Status = TaskStatusSkipped
			require.NoError(t, db.InsertRun(ctx, plan, stages))
			makeDue(t, db, ctx, plan.ID)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = db.AdvanceSkippedTask(dead, plan.ID, 0, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, int16(1), gotPlan.CurrentTask)
		})

		t.Run("CompleteRun", func(t *testing.T) {
			plan, got := claimRunning(t, 1)
			ok, err := db.CompleteRun(dead, plan.ID, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, WorkflowStatusSucceeded, gotPlan.Status)
		})

		t.Run("ParkRunAfterAttempt", func(t *testing.T) {
			plan, got := claimRunning(t, 1)
			ok, err := db.ParkRunAfterAttempt(dead, plan.ID, 0, TaskFailure{Message: "boom"}, got.ClaimID)
			require.NoError(t, err)
			require.True(t, ok)
			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, WorkflowStatusStuck, gotPlan.Status)
		})

		t.Run("ReleaseClaim", func(t *testing.T) {
			plan, got := claimRunning(t, 1)
			require.NoError(t, db.ReleaseClaim(dead, plan.ID, 0, got.ClaimID))
			gotPlan, err := db.GetRun(ctx, plan.ID)
			require.NoError(t, err)
			require.Nil(t, gotPlan.ClaimID)
		})

		t.Run("MarkTaskRunning still honours cancellation", func(t *testing.T) {
			plan, _ := seedDuePlan(t, db, ctx, 1)
			got, ok, err := db.ClaimRunByID(ctx, plan.ID)
			require.NoError(t, err)
			require.True(t, ok)
			_, err = db.MarkTaskRunning(dead, plan.ID, 0, got.ClaimID)
			require.ErrorIs(t, err, context.Canceled)
			stages, err := db.GetTasks(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusPending, stages[0].Status)
		})
	})

	t.Run("SweepStuck", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)

		runningPlan, _ := seedDuePlan(t, db, ctx, 1)
		runningClaim, ok, err := db.ClaimRunByID(ctx, runningPlan.ID)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = db.MarkTaskRunning(ctx, runningPlan.ID, 0, runningClaim.ClaimID)
		require.NoError(t, err)
		require.True(t, ok)
		ageClaim(t, db, ctx, runningPlan.ID, runningClaim.ClaimID, time.Hour)

		// Crashed before MarkTaskRunning ran.
		pendingPlan, _ := seedDuePlan(t, db, ctx, 1)
		pendingClaim, ok, err := db.ClaimRunByID(ctx, pendingPlan.ID)
		require.NoError(t, err)
		require.True(t, ok)
		ageClaim(t, db, ctx, pendingPlan.ID, pendingClaim.ClaimID, time.Hour)

		// Crashed before AdvanceSkippedTask moved current_task past it.
		skippedPlan, _ := seedDuePlan(t, db, ctx, 1)
		skipQ := db.DB.SQL.Update(TableTasks).
			Set("status", TaskStatusSkipped).
			Where(squirrel.Eq{"run_id": skippedPlan.ID, "seq": 0})
		_, err = db.DB.Query.Exec(ctx, skipQ)
		require.NoError(t, err)
		skippedClaim, ok, err := db.ClaimRunByID(ctx, skippedPlan.ID)
		require.NoError(t, err)
		require.True(t, ok)
		ageClaim(t, db, ctx, skippedPlan.ID, skippedClaim.ClaimID, time.Hour)

		// current_task already advanced past the last task row.
		nearCompleteRun, _ := seedDuePlan(t, db, ctx, 1)
		nearCompleteClaim, ok, err := db.ClaimRunByID(ctx, nearCompleteRun.ID)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = db.MarkTaskRunning(ctx, nearCompleteRun.ID, 0, nearCompleteClaim.ClaimID)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = db.PersistTaskSuccess(ctx, nearCompleteRun.ID, 0, json.RawMessage(`{}`), nearCompleteClaim.ClaimID)
		require.NoError(t, err)
		require.True(t, ok)
		ageClaim(t, db, ctx, nearCompleteRun.ID, nearCompleteClaim.ClaimID, time.Hour)

		released, err := db.SweepStuck(ctx, time.Minute)
		require.NoError(t, err)
		require.EqualValues(t, 4, released)

		runningStages, err := db.GetTasks(ctx, runningPlan.ID)
		require.NoError(t, err)
		require.Equal(t, TaskStatusPending, runningStages[0].Status)
		require.Equal(t, 1, runningStages[0].AttemptCount)
		require.NotNil(t, runningStages[0].LastError)
		require.Equal(t, "reclaimed stale claim", *runningStages[0].LastError)
		runningGotPlan, err := db.GetRun(ctx, runningPlan.ID)
		require.NoError(t, err)
		require.Nil(t, runningGotPlan.ClaimedAt)
		require.Nil(t, runningGotPlan.ClaimID)

		pendingStages, err := db.GetTasks(ctx, pendingPlan.ID)
		require.NoError(t, err)
		require.Equal(t, TaskStatusPending, pendingStages[0].Status)
		require.Equal(t, 0, pendingStages[0].AttemptCount)
		require.Nil(t, pendingStages[0].LastError)
		pendingGotPlan, err := db.GetRun(ctx, pendingPlan.ID)
		require.NoError(t, err)
		require.Nil(t, pendingGotPlan.ClaimedAt)
		require.Nil(t, pendingGotPlan.ClaimID)

		skippedStages, err := db.GetTasks(ctx, skippedPlan.ID)
		require.NoError(t, err)
		require.Equal(t, TaskStatusSkipped, skippedStages[0].Status)
		require.Equal(t, 0, skippedStages[0].AttemptCount)
		skippedGotPlan, err := db.GetRun(ctx, skippedPlan.ID)
		require.NoError(t, err)
		require.Nil(t, skippedGotPlan.ClaimedAt)
		require.Nil(t, skippedGotPlan.ClaimID)

		nearCompleteGotPlan, err := db.GetRun(ctx, nearCompleteRun.ID)
		require.NoError(t, err)
		require.Nil(t, nearCompleteGotPlan.ClaimedAt)
		require.Nil(t, nearCompleteGotPlan.ClaimID)
		require.Equal(t, int16(1), nearCompleteGotPlan.CurrentTask)
	})

	t.Run("a stale claimID write hits 0 rows", func(t *testing.T) {
		truncateWorkflows(t, db, ctx)
		plan, _ := seedDuePlan(t, db, ctx, 2)

		claimX, ok, err := db.ClaimRunByID(ctx, plan.ID)
		require.NoError(t, err)
		require.True(t, ok)
		claimXID := claimX.ClaimID

		// Simulate a crashed executor X: the sweeper reclaims it, then Y claims.
		ageClaim(t, db, ctx, plan.ID, claimXID, time.Hour)
		released, err := db.SweepStuck(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, released)
		makeDue(t, db, ctx, plan.ID)

		claimY, ok, err := db.ClaimRunByID(ctx, plan.ID)
		require.NoError(t, err)
		require.True(t, ok)
		claimYID := claimY.ClaimID
		require.NotEqual(t, claimXID, claimYID)

		ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, claimXID)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = db.PersistTaskSuccess(ctx, plan.ID, 0, json.RawMessage(`{"result":"zombie"}`), claimXID)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = db.PersistTaskFailure(ctx, plan.ID, 0, TaskFailure{Message: "zombie failure"}, time.Second, true, claimXID)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = db.AdvanceSkippedTask(ctx, plan.ID, 0, claimXID)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = db.Heartbeat(ctx, plan.ID, claimXID)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = db.CompleteRun(ctx, plan.ID, claimXID)
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = db.ParkRunAfterAttempt(ctx, plan.ID, 0, TaskFailure{Message: "zombie park"}, claimXID)
		require.NoError(t, err)
		require.False(t, ok)

		gotPlan, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, WorkflowStatusRunning, gotPlan.Status)
		require.Equal(t, int16(0), gotPlan.CurrentTask)
		require.NotNil(t, gotPlan.ClaimID)
		require.Equal(t, claimYID, *gotPlan.ClaimID)

		gotStages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, TaskStatusPending, gotStages[0].Status)
		require.Equal(t, 0, gotStages[0].AttemptCount)
		require.Nil(t, gotStages[0].Output)
		require.Nil(t, gotStages[0].LastError)

		ok, err = db.MarkTaskRunning(ctx, plan.ID, 0, claimYID)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("GetRun and GetTasks read back a plan and its stages ordered by seq", func(t *testing.T) {
		plan, _ := newTestPlan(2)
		require.NoError(t, db.InsertRun(ctx, plan, []*TaskRun{
			{RunID: plan.ID, Seq: 0, Handler: "test.first", Status: TaskStatusPending},
			{RunID: plan.ID, Seq: 1, Handler: "test.second", Status: TaskStatusPending},
		}))

		gotPlan, err := db.GetRun(ctx, plan.ID)
		require.NoError(t, err)
		require.Equal(t, plan.ID, gotPlan.ID)
		require.Equal(t, plan.Name, gotPlan.Name)

		gotStages, err := db.GetTasks(ctx, plan.ID)
		require.NoError(t, err)
		require.Len(t, gotStages, 2)
		require.Equal(t, int16(0), gotStages[0].Seq)
		require.Equal(t, "test.first", gotStages[0].Handler)
		require.Equal(t, int16(1), gotStages[1].Seq)
		require.Equal(t, "test.second", gotStages[1].Handler)
	})

	t.Run("OldestPending", func(t *testing.T) {
		t.Run("returns the earliest next_retry_at of the running, unclaimed backlog", func(t *testing.T) {
			truncateWorkflows(t, db, ctx)

			older, _ := newTestPlan(1)
			require.NoError(t, db.InsertRun(ctx, older, []*TaskRun{
				{RunID: older.ID, Seq: 0, Handler: "test.stage", Status: TaskStatusPending},
			}))
			backdateQ := db.DB.SQL.Update(TableRuns).
				Set("next_retry_at", squirrel.Expr("now() - interval '1 hour'")).
				Where(squirrel.Eq{"id": older.ID})
			_, err := db.DB.Query.Exec(ctx, backdateQ)
			require.NoError(t, err)
			gotOlder, err := db.GetRun(ctx, older.ID)
			require.NoError(t, err)

			newer, _ := newTestPlan(1)
			require.NoError(t, db.InsertRun(ctx, newer, []*TaskRun{
				{RunID: newer.ID, Seq: 0, Handler: "test.stage", Status: TaskStatusPending},
			}))

			oldest, err := db.OldestPending(ctx)
			require.NoError(t, err)
			require.NotNil(t, oldest)
			require.Equal(t, gotOlder.NextRetryAt, *oldest)

			_, ok, err := db.ClaimRunByID(ctx, older.ID)
			require.NoError(t, err)
			require.True(t, ok)

			oldestAfterClaim, err := db.OldestPending(ctx)
			require.NoError(t, err)
			require.NotNil(t, oldestAfterClaim)
			require.NotEqual(t, *oldest, *oldestAfterClaim)
		})

		t.Run("nil when the backlog is fully drained", func(t *testing.T) {
			truncateWorkflows(t, db, ctx)

			oldest, err := db.OldestPending(ctx)
			require.NoError(t, err)
			require.Nil(t, oldest)
		})
	})
}
