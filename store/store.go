package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/goware/pgkit/v2"
	"github.com/goware/pgkit/v2/pgerr"
	"github.com/jackc/pgx/v5"
)

const (
	TableRuns  = "workflow_runs"
	TableTasks = "workflow_tasks"
)

// WorkflowStatus is a workflow run's persisted lifecycle state.
// The numeric values are wire format: append, never renumber.
type WorkflowStatus int8

const (
	WorkflowStatusRunning   WorkflowStatus = 1
	WorkflowStatusSucceeded WorkflowStatus = 2
	WorkflowStatusStuck     WorkflowStatus = 3
	WorkflowStatusCancelled WorkflowStatus = 4
)

// String returns the status's durable lowercase name.
func (s WorkflowStatus) String() string {
	switch s {
	case WorkflowStatusRunning:
		return "running"
	case WorkflowStatusSucceeded:
		return "succeeded"
	case WorkflowStatusStuck:
		return "stuck"
	case WorkflowStatusCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("WorkflowStatus(%d)", int8(s))
	}
}

// TaskStatus is a workflow task's persisted lifecycle state.
// The numeric values are wire format: append, never renumber.
type TaskStatus int8

const (
	TaskStatusPending   TaskStatus = 1
	TaskStatusRunning   TaskStatus = 2
	TaskStatusSucceeded TaskStatus = 3
	TaskStatusStuck     TaskStatus = 4
	TaskStatusSkipped   TaskStatus = 5
)

// String returns the status's durable lowercase name.
func (s TaskStatus) String() string {
	switch s {
	case TaskStatusPending:
		return "pending"
	case TaskStatusRunning:
		return "running"
	case TaskStatusSucceeded:
		return "succeeded"
	case TaskStatusStuck:
		return "stuck"
	case TaskStatusSkipped:
		return "skipped"
	default:
		return fmt.Sprintf("TaskStatus(%d)", int8(s))
	}
}

// WorkflowRun is one row of a workflow run.
type WorkflowRun struct {
	ID             uuid.UUID       `db:"id"`
	Name           string          `db:"name"`
	IdempotencyKey *string         `db:"idempotency_key"`
	Input          json.RawMessage `db:"input"`
	CallContext    json.RawMessage `db:"call_context"`
	Status         WorkflowStatus  `db:"status"`
	CurrentTask    int16           `db:"current_task"`
	ClaimedAt      *time.Time      `db:"claimed_at"`
	ClaimID        *uuid.UUID      `db:"claim_id"`
	NextRetryAt    time.Time       `db:"next_retry_at"`
	CreatedAt      time.Time       `db:"created_at"`
	UpdatedAt      time.Time       `db:"updated_at"`
	CompletedAt    *time.Time      `db:"completed_at"`
}

// TaskRun is one snapshotted task row of a WorkflowRun.
type TaskRun struct {
	RunID uuid.UUID `db:"run_id"`
	// Seq is the task's ordinal within the run, not a retry counter.
	Seq           int16           `db:"seq"`
	Handler       string          `db:"handler"`
	Output        json.RawMessage `db:"output"`
	Status        TaskStatus      `db:"status"`
	AttemptCount  int             `db:"attempt_count"`
	LastError     *string         `db:"last_error"`
	LastErrorCode *string         `db:"last_error_code"`
	Attempts      json.RawMessage `db:"attempts"`
	NextRetryAt   time.Time       `db:"next_retry_at"`
	CreatedAt     time.Time       `db:"created_at"`
	UpdatedAt     time.Time       `db:"updated_at"`
}

// ClaimedRun is a run claimed by one executor. Every later write to the run
// must pass ClaimID, so a write from an executor that lost the claim matches
// no row.
type ClaimedRun struct {
	Run     *WorkflowRun
	Tasks   []*TaskRun
	ClaimID uuid.UUID
}

// ClaimStrategy selects the order runs are claimed in. The zero value is unset,
// so cmp.Or can fill in a default.
type ClaimStrategy int

const (
	ClaimStrategyFIFO ClaimStrategy = iota + 1
)

// Store provides typed access to the workflow_runs and workflow_tasks tables.
type Store struct {
	DB *pgkit.DB
}

// New binds the store to db's pool.
func New(db *pgkit.DB) *Store {
	return &Store{DB: db}
}

// WithTx returns a Store bound to tx.
func (t *Store) WithTx(tx pgx.Tx) *Store {
	return &Store{
		DB: &pgkit.DB{
			Conn:  t.DB.Conn,
			SQL:   t.DB.SQL,
			Query: t.DB.TxQuery(tx),
		},
	}
}

// InsertRun writes the run and its tasks. The caller owns the transaction.
func (t *Store) InsertRun(ctx context.Context, run *WorkflowRun, tasks []*TaskRun) error {
	runQ := t.DB.SQL.Insert(TableRuns).
		Columns("id", "name", "idempotency_key", "input", "call_context", "status").
		Values(run.ID, run.Name, run.IdempotencyKey, run.Input, run.CallContext, run.Status)
	if _, err := t.DB.Query.Exec(ctx, runQ); err != nil {
		return fmt.Errorf("insert workflow run: %w", err)
	}
	return t.insertTasks(ctx, run.ID, tasks)
}

// UpsertRun inserts the run and its tasks, or returns the existing run with the
// same idempotency_key untouched. The caller must bind it to a transaction.
func (t *Store) UpsertRun(ctx context.Context, run *WorkflowRun, tasks []*TaskRun) (uuid.UUID, bool, error) {
	runQ := t.DB.SQL.Insert(TableRuns).
		Columns("id", "name", "idempotency_key", "input", "call_context", "status").
		Values(run.ID, run.Name, run.IdempotencyKey, run.Input, run.CallContext, run.Status).
		Suffix(`ON CONFLICT (name, idempotency_key) DO NOTHING RETURNING id`)
	var id uuid.UUID
	err := t.DB.Query.GetOne(ctx, runQ, &id)
	if err == nil {
		if err := t.insertTasks(ctx, id, tasks); err != nil {
			return uuid.Nil, false, fmt.Errorf("upsert workflow run: tasks: %w", err)
		}
		return id, true, nil
	}
	if !pgerr.IsErrorNoRows(err) {
		return uuid.Nil, false, fmt.Errorf("upsert workflow run: %w", err)
	}
	sel := t.DB.SQL.Select("id").
		From(TableRuns).
		Where(squirrel.Eq{"name": run.Name, "idempotency_key": *run.IdempotencyKey})
	if err := t.DB.Query.GetOne(ctx, sel, &id); err != nil {
		return uuid.Nil, false, fmt.Errorf("upsert workflow run: resolve existing: %w", err)
	}
	return id, false, nil
}

func (t *Store) insertTasks(ctx context.Context, runID uuid.UUID, tasks []*TaskRun) error {
	if len(tasks) == 0 {
		return nil
	}
	q := t.DB.SQL.Insert(TableTasks).
		Columns("run_id", "seq", "handler", "status")
	for _, task := range tasks {
		q = q.Values(runID, task.Seq, task.Handler, task.Status)
	}
	if _, err := t.DB.Query.Exec(ctx, q); err != nil {
		return fmt.Errorf("insert workflow tasks: %w", err)
	}
	return nil
}

// workflowClaimRow is mapped back onto its run by id because RETURNING row
// order is unspecified.
type workflowClaimRow struct {
	ID        uuid.UUID `db:"id"`
	ClaimID   uuid.UUID `db:"claim_id"`
	ClaimedAt time.Time `db:"claimed_at"`
}

// claimableRunsQuery matches nothing when names is empty, so a process with no
// registered workflows never claims a run.
func (t *Store) claimableRunsQuery(strategy ClaimStrategy, names []string, limit int) (squirrel.SelectBuilder, error) {
	var orderBy string
	switch strategy {
	case ClaimStrategyFIFO:
		orderBy = "next_retry_at"
	default:
		return squirrel.SelectBuilder{}, fmt.Errorf("claim runs: unknown claim strategy %d", strategy)
	}
	return t.DB.SQL.Select("*").
		From(TableRuns).
		Where(squirrel.Eq{"status": WorkflowStatusRunning, "claimed_at": nil, "name": names}).
		Where(squirrel.Expr("next_retry_at <= now()")).
		OrderBy(orderBy).
		Limit(uint64(limit)).
		Suffix("FOR UPDATE SKIP LOCKED"), nil
}

// ClaimRuns claims up to limit due, unclaimed runs named in names, with their
// tasks. It opens its own transaction, so call it on the pool-bound Store.
func (t *Store) ClaimRuns(ctx context.Context, strategy ClaimStrategy, names []string, limit int) ([]*ClaimedRun, error) {
	if limit <= 0 {
		return nil, nil
	}
	// Resolved before the tx opens so an unknown strategy costs no transaction.
	selectQ, err := t.claimableRunsQuery(strategy, names, limit)
	if err != nil {
		return nil, err
	}
	var claimed []*ClaimedRun
	err = pgx.BeginFunc(ctx, t.DB.Conn, func(tx pgx.Tx) error {
		txt := t.WithTx(tx)

		var runs []*WorkflowRun
		if err := txt.DB.Query.GetAll(ctx, selectQ, &runs); err != nil {
			return fmt.Errorf("select claimable runs: %w", err)
		}
		if len(runs) == 0 {
			return nil
		}

		ids := make([]uuid.UUID, len(runs))
		for i, run := range runs {
			ids[i] = run.ID
		}
		updateQ := txt.DB.SQL.Update(TableRuns).
			Set("claimed_at", squirrel.Expr("now()")).
			Set("claim_id", squirrel.Expr("gen_random_uuid()")).
			Where(squirrel.Eq{"id": ids}).
			Suffix("RETURNING id, claim_id, claimed_at")
		var rows []workflowClaimRow
		if err := txt.DB.Query.GetAll(ctx, updateQ, &rows); err != nil {
			return fmt.Errorf("claim runs: %w", err)
		}
		byID := make(map[uuid.UUID]workflowClaimRow, len(rows))
		for _, row := range rows {
			byID[row.ID] = row
		}

		for _, run := range runs {
			row, ok := byID[run.ID]
			if !ok {
				continue
			}
			run.ClaimID = &row.ClaimID
			run.ClaimedAt = &row.ClaimedAt

			tasks, err := txt.GetTasks(ctx, run.ID)
			if err != nil {
				return fmt.Errorf("claim runs: tasks: %w", err)
			}
			claimed = append(claimed, &ClaimedRun{Run: run, Tasks: tasks, ClaimID: row.ClaimID})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// ClaimRunByID claims the run if it is due and unclaimed, and returns false
// when it is already claimed, not visible, or terminal.
func (t *Store) ClaimRunByID(ctx context.Context, id uuid.UUID) (*ClaimedRun, bool, error) {
	var claimed *ClaimedRun
	err := pgx.BeginFunc(ctx, t.DB.Conn, func(tx pgx.Tx) error {
		txt := t.WithTx(tx)

		selectQ := txt.DB.SQL.Select("*").
			From(TableRuns).
			Where(squirrel.Eq{"id": id, "status": WorkflowStatusRunning, "claimed_at": nil}).
			Where(squirrel.Expr("next_retry_at <= now()")).
			Suffix("FOR UPDATE SKIP LOCKED")
		var run WorkflowRun
		if err := txt.DB.Query.GetOne(ctx, selectQ, &run); err != nil {
			if pgerr.IsErrorNoRows(err) {
				return nil
			}
			return fmt.Errorf("select claimable run: %w", err)
		}

		updateQ := txt.DB.SQL.Update(TableRuns).
			Set("claimed_at", squirrel.Expr("now()")).
			Set("claim_id", squirrel.Expr("gen_random_uuid()")).
			Where(squirrel.Eq{"id": run.ID}).
			Suffix("RETURNING id, claim_id, claimed_at")
		var row workflowClaimRow
		if err := txt.DB.Query.GetOne(ctx, updateQ, &row); err != nil {
			return fmt.Errorf("claim run: %w", err)
		}
		run.ClaimID = &row.ClaimID
		run.ClaimedAt = &row.ClaimedAt

		tasks, err := txt.GetTasks(ctx, run.ID)
		if err != nil {
			return fmt.Errorf("claim run: tasks: %w", err)
		}
		claimed = &ClaimedRun{Run: &run, Tasks: tasks, ClaimID: row.ClaimID}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if claimed == nil {
		return nil, false, nil
	}
	return claimed, true, nil
}

// ownershipWriteBudget is longer than any healthy write takes. Waiting longer
// would only hold a semaphore slot and delay shutdown.
const ownershipWriteBudget = time.Minute

// ownershipWrite detaches from ctx so a shutdown cannot cancel a write halfway.
// A write that lands late is harmless behind the claim_id check; a write that
// is dropped makes a task that already succeeded run again.
func ownershipWrite(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), ownershipWriteBudget)
}

// MarkTaskRunning moves the current task from pending to running, returning
// false on a stale claim or a task that is not pending. It stays cancellable:
// never mark running what ctx cannot run.
func (t *Store) MarkTaskRunning(ctx context.Context, runID uuid.UUID, seq int16, claimID uuid.UUID) (bool, error) {
	q := t.DB.SQL.Update(TableTasks).
		Set("status", TaskStatusRunning).
		Set("updated_at", squirrel.Expr("now()")).
		Where(squirrel.Eq{"run_id": runID, "seq": seq, "status": TaskStatusPending}).
		Where(squirrel.Expr(
			"EXISTS (SELECT 1 FROM workflow_runs p WHERE p.id = ? AND p.claim_id = ? AND p.current_task = ?)",
			runID, claimID, seq,
		))
	res, err := t.DB.Query.Exec(ctx, q)
	if err != nil {
		return false, fmt.Errorf("mark task running: %w", err)
	}
	return res.RowsAffected() > 0, nil
}

// PersistTaskSuccess records the task's output and advances the run, returning
// false with no write when the caller has lost the claim.
func (t *Store) PersistTaskSuccess(ctx context.Context, runID uuid.UUID, seq int16, output json.RawMessage, claimID uuid.UUID) (bool, error) {
	ctx, cancel := ownershipWrite(ctx)
	defer cancel()
	var advanced bool
	err := pgx.BeginFunc(ctx, t.DB.Conn, func(tx pgx.Tx) error {
		txt := t.WithTx(tx)

		runQ := txt.DB.SQL.Update(TableRuns).
			Set("current_task", seq+1).
			Set("next_retry_at", squirrel.Expr("now()")).
			Set("updated_at", squirrel.Expr("now()")).
			Where(squirrel.Eq{"id": runID, "claim_id": claimID, "current_task": seq})
		res, err := txt.DB.Query.Exec(ctx, runQ)
		if err != nil {
			return fmt.Errorf("advance run: %w", err)
		}
		if res.RowsAffected() == 0 {
			return nil
		}
		advanced = true

		stageQ := txt.DB.SQL.Update(TableTasks).
			Set("output", output).
			Set("status", TaskStatusSucceeded).
			Set("updated_at", squirrel.Expr("now()")).
			Where(squirrel.Eq{"run_id": runID, "seq": seq})
		if _, err := txt.DB.Query.Exec(ctx, stageQ); err != nil {
			return fmt.Errorf("persist task success: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return advanced, nil
}

const maxErrMsgLen = 512

// truncateErrMsg strips invalid UTF-8 and NUL bytes: Postgres rejects both in
// text and jsonb values, even though 0x00 is valid UTF-8.
func truncateErrMsg(errMsg string) string {
	valid := strings.ToValidUTF8(errMsg[:min(len(errMsg), maxErrMsgLen)], "")
	return strings.ReplaceAll(valid, "\x00", "")
}

// TaskFailure is what one failed attempt records. Message must be PII-free.
type TaskFailure struct {
	Message string
	Code    string
}

func (f TaskFailure) sanitized() TaskFailure {
	return TaskFailure{
		Message: truncateErrMsg(f.Message),
		Code:    truncateErrMsg(f.Code),
	}
}

// codeArg makes an untagged failure read back as nil rather than "".
func (f TaskFailure) codeArg() *string {
	if f.Code == "" {
		return nil
	}
	return new(f.Code)
}

// PersistTaskFailure records a failure and backs off next_retry_at. With
// release it also clears the claim and resets the task to pending. It returns
// false with no write on a stale claim.
func (t *Store) PersistTaskFailure(ctx context.Context, runID uuid.UUID, seq int16, failure TaskFailure, backoff time.Duration, release bool, claimID uuid.UUID) (bool, error) {
	ctx, cancel := ownershipWrite(ctx)
	defer cancel()
	failure = failure.sanitized()

	var affected bool
	err := pgx.BeginFunc(ctx, t.DB.Conn, func(tx pgx.Tx) error {
		txt := t.WithTx(tx)

		runQ := txt.DB.SQL.Update(TableRuns).
			Set("next_retry_at", squirrel.Expr("now() + make_interval(secs => ?)", backoff.Seconds())).
			Set("updated_at", squirrel.Expr("now()"))
		if release {
			runQ = runQ.Set("claimed_at", nil).Set("claim_id", nil)
		}
		runQ = runQ.Where(squirrel.Eq{"id": runID, "claim_id": claimID, "current_task": seq})
		res, err := txt.DB.Query.Exec(ctx, runQ)
		if err != nil {
			return fmt.Errorf("record run failure: %w", err)
		}
		if res.RowsAffected() == 0 {
			return nil
		}
		affected = true

		stageQ := txt.DB.SQL.Update(TableTasks).
			Set("attempt_count", squirrel.Expr("attempt_count + 1")).
			Set("last_error", failure.Message).
			Set("last_error_code", failure.codeArg()).
			Set("attempts", squirrel.Expr("attempts || jsonb_build_object('n', attempt_count + 1, 'error', ?::text, 'at', now())", failure.Message)).
			Set("next_retry_at", squirrel.Expr("now() + make_interval(secs => ?)", backoff.Seconds())).
			Set("updated_at", squirrel.Expr("now()"))
		if release {
			stageQ = stageQ.Set("status", TaskStatusPending)
		}
		stageQ = stageQ.Where(squirrel.Eq{"run_id": runID, "seq": seq})
		if _, err := txt.DB.Query.Exec(ctx, stageQ); err != nil {
			return fmt.Errorf("record task failure: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return affected, nil
}

// AdvanceSkippedTask advances the run past a skipped task, returning false on a
// stale claim.
func (t *Store) AdvanceSkippedTask(ctx context.Context, runID uuid.UUID, seq int16, claimID uuid.UUID) (bool, error) {
	ctx, cancel := ownershipWrite(ctx)
	defer cancel()
	q := t.DB.SQL.Update(TableRuns).
		Set("current_task", seq+1).
		Set("next_retry_at", squirrel.Expr("now()")).
		Set("updated_at", squirrel.Expr("now()")).
		Where(squirrel.Eq{"id": runID, "claim_id": claimID, "current_task": seq})
	res, err := t.DB.Query.Exec(ctx, q)
	if err != nil {
		return false, fmt.Errorf("advance skipped task: %w", err)
	}
	return res.RowsAffected() > 0, nil
}

// Heartbeat refreshes claimed_at so SweepStuck skips a live run. On false or
// an error the caller must abort and release the claim.
func (t *Store) Heartbeat(ctx context.Context, runID uuid.UUID, claimID uuid.UUID) (bool, error) {
	q := t.DB.SQL.Update(TableRuns).
		Set("claimed_at", squirrel.Expr("now()")).
		Where(squirrel.Eq{"id": runID, "claim_id": claimID})
	res, err := t.DB.Query.Exec(ctx, q)
	if err != nil {
		return false, fmt.Errorf("heartbeat: %w", err)
	}
	return res.RowsAffected() > 0, nil
}

// CompleteRun marks the run succeeded, returning false on a stale claim.
func (t *Store) CompleteRun(ctx context.Context, runID uuid.UUID, claimID uuid.UUID) (bool, error) {
	ctx, cancel := ownershipWrite(ctx)
	defer cancel()
	q := t.DB.SQL.Update(TableRuns).
		Set("status", WorkflowStatusSucceeded).
		Set("completed_at", squirrel.Expr("now()")).
		Set("claimed_at", nil).
		Set("claim_id", nil).
		Where(squirrel.Eq{"id": runID, "claim_id": claimID})
	res, err := t.DB.Query.Exec(ctx, q)
	if err != nil {
		return false, fmt.Errorf("complete run: %w", err)
	}
	return res.RowsAffected() > 0, nil
}

// ParkRunAfterAttempt marks the run and its task stuck and records the spent
// attempt. It returns false on a stale claim.
func (t *Store) ParkRunAfterAttempt(ctx context.Context, runID uuid.UUID, seq int16, failure TaskFailure, claimID uuid.UUID) (bool, error) {
	return t.parkRun(ctx, runID, seq, failure, claimID, true)
}

// ParkRunWithoutAttempt parks the run like ParkRunAfterAttempt but records no attempt.
func (t *Store) ParkRunWithoutAttempt(ctx context.Context, runID uuid.UUID, seq int16, failure TaskFailure, claimID uuid.UUID) (bool, error) {
	return t.parkRun(ctx, runID, seq, failure, claimID, false)
}

func (t *Store) parkRun(ctx context.Context, runID uuid.UUID, seq int16, failure TaskFailure, claimID uuid.UUID, spendAttempt bool) (bool, error) {
	ctx, cancel := ownershipWrite(ctx)
	defer cancel()
	failure = failure.sanitized()

	var affected bool
	err := pgx.BeginFunc(ctx, t.DB.Conn, func(tx pgx.Tx) error {
		txt := t.WithTx(tx)

		runQ := txt.DB.SQL.Update(TableRuns).
			Set("status", WorkflowStatusStuck).
			Set("claimed_at", nil).
			Set("claim_id", nil).
			Where(squirrel.Eq{"id": runID, "claim_id": claimID, "current_task": seq})
		res, err := txt.DB.Query.Exec(ctx, runQ)
		if err != nil {
			return fmt.Errorf("park run: %w", err)
		}
		if res.RowsAffected() == 0 {
			return nil
		}
		affected = true

		stageQ := txt.DB.SQL.Update(TableTasks).
			Set("status", TaskStatusStuck).
			Set("last_error", failure.Message).
			Set("last_error_code", failure.codeArg())
		if spendAttempt {
			stageQ = stageQ.
				Set("attempt_count", squirrel.Expr("attempt_count + 1")).
				Set("attempts", squirrel.Expr("attempts || jsonb_build_object('n', attempt_count + 1, 'error', ?::text, 'at', now())", failure.Message))
		}
		stageQ = stageQ.Where(squirrel.Eq{"run_id": runID, "seq": seq})
		if _, err := txt.DB.Query.Exec(ctx, stageQ); err != nil {
			return fmt.Errorf("park task: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return affected, nil
}

// ReleaseClaim hands the run back without spending an attempt. The task reset
// is guarded on status='running' so a release at a task boundary skips it.
func (t *Store) ReleaseClaim(ctx context.Context, runID uuid.UUID, seq int16, claimID uuid.UUID) error {
	ctx, cancel := ownershipWrite(ctx)
	defer cancel()
	return pgx.BeginFunc(ctx, t.DB.Conn, func(tx pgx.Tx) error {
		txt := t.WithTx(tx)

		runQ := txt.DB.SQL.Update(TableRuns).
			Set("claimed_at", nil).
			Set("claim_id", nil).
			Where(squirrel.Eq{"id": runID, "claim_id": claimID})
		res, err := txt.DB.Query.Exec(ctx, runQ)
		if err != nil {
			return fmt.Errorf("release claim: %w", err)
		}
		if res.RowsAffected() == 0 {
			return nil
		}

		stageQ := txt.DB.SQL.Update(TableTasks).
			Set("status", TaskStatusPending).
			Where(squirrel.Eq{"run_id": runID, "seq": seq, "status": TaskStatusRunning})
		if _, err := txt.DB.Query.Exec(ctx, stageQ); err != nil {
			return fmt.Errorf("release claim: reset task: %w", err)
		}
		return nil
	})
}

// SweepStuck releases claims older than maxAge and returns how many. Only a
// task still 'running' is bumped and reset: without that join filter a skipped
// task would rerun and a pending one would take a premature attempt bump.
func (t *Store) SweepStuck(ctx context.Context, maxAge time.Duration) (int64, error) {
	var released int64
	err := pgx.BeginFunc(ctx, t.DB.Conn, func(tx pgx.Tx) error {
		txt := t.WithTx(tx)

		stale := squirrel.Expr(
			"p.status = ? AND p.claimed_at IS NOT NULL AND p.claimed_at < now() - make_interval(secs => ?)",
			WorkflowStatusRunning, maxAge.Seconds(),
		)

		stageQ := txt.DB.SQL.Update(TableTasks+" s").
			Set("attempt_count", squirrel.Expr("attempt_count + 1")).
			Set("status", TaskStatusPending).
			Set("last_error", "reclaimed stale claim").
			Set("attempts", squirrel.Expr("attempts || jsonb_build_object('n', attempt_count + 1, 'error', 'reclaimed stale claim', 'at', now())")).
			Set("updated_at", squirrel.Expr("now()")).
			From(TableRuns + " p").
			Where(squirrel.Expr("s.run_id = p.id AND s.seq = p.current_task")).
			Where(squirrel.Eq{"s.status": TaskStatusRunning}).
			Where(stale)
		if _, err := txt.DB.Query.Exec(ctx, stageQ); err != nil {
			return fmt.Errorf("sweep stuck tasks: %w", err)
		}

		runQ := txt.DB.SQL.Update(TableRuns+" p").
			Set("claimed_at", nil).
			Set("claim_id", nil).
			Set("next_retry_at", squirrel.Expr("now()")).
			Where(stale)
		res, err := txt.DB.Query.Exec(ctx, runQ)
		if err != nil {
			return fmt.Errorf("sweep stuck runs: %w", err)
		}
		released = res.RowsAffected()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return released, nil
}

// GetRun reads a run by id.
func (t *Store) GetRun(ctx context.Context, id uuid.UUID) (*WorkflowRun, error) {
	q := t.DB.SQL.Select("*").From(TableRuns).Where(squirrel.Eq{"id": id})
	var run WorkflowRun
	if err := t.DB.Query.GetOne(ctx, q, &run); err != nil {
		return nil, fmt.Errorf("get workflow run: %w", err)
	}
	return &run, nil
}

// GetTasks reads a run's tasks ordered by seq.
func (t *Store) GetTasks(ctx context.Context, runID uuid.UUID) ([]*TaskRun, error) {
	q := t.DB.SQL.Select("*").From(TableTasks).Where(squirrel.Eq{"run_id": runID}).OrderBy("seq")
	var tasks []*TaskRun
	if err := t.DB.Query.GetAll(ctx, q, &tasks); err != nil {
		return nil, fmt.Errorf("get workflow tasks: %w", err)
	}
	return tasks, nil
}

// OldestPending returns the oldest unclaimed run's next_retry_at, or nil when
// the backlog is empty.
func (t *Store) OldestPending(ctx context.Context) (*time.Time, error) {
	q := t.DB.SQL.Select("MIN(next_retry_at)").
		From(TableRuns).
		Where(squirrel.Eq{"status": WorkflowStatusRunning, "claimed_at": nil})
	var oldest *time.Time
	if err := t.DB.Query.GetOne(ctx, q, &oldest); err != nil {
		return nil, fmt.Errorf("oldest pending workflow run: %w", err)
	}
	return oldest, nil
}
