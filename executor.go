package workflow

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/goware/workflow/store"
)

// executePlan returns StatusSucceeded or StatusStuck only once the write that
// ends the run was accepted under this executor's claim. Every other exit
// returns StatusRunning: the run continues in another process or on a later
// tick.
func (m *Manager) executePlan(ctx context.Context, claimed *store.ClaimedRun) Status {
	claimID := claimed.ClaimID
	runID := claimed.Run.ID
	workflowName := claimed.Run.Name
	tasks := claimed.Tasks
	current := claimed.Run.CurrentTask
	// marked remembers the task this executor already set to running. An
	// in-process retry re-enters with the task still running, and
	// MarkTaskRunning would return false because it is no longer pending.
	marked := int16(-1)
	runLog := m.log.With(slog.String("run_id", runID.String()), slog.String("run", workflowName))

	input, handlerCtx, err := m.openRun(ctx, claimed.Run)
	if err != nil {
		// No task has run, so this costs no attempt budget.
		msg := "workflow: run data could not be opened"
		parked, perr := m.db.ParkRunWithoutAttempt(ctx, runID, current, store.TaskFailure{Message: msg}, claimID)
		if perr != nil {
			return m.abandon(ctx, runLog, "workflow: park on unopenable run data failed", perr, runID, current, claimID)
		}
		if !parked {
			return StatusRunning
		}
		runLog.Log(ctx, LevelAlert, "workflow: parked on unopenable run data", slog.Any("error", err))
		m.signal(runID, StatusStuck)
		return StatusStuck
	}
	for {
		if int(current) >= len(tasks) {
			// Checked before the drain check so a shutdown that arrives after
			// the last task finished still writes the run's success.
			ok, err := m.db.CompleteRun(ctx, runID, claimID)
			if err != nil {
				return m.abandon(ctx, runLog, "workflow: complete run failed", err, runID, current, claimID)
			}
			if !ok {
				return StatusRunning
			}
			m.signal(runID, StatusSucceeded)
			return StatusSucceeded
		}

		if ctx.Err() != nil || m.Draining() {
			m.release(ctx, runID, current, claimID)
			return StatusRunning
		}

		task := tasks[current]
		seq := task.Seq
		taskLog := runLog.With(slog.Int("seq", int(seq)), slog.String("handler", task.Handler))

		if task.Status == store.TaskStatusSkipped {
			ok, err := m.db.AdvanceSkippedTask(ctx, runID, seq, claimID)
			if err != nil {
				return m.abandon(ctx, taskLog, "workflow: advance skipped task failed", err, runID, seq, claimID)
			}
			if !ok {
				return StatusRunning
			}
			current++
			if !m.heartbeat(ctx, runID, current, claimID) {
				return StatusRunning
			}
			continue
		}

		handler, sc, resolved := m.reg.resolve(workflowName, task.Handler)
		if !resolved {
			msg := fmt.Sprintf("workflow: version skew — no handler %q registered for run %q", task.Handler, workflowName)
			ok, err := m.db.ParkRunWithoutAttempt(ctx, runID, seq, store.TaskFailure{Message: msg}, claimID)
			if err != nil {
				return m.abandon(ctx, taskLog, "workflow: park on version skew failed", err, runID, seq, claimID)
			}
			if !ok {
				return StatusRunning
			}
			taskLog.Log(ctx, LevelAlert, "workflow: parked on version skew", slog.Any("error", errors.New(msg)))
			m.signal(runID, StatusStuck)
			return StatusStuck
		}

		// The post-failure check only runs when a handler returns. A process
		// that died mid-attempt never gets there, and SweepStuck bumps
		// attempt_count on reclaim, so without this the run retries forever.
		budget := cmp.Or(sc.maxAttempts, m.cfg.DefaultMaxAttempts)
		if task.AttemptCount >= budget {
			msg := fmt.Sprintf("workflow: attempt budget exhausted after %d attempts", task.AttemptCount)
			ok, err := m.db.ParkRunWithoutAttempt(ctx, runID, seq, store.TaskFailure{Message: msg}, claimID)
			if err != nil {
				return m.abandon(ctx, taskLog, "workflow: park on exhausted budget failed", err, runID, seq, claimID)
			}
			if !ok {
				return StatusRunning
			}
			taskLog.Log(ctx, LevelAlert, "workflow: parked on exhausted budget",
				slog.Int("attempt_count", task.AttemptCount), slog.Int("budget", budget),
				slog.Any("error", errors.New(msg)))
			m.signal(runID, StatusStuck)
			return StatusStuck
		}

		if marked != seq {
			ok, err := m.db.MarkTaskRunning(ctx, runID, seq, claimID)
			if err != nil {
				return m.abandon(ctx, taskLog, "workflow: mark task running failed", err, runID, seq, claimID)
			}
			if !ok {
				// Another executor owns this task. That is legitimate, but
				// without a log line it looks the same as a broken claim check.
				taskLog.DebugContext(ctx, "workflow: task already claimed elsewhere")
				return StatusRunning
			}
			marked = seq
		}

		core := taskRunCore{
			log:     taskLog.With(slog.Int("attempt", task.AttemptCount+1)),
			runID:   runID,
			seq:     seq,
			input:   input,
			outputs: priorOutputs(tasks),
			attempt: task.AttemptCount + 1,
			budget:  budget,
		}
		stageCtx, cancel := context.WithTimeout(handlerCtx, cmp.Or(sc.timeout, m.cfg.DefaultTaskTimeout))
		raw, err := handler(stageCtx, core)
		cancel()

		var output json.RawMessage
		if err == nil {
			output, err = json.Marshal(raw)
			if err != nil {
				err = Permanent(fmt.Errorf("marshal task output: %w", err))
			}
		}

		// A drain does not count as an attempt: persist finished work, then
		// release for immediate re-claim. The data layer detaches both writes
		// from ctx, which may be the shutdown signal itself.
		if ctx.Err() != nil || m.Draining() {
			if err == nil {
				_, perr := m.db.PersistTaskSuccess(ctx, runID, seq, output, claimID)
				if perr != nil {
					return m.hold(ctx, taskLog, "workflow: persist task success on drain failed", perr)
				}
			}
			m.release(ctx, runID, seq, claimID)
			return StatusRunning
		}

		if err == nil {
			ok, perr := m.db.PersistTaskSuccess(ctx, runID, seq, output, claimID)
			if perr != nil {
				return m.hold(ctx, taskLog, "workflow: persist task success failed", perr)
			}
			if !ok {
				return StatusRunning
			}
			task.Status = store.TaskStatusSucceeded
			task.Output = output
			current++
			if !m.heartbeat(ctx, runID, current, claimID) {
				return StatusRunning
			}
			continue
		}

		failure := store.TaskFailure{Message: err.Error(), Code: codeOf(err)}
		attempt := task.AttemptCount + 1
		backoff := nextBackoff(sc, m.cfg, attempt)
		_, permanent := errors.AsType[*PermanentError](err)

		if permanent || attempt >= budget {
			parked, perr := m.db.ParkRunAfterAttempt(ctx, runID, seq, failure, claimID)
			if perr != nil {
				return m.hold(ctx, taskLog, "workflow: park run failed", perr)
			}
			if !parked {
				return StatusRunning
			}
			taskLog.Log(ctx, LevelAlert, "workflow: parked task",
				slog.Int("attempt", attempt), slog.Bool("permanent", permanent),
				slog.Any("error", err))
			m.signal(runID, StatusStuck)
			return StatusStuck
		}

		if backoff <= m.cfg.MaxInProcessBackoff {
			persisted, perr := m.db.PersistTaskFailure(ctx, runID, seq, failure, backoff, false, claimID)
			if perr != nil {
				return m.hold(ctx, taskLog, "workflow: persist task failure failed", perr)
			}
			if !persisted {
				return StatusRunning
			}
			// Mirror the persisted attempt_count bump so the next backoff matches
			// what a resumed executor would compute.
			task.AttemptCount++
			if !sleepCtx(ctx, backoff) {
				continue
			}
			if !m.heartbeat(ctx, runID, seq, claimID) {
				return StatusRunning
			}
			continue
		}

		if _, perr := m.db.PersistTaskFailure(ctx, runID, seq, failure, backoff, true, claimID); perr != nil {
			return m.hold(ctx, taskLog, "workflow: persist task failure (release) failed", perr)
		}
		return StatusRunning
	}
}

// abandon releases the claim when a write failed before the handler ran. No
// attempt was spent, so the next poll tick can re-claim the run instead of
// waiting for the sweeper.
func (m *Manager) abandon(ctx context.Context, logger *slog.Logger, msg string, err error, runID uuid.UUID, seq int16, claimID uuid.UUID) Status {
	logger.ErrorContext(ctx, msg, slog.Any("error", err))
	m.release(ctx, runID, seq, claimID)
	return StatusRunning
}

// hold keeps the claim when a write failed after the handler ran. The attempt
// was spent but never recorded, so the run must wait for SweepStuck, which
// bumps attempt_count on reclaim, before anything re-runs the task.
func (m *Manager) hold(ctx context.Context, logger *slog.Logger, msg string, err error) Status {
	logger.ErrorContext(ctx, msg, slog.Any("error", err))
	return StatusRunning
}

// heartbeat releases the claim when the refresh fails, so the next tick picks
// the run up instead of it waiting out SweepMaxAge and taking a SweepStuck
// attempt bump.
func (m *Manager) heartbeat(ctx context.Context, runID uuid.UUID, seq int16, claimID uuid.UUID) bool {
	ok, err := m.db.Heartbeat(ctx, runID, claimID)
	if err != nil {
		m.log.ErrorContext(ctx, "workflow: heartbeat failed",
			slog.String("run_id", runID.String()), slog.Any("error", err))
	}
	if ok {
		return true
	}
	m.release(ctx, runID, seq, claimID)
	return false
}

func (m *Manager) release(ctx context.Context, runID uuid.UUID, seq int16, claimID uuid.UUID) {
	if err := m.db.ReleaseClaim(ctx, runID, seq, claimID); err != nil {
		m.log.ErrorContext(ctx, "workflow: release claim failed",
			slog.String("run_id", runID.String()), slog.Int("seq", int(seq)), slog.Any("error", err))
	}
}

func priorOutputs(tasks []*store.TaskRun) map[string]json.RawMessage {
	outputs := make(map[string]json.RawMessage, len(tasks))
	for _, s := range tasks {
		if s.Status == store.TaskStatusSucceeded && s.Output != nil {
			outputs[s.Handler] = s.Output
		}
	}
	return outputs
}

func nextBackoff(sc taskConfig, cfg Config, attempt int) time.Duration {
	if len(sc.backoffSchedule) > 0 {
		return scheduleBackoff(sc.backoffSchedule, attempt)
	}
	return computeBackoff(
		cmp.Or(sc.backoffBase, cfg.BackoffBase),
		cmp.Or(sc.backoffMax, cfg.BackoffMax),
		attempt,
	)
}

// scheduleBackoff takes attempt from the persisted count, so a resumed
// executor picks the same delay. Register rejects an empty schedule.
func scheduleBackoff(schedule []time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > len(schedule) {
		attempt = len(schedule)
	}
	return schedule[attempt-1]
}

// computeBackoff returns min(base*2^(attempt-1), max). It doubles iteratively
// so an overflow clamps to max.
func computeBackoff(base, max time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := base
	for range attempt - 1 {
		backoff *= 2
		if backoff <= 0 || backoff >= max {
			return max
		}
	}
	if backoff > max {
		return max
	}
	return backoff
}

// sleepCtx returns false when ctx is done before d elapses.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
