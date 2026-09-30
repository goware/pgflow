package workflow

import (
	"encoding/json"
	"time"

	"github.com/goware/workflow/store"
)

// taskConfig is one task's resolved tuning; zero fields fall back to Config's defaults.
type taskConfig struct {
	maxAttempts     int
	backoffBase     time.Duration
	backoffMax      time.Duration
	backoffSchedule []time.Duration
	timeout         time.Duration
	skip            func(json.RawMessage) (bool, error)
}

// TaskOption configures one task's retry, timeout, or skip behavior.
type TaskOption func(*taskConfig)

// MaxAttempts caps the number of attempts before a task parks.
func MaxAttempts(n int) TaskOption {
	return func(cfg *taskConfig) { cfg.maxAttempts = n }
}

// Backoff sets exponential backoff min(base*2^(attempt-1), max). Mutually
// exclusive with BackoffSchedule.
func Backoff(base, max time.Duration) TaskOption {
	return func(cfg *taskConfig) {
		cfg.backoffBase = base
		cfg.backoffMax = max
	}
}

// BackoffSchedule sets the delay after each failed attempt, reusing the last
// entry past the end. Mutually exclusive with Backoff; MaxAttempts still caps.
func BackoffSchedule(schedule []time.Duration) TaskOption {
	return func(cfg *taskConfig) {
		// Copied because the caller's backing array is mutable. A non-nil empty
		// slice keeps BackoffSchedule(nil) distinguishable from never calling it.
		cfg.backoffSchedule = make([]time.Duration, len(schedule))
		copy(cfg.backoffSchedule, schedule)
	}
}

// Timeout bounds a single task attempt.
func Timeout(d time.Duration) TaskOption {
	return func(cfg *taskConfig) { cfg.timeout = d }
}

// SkipIf marks the task skipped when pred holds for the run's input, evaluated
// once when the run is built.
func (s TaskSpec[In]) SkipIf(pred func(In) bool) TaskSpec[In] {
	s.core.cfg.skip = func(raw json.RawMessage) (bool, error) {
		var in In
		if err := json.Unmarshal(raw, &in); err != nil {
			return false, err
		}
		return pred(in), nil
	}
	return s
}

// Config holds executor defaults; NewManager fills zero fields.
type Config struct {
	DefaultTaskTimeout  time.Duration // 60s
	DefaultMaxAttempts  int           // 8
	BackoffBase         time.Duration // 1s
	BackoffMax          time.Duration // 5m
	MaxInProcessBackoff time.Duration // 5s
	SweepMaxAge         time.Duration // 10m
	MaxLocalRuns        int           // 64

	Context Propagator[json.RawMessage] // built with Propagate; nil leaves a task with no calling context off the fast path
	Cipher  Cipher                      // nil stores input and call context as plain JSON
}

// RunnerConfig configures the poller and sweeper.
type RunnerConfig struct {
	PollInterval  time.Duration
	BatchSize     int
	Concurrency   int
	SweepEnabled  bool
	SweepInterval time.Duration
	ClaimStrategy store.ClaimStrategy
}

func (r *Registry) maxTaskTimeout(cfg Config) time.Duration {
	max := cfg.DefaultTaskTimeout
	for _, run := range r.workflows {
		for _, task := range run.tasks {
			timeout := task.cfg.timeout
			if timeout == 0 {
				timeout = cfg.DefaultTaskTimeout
			}
			if timeout > max {
				max = timeout
			}
		}
	}
	return max
}
