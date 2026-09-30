# workflow

Durable, ordered task runs for Go, stored in PostgreSQL. PostgreSQL only: the
engine claims work with `SELECT ... FOR UPDATE SKIP LOCKED`, and the `store`
package is the only code that talks to the database. A run is a named workflow
plus a snapshot of its tasks, stored in two tables the module owns. The process
that starts a run tries to finish it right there, in the same process. If that
process dies, is saturated, or is draining for a deploy, a poller in any other
process claims the same row and carries on from the last recorded task. Task
execution is therefore at-least-once, and a handler that cannot safely run
twice is a bug in the handler, not in the engine.

A typical consumer has two binaries. The one serving requests registers its
workflows, builds a `Manager` over a `*pgkit.DB` and starts runs. The worker
registers the same workflows, builds its own `Manager`, and runs the poller and
the sweeper through `NewRunnable`. Both talk to the same tables.

## Install

```
go get github.com/goware/workflow
```

Import `github.com/goware/workflow` for the engine and
`github.com/goware/workflow/store` for the migration entry point. The engine
takes a `*pgkit.DB` from [pgkit](https://github.com/goware/pgkit) on the
database that holds its tables. It needs Go 1.26 or later.

## Schema and migrations

The store owns its two tables, `workflow_runs` and `workflow_tasks`, and the
goose chain that creates them. The chain is embedded in the module under
`store/migrations/` and tracks its versions in its own table,
`workflows_goose_db_version`, so it never collides with the consumer's own
numbering.

The consumer runs the chain by calling `store.Migrate(ctx, sqlDB)` from its own
migration command, after its own chain, with a `database/sql` handle on the
same database. The call is idempotent. A change to these tables is a new file
in `store/migrations/`, never a migration in the consumer's chain.

`NewManager` reads `workflows_goose_db_version` on start and refuses to run
when the database is behind `store.SchemaVersion`, the version this build of
the code expects. A missing version table counts as version 0. The error wraps
`store.ErrSchemaOutdated`, so the consumer can match it and name its own
migration command in the message:

```go
mgr, err := workflow.NewManager(ctx, db, reg, cfg, logger)
if errors.Is(err, store.ErrSchemaOutdated) {
	return fmt.Errorf("%w (run migrate up)", err)
}
```

Because consumers upgrade on their own schedules, a schema change ships as
expand then contract. A release only adds: nullable
columns, indexes, appended enum values. A drop lands one release later, once
every consumer runs a version that stopped reading the column.

## Quick start

### Define a workflow

Declare the workflow and each task at package scope. `In` is the input every
task receives. `Out` is what that task records for the tasks after it. Use
`None` when a task records nothing.

```go
var Transfer = workflow.NewWorkflow[TransferInput]("Transfer.Send")

var (
    reserve = workflow.NewTask[reserveOutput]("transfer.reserve")
    send    = workflow.NewTask[workflow.None]("transfer.send")
)

func Register(reg *workflow.Registry, deps Handlers) error {
    return reg.Register(workflow.Define(Transfer,
        workflow.Handle(reserve, deps.reserve, workflow.MaxAttempts(8)),
        workflow.Handle(send, deps.send,
            workflow.Timeout(30*time.Second)).SkipIf(alreadySent),
    ))
}
```

`Register` is where validation happens. It rejects a frozen registry, a
duplicate workflow name, a workflow with zero tasks, an empty or duplicate task
name inside one workflow, more than `math.MaxInt16` tasks, a bad backoff
config, and a task name that is already registered with a different output
type. That last rule exists because `Output` looks a task up by name alone: two
workflows may share a task name only if they agree on `Out`.

`NewManager` freezes the registry, so register every workflow before building
it.

### Start a run

```go
run, err := Transfer.Start(ctx, mgr, input, workflow.WithIdempotencyKey(projectID+":"+key))
status, err := run.Await(ctx)
```

`Start` inserts the run in its own transaction and then tries the same-process
fast path. When the local run semaphore is full it leaves the row for the
poller instead.

`StartTx` inserts inside the caller's transaction and does not launch
anything. The run becomes visible when that transaction commits, and the first
`Await`, or the poller, claims it. `ClaimRunByID` uses `SKIP LOCKED`, so a
double kick claims at most once.

`WithIdempotencyKey` collapses a duplicate enqueue onto the existing run and
leaves that run untouched, so a parked run stays parked. Without the option
every call inserts a fresh run. After a duplicate start, read the run's own
input with `WorkflowDef.Input` rather than answering from the input this call
passed in, because the two may differ.

`Await` blocks until the run is terminal. If the caller's context ends first it
returns `StatusRunning` and the run keeps going. The in-memory completion
channel is a latency win for a run in this process. Correctness comes from the
one-second `GetRun` poll, which also covers a run finished by another process.

`Status` is a plain database read. `Failure` is nil unless the run is stuck.
When it is set it names the parked task, the last error text, the code the
handler set with `WithCode`, and the attempt count. Persist PII-free error
text, because callers surface `Failure.Error`.

## Handler contract

```go
func (h Handlers) send(ctx context.Context, run *workflow.TaskRun[TransferInput]) (workflow.None, error) {
    prior, err := workflow.Output(run, reserve)
    if err != nil {
        return workflow.None{}, err
    }
    if err := h.provider.Send(ctx, prior.ID, run.IdempotencyKey()); err != nil {
        if isRejected(err) {
            return workflow.None{}, workflow.Permanent(workflow.WithCode("rejected", err))
        }
        return workflow.None{}, err
    }
    return workflow.None{}, nil
}
```

A handler must respect `ctx` cancellation and must be idempotent, because the
engine re-invokes a task after a retry, a crash, or a claim reclaim. The
`TaskRun` it receives carries what it needs:

- `Input` is the workflow input, already decoded. A decode failure is
  permanent: the stored input never changes, so a retry could not fix it.
- `Output` decodes an earlier task's recorded output. It errors when that task
  recorded none, which includes a skipped task.
- `IdempotencyKey` is `runID:seq`, stable across retries and crash resume. Hand
  it to an external provider so a re-run does not repeat the side effect.
  Provider dedup windows are finite, and a run parked for weeks can still
  re-emit.
- `LastAttempt` is true when this failure spends the attempt budget. Use it for
  a give-up side effect. A permanent failure parks earlier regardless.

Failures are plain errors with two optional wrappers:

- `Permanent` parks the run now. A plain error retries until the attempt budget
  is spent, then parks.
- `WithCode` tags a failure so `Failure.Code` can map it to a public error. The
  engine stores the code and never interprets it. `Permanent` and `WithCode`
  compose in either order, and `WithCode` on a nil error returns nil.

`NewTaskRun` builds a `TaskRun` for a handler unit test with no manager and no
database. It is a single-attempt run, so `LastAttempt` is true.

`SkipIf` is evaluated once, when the run is instantiated, against the plaintext
input. A true predicate snapshots that task as skipped. The predicate takes the
workflow input type, so a drifted predicate is a compile error rather than a
runtime surprise.

## Retries

A task with no options falls back to `Config`, and `NewManager` fills a zero
`Config` with these defaults:

| Field | Default | Meaning |
|---|---|---|
| `DefaultTaskTimeout` | 60s | One attempt, unless the task sets `Timeout`. |
| `DefaultMaxAttempts` | 8 | Attempts before the task parks, unless it sets `MaxAttempts`. |
| `BackoffBase` / `BackoffMax` | 1s / 5m | Exponential delay `min(base*2^(attempt-1), max)`. |
| `MaxInProcessBackoff` | 5s | Shorter delays sleep in this process. Longer ones release the claim for the poller. |
| `SweepMaxAge` | 10m | A claim older than this is stale. |
| `MaxLocalRuns` | 64 | Fast-path concurrency. A full semaphore leaves the run for the poller. |

`Backoff` and `BackoffSchedule` are mutually exclusive. A schedule uses
`schedule[attempt-1]`, and attempts past the end reuse the last entry.
`Register` rejects an empty schedule, a negative entry, or both backoff modes
on one task. The delay is computed from the persisted attempt count, so an
executor that resumes after a crash waits the same amount the original would
have.

`NewManager` refuses to start unless the widest task timeout plus
`MaxInProcessBackoff` is strictly under `SweepMaxAge`. Otherwise the sweeper
could reclaim a run whose executor is still inside one legitimate attempt.

## How a run moves

Each binary that executes work builds a `Manager`. The one that should also
drain orphaned runs calls `NewRunnable`, which runs the poller and the sweeper:

```go
mgr.NewRunnable(workflow.RunnerConfig{
    PollInterval:  time.Second,
    BatchSize:     100,
    Concurrency:   64,
    SweepEnabled:  true,
    SweepInterval: 30 * time.Second,
})
```

A zero `RunnerConfig` defaults to a 30s poll, batch 10, concurrency 4, a 1m
sweep interval, and FIFO claims. `SweepInterval` is how often the sweeper
ticks. `Config.SweepMaxAge` is how old a claim has to be before the sweeper
treats it as abandoned. Sweep is a guarded update, so it is safe to run from
more than one replica.

A claim is exclusive, and every later write carries the claim id as a fence. A
write that matches zero rows means another executor now owns the run, and this
one stops without touching the run's state.

On a retryable failure inside `MaxInProcessBackoff`, the executor records the
failure, sleeps, heartbeats, and tries again. A longer backoff releases the
claim and leaves `next_retry_at` for the poller. Shutdown during an attempt
does not spend budget. If the handler had already succeeded, that output is
persisted first and the claim is released for immediate re-claim.

A lost heartbeat releases the claim instead of waiting out `SweepMaxAge`.
`SweepStuck` bumps `attempt_count` on reclaim, because the dead process never
came back to do it. The executor parks a task whose persisted count has already
spent the budget, so a crash loop cannot re-invoke it forever.

The runnable sets draining as soon as its context ends, then waits for
in-flight fast-path and poller executions. New fast-path launches stop, and a
deploy does not abandon a mid-flight run. `StatusCancelled` is reserved. No
path produces it yet.

## Calling context and sealing

`Config.Context` carries the caller's context across processes. Implement
`Propagator[T]` for your own snapshot type and wrap it with `Propagate`. The
engine encodes the snapshot as JSON, stores it on the run at start, and
decodes it again around each handler, never around the executor's own writes.
A snapshot that no longer decodes parks the run without spending an attempt.
With a nil propagator the handler sees no calling context on the fast path,
and none at all on a poller resume.

```go
type caller struct {
    Actor string `json:"actor"`
}

type callerPropagator struct{}

func (callerPropagator) Capture(ctx context.Context) (caller, error) {
    return caller{Actor: actorFrom(ctx)}, nil
}

func (callerPropagator) Restore(ctx context.Context, c caller) (context.Context, error) {
    return withActor(ctx, c.Actor), nil
}

cfg := workflow.Config{Context: workflow.Propagate(callerPropagator{})}
```

`Config.Cipher` seals the input and the call-context snapshot at rest. A nil
cipher stores plain JSON. A document written before a cipher was configured
still opens. A sealed document with no cipher configured parks the run without
spending an attempt.

A propagator that captures actor or request metadata is carrying PII. Do not
configure one without a cipher.

## Alerts and logging

The engine logs through the `*slog.Logger` passed to `NewManager`. Every
message starts with `workflow: `, and a failed write carries its error in an
`error` attribute.

Four events mean a run is parked and will not move again without a person:
input or call context that no longer opens, a task with no registered handler
(version skew), a task whose attempt budget was already spent, and a task that
failed permanently or on its last attempt. These log at `workflow.LevelAlert`,
which is `slog.Level(16)`, above `slog.LevelError`. slog prints an unnamed
level relative to the nearest named one, so the default handlers show it as
`ERROR+8`.

To page on these events, match the level in your handler. This example names
the level `ALERT` in the output:

```go
opts := &slog.HandlerOptions{
    ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
        if a.Key == slog.LevelKey && a.Value.Any().(slog.Level) >= workflow.LevelAlert {
            a.Value = slog.StringValue("ALERT")
        }
        return a
    },
}
logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
```

## Testing

`make test` runs the whole suite. The registry, option and error tests need
nothing. Tests that touch the store need a PostgreSQL server and read its URL
from `WORKFLOW_TEST_DATABASE_URL`, which the Makefile sets from `PG_URL`
(default `postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable`).

```
make test PG_URL='postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable'
```

Each such test creates its own scratch database on that server, migrates it,
and drops it on cleanup, so tests in different packages never see each other's
rows and can run in parallel. When the variable is unset those tests skip, so
a plain `go test ./...` passes without PostgreSQL. With `CI` set and the
variable missing they fail instead, so a misconfigured job cannot pass by
skipping. CI runs the same suite against a PostgreSQL service container.

## License

MIT. See [LICENSE](LICENSE).
