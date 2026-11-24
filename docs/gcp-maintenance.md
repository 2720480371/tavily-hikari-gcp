# GCP maintenance fork

This fork tracks upstream `IvanLi-CN/tavily-hikari` and retains the deployment's dashboard recovery
changes on top of `v0.86.6` (`aad83d01d9843102e699dcf4e437a6145e7722ca`).

## Retained recovery tuning

- Progressing quota recovery batches yield for 20 milliseconds; actual contention keeps the
  one-second backoff. The existing eight-query and 250-millisecond batch limits remain in place.
- Startup dashboard prewarming has a 45-second budget.
- Deferred dashboard integrity work waits at least 15 seconds. Foreground, connection-pool, and
  recent-contention admission defers wait 30 seconds.

## Completed checkpoint recovery after restart

Dashboard integrity checkpoints persist a durable source revision and a combined source version.
The combined version includes a process-local mutation counter. That counter starts empty after a
restart, so a completed checkpoint from the previous process can otherwise fail daily sealing even
when its source has not changed.

During store startup, discard that old process-local contribution only for completed checkpoints
whose durable revision still matches the source and whose visible source-row fence has not advanced.
Keep the stored counts, completion status, durable revision, and fence. Real source mutations and
late inserts continue to invalidate completed work through the existing recovery paths. Pending
work keeps its existing startup reset behavior. Exclusive offline statistics recovery also
normalizes completed checkpoints, without resetting its resumable pending work. The shared
request-log GC opener does not normalize checkpoints while the service may still be running.

The restart regression tests live in
`src/tests/dashboard_rollup_integrity/restart_recovery.rs` and use disposable local SQLite fixtures.
The existing backend runner covers them in the `lib-request-rollup-integrity` shard through its
unchanged parent-module prefix. Quota-recovery and scheduler checks also cover the retained
deployment patches. Follow `docs/agents/testing.md` when choosing an execution target.

## Delivery boundary

Source changes and pull-request merges do not deploy the service. Keep runtime credentials,
databases, and environment files out of this repository. Source-only maintenance pull requests use
`type:skip` with `channel:stable` so merging does not start an automatic package release. A later
deployment must build and validate a fixed candidate against the running deployment's data and
configuration requirements.
