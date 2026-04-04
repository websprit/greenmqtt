# CI Artifact Index

## Artifact names

- `workspace-smoke-artifact`
  - `workspace-smoke.json`
- `release-summary-artifacts`
  - `release-summary.json`
  - `rollback-drill.json`
- `post-release-inspection-artifact`
  - `post-release-inspection.json`

For local CI-like execution, [run-ci-artifact-suite.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-ci-artifact-suite.sh)
produces:

- `release-summary.json`
- `rollback-drill.json`
- `post-release-inspection.json`
- `post-release-metrics.prom`

## Summary fields

Common fields:
- `profile`
- `status`
- `results`

Additional fields by summary:

### `release-summary.json`

- `mode`
- `generated_at`
- `components[]`

Each component includes:
- `name`
- `status`
- `duration_seconds`
- `profile`
- `summary`

### `nightly-matrix.json`

- `generated_at`
- `components[]`

Each component includes:
- `name`
- `status`
- `required`
- `duration_seconds`
- `profile`
- `summary`

### `post-release-inspection.json`

- `mode`
- `run_mode`
- `results[]`

### `rollback-drill.json`

- `results[]` for:
  - shard move rollback
  - cluster failover rollback
  - config rollback

## Local artifact viewing

List and summarize a directory of JSON artifacts:

```bash
./scripts/view-ci-artifacts.sh /tmp/greenmqtt-ci-artifacts
```

Compare two historical summaries:

```bash
./scripts/run-summary-diff.sh old.json new.json
```

## Rollback judgment

Treat as rollback candidates when:
- any required component status regresses from `0` to non-zero
- `release-summary.status != 0`
- `post-release-inspection.status != 0`
- `rollback-drill.status != 0`

Treat as advisory-only when:
- `nightly-matrix` reports a non-zero advisory component such as `cluster-flaky`
- the required nightly components remain green
