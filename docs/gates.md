# Gate Policy

## Gate types

- `release`
- `nightly`
- `post-release`

Use [evaluate-gate.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/evaluate-gate.sh)
to turn a summary into a blocking or non-blocking decision:

```bash
./scripts/evaluate-gate.sh release release-summary.json
./scripts/evaluate-gate.sh nightly nightly-matrix.json
./scripts/evaluate-gate.sh post-release post-release-inspection.json
```

## Blocking rules

Block on:
- any required component with non-zero status
- any top-level summary with non-zero status and no finer-grained component structure

Do not block on:
- advisory-only components such as `cluster-flaky`

## Expected interpretation

### Release gate

Block when:
- `verification`
- `performance`
- `cluster`
- `compatibility`
- `storage-upgrade`
- `rollback-drill`

regresses or fails.

### Nightly gate

Block when:
- `cluster-nightly`
- `compatibility-release`
- `storage-upgrade`
- `rollback-drill`

fails.

Warn only when:
- `cluster-flaky` fails

### Post-release gate

Block when:
- shard audit fetch fails
- shard metrics snapshot fails
- daily cluster residual check fails

## Operator rule of thumb

- `decision=pass` means proceed
- `decision=block` means stop and investigate
- advisory failures should still be communicated, but they do not stop the stable baseline
