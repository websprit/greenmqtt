# Final Audit

## Audit items

- `TODO.md` active plan has no unchecked items
- git worktree cleanliness is reported
- key operational entry points exist on disk

## Failure handling

### TODO still has unchecked items

- do not mark the delivery as frozen
- finish the remaining TODO items or explicitly regenerate the plan

### Worktree is dirty

- treat modified tracked files as pending delivery work
- treat unexpected untracked files as packaging noise until classified

### Missing key entry points

- stop the freeze process
- restore or regenerate the missing script/doc before acceptance

## Machine-readable report

Use:

```bash
./scripts/run-final-audit.sh
```

The script always emits JSON.  
`status=0` means the audit passes.  
`status=1` means one or more audit checks failed.
