# Spec Kit Feature Workspace

This brownfield effort intentionally keeps several proposed feature directories on one review branch.
GitHub Spec Kit v1.0.4 decouples a feature directory from the Git branch, but its local
`.specify/feature.json` pointer is ignored and can select the wrong feature after another command runs.

## Activate One Slice Explicitly

Before every per-feature Spec Kit command, set the intended directory explicitly in the command environment:

```bash
SPECIFY_FEATURE_DIRECTORY=specs/002-reproducible-runtime <speckit command>
```

Replace the value for the active slice. Never rely on the ignored pointer when reviewing or executing a
different feature, and never run two feature-writing commands concurrently in the same checkout.

Proposed execution order:

1. `002-reproducible-runtime`
2. `001-supported-trade-ingestion`
3. `003-safe-observable-operation`
4. `004-truthful-detection-contract`

Numeric prefixes record creation order, not dependency order.

## Checklist Ownership

- `checklists/requirements.md` is the built-in author-owned specification-quality checklist created by
  `$speckit-specify` and maintained by `$speckit-clarify`.
- Other `checklists/<domain>.md` files are reviewer-owned requirements-quality checklists generated later by
  `$speckit-checklist` from the approved review focus.
- Agents leave new reviewer-owned checklist items unchecked. Patrick or a human reviewer he designates owns
  the `[x]` decision. Checked items mean the written requirements passed review, not that code is complete.

## Required Per-Slice Order

Use individual skills in this order:

1. specify
2. clarify
3. plan
4. reviewer-owned domain checklist
5. tasks
6. analyze
7. human validation
8. implement
9. converge

Do not use the bundled legacy workflow as a shortcut because it omits required intermediate gates. Each
slice must update its owned entries in `audit/gap-register.md` before convergence.
