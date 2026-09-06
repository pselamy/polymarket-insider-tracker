# Constitution Amendment Proposal: Spec Kit Artifact Ownership

**Proposed version**: 1.0.1
**Status**: Awaiting Patrick approval
**Reason**: Clarify current GitHub Spec Kit v1.0.4 artifact ownership and multi-feature activation without
changing the product boundary or quality bar.

The constitution's phrase “requirements checklist” is directionally correct but insufficiently precise:
Spec Kit has both an author-owned built-in spec-quality checklist at `checklists/requirements.md` and
reviewer-owned custom domain checklists at `checklists/<domain>.md`. The ignored local feature pointer also
creates a silent wrong-slice risk when one branch contains multiple feature directories.

Upon Patrick's approval of the revised baseline, apply this patch-level governance clarification:

1. Replace Development Workflow and Quality Gates item 2 with:

   > Each material slice MUST follow the applicable Spec Kit sequence: specify, clarify, plan,
   > reviewer-owned domain requirements-quality checklists, tasks, analyze, human validation, implement,
   > and converge. The built-in `checklists/requirements.md` remains the author-owned spec-quality checklist.

2. Add after item 3:

   > Agents MUST leave newly generated reviewer-owned domain checklist items unchecked. Only Patrick or a
   > human reviewer he designates may mark those requirements-quality criteria satisfied.

3. Add before the pull-request gate:

   > Before every per-slice Spec Kit command, the intended feature directory MUST be activated explicitly.
   > Ignored local pointer state MUST NOT select a slice implicitly on a multi-feature branch or fresh clone.

4. Add before the pull-request gate:

   > Every slice MUST update its owned gap-register entries and record any approved deferral before convergence.

5. Keep all other principles and governance unchanged; update the Sync Impact Report, version, and amended
   date as required by `$speckit-constitution` only after approval.
