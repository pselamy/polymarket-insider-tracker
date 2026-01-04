---
name: priority-issue-resolution
description: Autonomous issue resolution workflow for processing priority issues in polymarket-insider-tracker. Covers claiming, investigation, worktree-based fixes, PR shepherding, and CI monitoring. Activates on "fix issues", "resolve bugs", "claim issue", "priority queue", "issue triage", or "autonomous resolution".
allowed-tools: Bash, Read, Grep, Glob, Task, GitHub, Write, Edit
---

# Priority Issue Resolution Workflow

Autonomous workflow for resolving issues from highest to lowest priority in polymarket-insider-tracker.

## When to Use

- Working through backlog of priority issues
- Autonomous issue resolution sessions
- After test verification discovers failures
- When cleaning up after feature deployments

## Core Workflow

```
┌─────────────────────────────────────────────────────────────┐
│  PRIORITY ISSUE RESOLUTION LOOP                             │
│                                                             │
│  1. SCAN → Find highest priority unclaimed issue            │
│  2. CLAIM → Post comment with ETA                           │
│  3. INVESTIGATE → Verify relevance, gather context          │
│  4. DECIDE → Close / Add details / Decompose / Fix          │
│  5. FIX → Worktree + PR + Monitor CI + Merge                │
│  6. REPEAT → Return to step 1                               │
│                                                             │
│  STOP when no issues remain                                 │
└─────────────────────────────────────────────────────────────┘
```

## Step 1: Scan for Issues

### Priority Scan

```bash
#!/bin/bash
# scan-priority-issues.sh - Find highest priority issues

echo "=== PRIORITY ISSUE SCAN ==="

REPO="pselamy/polymarket-insider-tracker"

# Priority high first
echo "--- priority:high ---"
gh issue list --repo $REPO --label "priority:high" --state open \
  --json number,title --jq '.[] | "#\(.number): \(.title)"'

# Then priority medium
echo "--- priority:medium ---"
gh issue list --repo $REPO --label "priority:medium" --state open \
  --json number,title --jq '.[] | "#\(.number): \(.title)"'

# Then unlabeled (may need triage)
echo "--- unlabeled ---"
gh issue list --repo $REPO --state open \
  --json number,title,labels --jq '.[] | select(.labels | length == 0) | "#\(.number): \(.title)"'
```

### Priority Order

| Priority | Label            | Action         |
| -------- | ---------------- | -------------- |
| High     | `priority:high`  | Fix NOW        |
| Medium   | `priority:medium`| Fix this cycle |
| Low      | `priority:low`   | Queue or close |

## Step 2: Claim the Issue

### Check Claim Status

Before claiming, verify the issue isn't already claimed:

```bash
# Check for claim tags in recent comments
ISSUE_NUM=1234
REPO="pselamy/polymarket-insider-tracker"

LAST_COMMENT=$(gh api repos/$REPO/issues/$ISSUE_NUM/comments \
  --jq '.[(-1)]?.body // ""' 2>/dev/null)

if echo "$LAST_COMMENT" | grep -qE '\[(CLAIMED|AGENT-|ANALYSIS|EDITING)\]'; then
  echo "Issue already claimed - skip"
else
  echo "Issue available - claim it"
fi
```

### Post Claim Comment

```bash
gh issue comment $ISSUE_NUM --repo $REPO --body "[CLAIMED] Claiming this issue. ETA: ~30 minutes.

**Investigation Plan:**
1. Understand the root cause
2. Identify affected files
3. Implement fix with worktree
4. Create PR and monitor CI

Will update with analysis shortly."
```

## Step 3: Investigate

### Investigation Checklist

1. **Read the full issue** - Understand the reported problem
2. **Check for related issues** - May be duplicate or related to other work
3. **Search codebase** - Find relevant files and understand context
4. **Verify reproducibility** - Confirm the issue is real and current

### Investigation Outcomes

| Finding             | Action                           |
| ------------------- | -------------------------------- |
| Issue is irrelevant | Close with explanation           |
| Insufficient detail | Add research findings, keep open |
| Too big             | Decompose into smaller issues    |
| Ready to fix        | Proceed to Step 4                |

### Close Irrelevant Issues

```bash
gh issue close $ISSUE_NUM --repo $REPO --reason "not_planned" \
  --comment "[ANALYSIS] Closing as not relevant.

**Reason:** [Explain why the issue is no longer valid]

Examples:
- Feature already removed
- Duplicate of #XXX
- Working as intended
- Cannot reproduce"
```

### Add Missing Details

```bash
gh issue comment $ISSUE_NUM --repo $REPO --body "[ANALYSIS] Investigation findings:

## Root Cause
[Describe what you found]

## Affected Files
- \`path/to/file1.py\`
- \`path/to/file2.py\`

## Proposed Fix
[Describe the solution approach]

## Complexity
[Low/Medium/High] - [Time estimate]"
```

### Decompose Large Issues

```bash
# Create sub-issues for complex work
gh issue create --repo $REPO \
  --title "[Sub] Part 1: Implement X" \
  --label "priority:medium" \
  --body "Parent issue: #$ISSUE_NUM

## Scope
[Describe this sub-task]

## Acceptance Criteria
- [ ] Criterion 1
- [ ] Criterion 2"

# Update parent issue
gh issue comment $ISSUE_NUM --repo $REPO \
  --body "[DECOMPOSED] Split into smaller issues:
- #XXX - Part 1: Implement X
- #YYY - Part 2: Implement Y"
```

## Step 4: Fix with Worktree

### Create Worktree

```bash
ISSUE_ID=1234

# Navigate to repo
cd /path/to/polymarket-insider-tracker

# Fetch latest and create worktree
git fetch origin
git worktree add -b fix/${ISSUE_ID} \
  ../worktrees/polymarket-${ISSUE_ID} \
  origin/main

# Work in worktree
cd ../worktrees/polymarket-${ISSUE_ID}
```

### Make the Fix

1. **Edit files** - Apply the minimal fix
2. **Run tests locally** - Verify the fix works
3. **Check formatting** - Run linters

```bash
# Run linting
ruff check src/ tests/

# Run type checking
mypy src/

# Run tests
pytest
```

### Create PR

```bash
git add -A
git commit -m "fix: [description] (#$ISSUE_ID)"
git push -u origin fix/${ISSUE_ID}

gh pr create --title "fix: [description] (#$ISSUE_ID)" \
  --body "## Summary
- [Describe the fix]

## Root Cause
- [Explain what was wrong]

## Test plan
- [ ] Tests pass
- [ ] Verified locally

Closes #$ISSUE_ID"
```

## Step 5: Monitor CI and Merge

### CI Monitoring Loop

```bash
PR_NUMBER=1234
REPO="pselamy/polymarket-insider-tracker"

while true; do
  STATUS=$(gh pr checks $PR_NUMBER --repo $REPO 2>&1)

  if echo "$STATUS" | grep -q "fail"; then
    echo "CI FAILED - investigating..."
    gh pr checks $PR_NUMBER --repo $REPO 2>&1
    # Fix the failure, commit, push
    break
  elif ! echo "$STATUS" | grep -q "pending"; then
    echo "All checks passed - merging"
    gh pr merge $PR_NUMBER --repo $REPO --merge --delete-branch
    break
  else
    echo "CI still running..."
    sleep 60
  fi
done
```

### Fix CI Failures

Common CI failures and fixes:

| Failure Type | Fix                                    |
| ------------ | -------------------------------------- |
| Lint (Ruff)  | `ruff check . --fix`                   |
| Format       | `ruff format .`                        |
| Type check   | Fix type errors in code                |
| Tests        | Fix failing tests or update assertions |

```bash
# After fixing, amend and push
git add -A
git commit --amend --no-edit
git push --force-with-lease
```

### Post Victory Comment

```bash
gh issue comment $ISSUE_NUM --repo $REPO \
  --body "[VICTORY] Fixed in PR #$PR_NUMBER (merged).

**Root Cause:** [Brief explanation]

**Fix:** [What was changed]"
```

## Step 6: Cleanup and Repeat

### Cleanup Worktree

```bash
cd /path/to/polymarket-insider-tracker
git worktree remove ../worktrees/polymarket-${ISSUE_ID}
git branch -d fix/${ISSUE_ID}  # Local branch cleanup
```

### Return to Step 1

Continue scanning for the next highest priority issue until none remain.

## Creating New Issues

When you discover bugs or missing features while working:

1. **DO NOT fix them in the current PR** - Stay focused
2. **Create a new issue** with appropriate priority
3. **Comment in current issue** referencing the new discovery

```bash
# Create new issue for discovered problem
NEW_ISSUE=$(gh issue create --repo $REPO \
  --title "[Discovery] [Brief description]" \
  --label "priority:medium,bug" \
  --body "Discovered while fixing #$ISSUE_NUM.

## Problem
[Describe the issue]

## Where Found
\`path/to/file.py:line\`

## Impact
[Severity/Impact]" \
  --json number --jq '.number')

# Comment in current issue
gh issue comment $ISSUE_NUM --repo $REPO \
  --body "[DISCOVERY] Found unrelated issue while investigating.
Created #$NEW_ISSUE to track. Staying focused on current fix."
```

## Quick Reference

### Common Commands

```bash
REPO="pselamy/polymarket-insider-tracker"

# Scan priority:high issues
gh issue list --repo $REPO --label "priority:high" --state open

# Check if issue is claimed
gh api repos/$REPO/issues/1234/comments \
  --jq '.[(-1)]?.body // ""' | grep -q '\[CLAIMED\]'

# Monitor PR CI
gh pr checks 1234 --repo $REPO

# Merge when ready
gh pr merge 1234 --repo $REPO --merge --delete-branch

# Close with reason
gh issue close 1234 --repo $REPO --reason completed
```

### Component Labels

| Component       | Label                    |
| --------------- | ------------------------ |
| Data Collection | `component:collector`    |
| Risk Analysis   | `component:analyzer`     |
| Alerting        | `component:alerter`      |
| Storage/DB      | `component:storage`      |
| Infrastructure  | `component:infrastructure`|

## Anti-Patterns

| Anti-Pattern           | Problem                          | Prevention                        |
| ---------------------- | -------------------------------- | --------------------------------- |
| Fix without claiming   | Duplicate work with other agents | Always claim first                |
| Scope creep during fix | PR becomes too large             | Create new issues for discoveries |
| Skip CI monitoring     | Failed PRs left unmerged         | Monitor until green and merged    |
| Develop on main        | Messy git history                | Always use worktree               |
| Abandon failing CI     | Stale PRs                        | Fix failures, don't abandon       |

## Related Skills

- `@worktree-lifecycle` - Full worktree patterns
- `@creating-issues-and-prs` - PR creation conventions
