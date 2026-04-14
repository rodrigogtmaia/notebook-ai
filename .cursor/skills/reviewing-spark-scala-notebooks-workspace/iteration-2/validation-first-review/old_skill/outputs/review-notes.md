# Review notes — validation-first (OLD-SKILL baseline)

## Checkpoint questions (before editing)

Ask only the next unanswered checkpoint, in order, using the skill’s default English phrasing.

### 1. Notebook to review

- `Which notebook do you want me to review and standardize?`
- Confirm whether the original should stay untouched and the preferred output path under `outputs/`.

### 2. Objective

- `Which business question should this notebook answer?`

### 3. Datasets

- `Do you already have the datasets, or do you want me to research them?`
- For each dataset: why it is needed, grain/key, mandatory columns, and expected join keys (if any).

### 4. Scope and reconciliation

- `Which period, filters, segments, and reference number should we preserve?`

### 5. Change budget

- `What must be preserved and what can I reorganize?`
- Options such as: preserve logic and only improve structure; prioritize result reliability.

### 6. Structure approval

- `Does this structure make sense before I edit the notebook?`
- Options: yes / no / I want to correct or add something.

### 7. Final validation

- `I reviewed the structure, datasets, and sanity checks. Do you want to adjust anything before the final version?`
- Options: yes, adjust it / no, you can finalize it / I want to add more context.

## Summary style and confirmation format

- After each checkpoint block, give a **short summary** of what was agreed (notebook path, keep-original flag, objective, dataset list, scope, change budget).
- For dataset confirmation when research was **not** needed, use the compact confirmation pattern:  
  `Can I proceed with this dataset list before I reorganize the notebook and propose the standard structure?`
- Before major edits, restate the **notebook outline** (intro, imports, sources, filters, analysis, validation, display) and wait for structure approval when the workflow is interactive.

## Assumptions used for this benchmark iteration

- **Notebook to review**: `inputs/ai_pr_monitor_itaipu.scala`
- **Keep original untouched**: yes
- **Output notebook path**:  
  `/Users/rodrigo.maia/Documents/notebook-ai/.cursor/skills/reviewing-spark-scala-notebooks-workspace/iteration-2/validation-first-review/old_skill/outputs/ai_pr_monitor_itaipu_validation_first.scala`
- **Business objective**: monthly OKR reporting of AI-assisted PR activity
- **Datasets**: `etl.ist__dataset.pull_request_events` only; user-confirmed list, no discovery branch
- **Scope and reconciliation**: preserve current filters, output tables, and PR classification logic (Rapidash / Warp Pipe / cross-segment-ai-tools)
- **Change budget**: preserve business logic; prioritize data correctness and validation strength over stylistic refactors

## Why dataset research was not triggered

The user confirmed a **single, fixed dataset** (`etl.ist__dataset.pull_request_events`) with no request for Slack or Free Willy discovery. Per the skill, the dataset research branch runs only when the user asks for help finding or confirming datasets. Here the list was already confirmed, so research (Slack, Free Willy links) was **out of scope** and would delay delivery without adding agreed inputs.

## Joins

This notebook **does not join** any tables. It reads one source and applies filters and aggregations. Join-specific checks (row count before/after join, unmatched keys) were **not** added; instead, the notebook documents **no-join reasoning** and validates **filters, grain, duplicates, and aggregation safety**.

## Validations added

1. **Required columns**: `require` after read on `pull_request_events` for all columns used downstream (actions, merge state, timestamps, repo, text fields, PR id, author, line/file stats).
2. **Date filter scope**: `min`/`max` of `merged_at` on the filtered `allPRs` population after closed/merged/date predicates.
3. **Repo and classification filters**: `groupBy` counts for `repo_name` and `ai_tool` on `aiPRs` to spot unexpected repos or empty segments.
4. **Intended grain and duplicate PR counting**: duplicate detection at `(repo_name, pr_number)`; row count vs distinct composite grain; comparison of `countDistinct(pr_number)` vs composite distinct to surface cross-repo `pr_number` collision risk.
5. **Aggregation safety**: after `monthlyVolume`, sum of per-group `total_prs` compared (via printed reconciliation) to global distinct PR metrics to reason about double-counting across months/tools.
6. **Explicit no-join note**: intro and validation section markdown state there are no joins and what was validated instead.

## Remaining risks

- **`pr_number` uniqueness**: If `pr_number` is not globally unique, `countDistinct("pr_number")` may not match business “unique PR” intent; composite `(repo_name, pr_number)` may be the true grain — validation prints both for manual judgment.
- **Heuristic metrics**: `estimated_outputs` and regex extraction remain approximate; no external benchmark was provided for reconciliation.
- **Operational cost**: Additional `count`, `groupBy`, and `display` calls add cluster work on large tables; production schedules may need sampling or bounded checks.
- **Hard-coded window**: `merged_at >= "2025-01-01"` is not parameterized via widgets; changing OKR windows still requires code edits.
