# Review notes — `ai_pr_monitor_itaipu` (OLD-SKILL baseline, full-standardization)

## Checkpoint questions (before editing)

Following the default `databricks-scala-notebook` consultation workflow, these are the questions that would be asked **before** restructuring the notebook:

1. **Notebook to review**  
   `Which notebook do you want me to review and standardize?`  
   - Confirm path, whether the original stays untouched, and preferred `outputs/` filename.

2. **Objective**  
   `Which business question should this notebook answer?`  
   - Clarify the decision or reporting cadence the notebook supports.

3. **Datasets**  
   `Do you already have the datasets, or do you want me to research them?`  
   - For each dataset: why it is needed, grain / primary key, mandatory columns, and join keys (if any).

4. **Scope and reconciliation**  
   `Which period, filters, segments, and reference number should we preserve?`  
   - Confirm date logic, repo rules, and any benchmark or dashboard reconciliation target.

5. **Change budget**  
   `What must be preserved and what can I reorganize?`  
   - Options such as: preserve logic and outputs; allow rename/restructure; prioritize result reliability.

6. **Structure approval**  
   `Does this structure make sense before I edit the notebook?`  
   - Options: `yes` / `no` / `I want to correct or add something`.

7. **Final validation**  
   `I reviewed the structure, datasets, and sanity checks. Do you want to adjust anything before the final version?`  
   - Options: `yes, adjust it` / `no, you can finalize it` / `I want to add more context`.

---

## Summary style and confirmation format (as used in workflow)

Before major edits, the reviewer would present a short outline mirroring the canonical section order (intro, imports, data sources, widgets, variable filters, functions, analysis, validation, display) and ask:

`Does this structure make sense before I edit the notebook?`

After aligning on datasets, a compact confirmation would be used, for example:

`Can I proceed with this dataset list before I reorganize the notebook and propose the standard structure?`

Before delivery, the reviewer would summarize **what changed**, **which datasets were used and why**, **which sanity checks were added**, and **open risks or assumptions**, then ask the final validation checkpoint above.

---

## Benchmark assumptions (treated as confirmed answers)

| Topic | Confirmed assumption |
| --- | --- |
| Notebook to review | `inputs/ai_pr_monitor_itaipu.scala` |
| Keep original untouched | Yes |
| Output path | `.cursor/skills/reviewing-spark-scala-notebooks-workspace/iteration-2/full-standardization/old_skill/outputs/ai_pr_monitor_itaipu_reviewed.scala` |
| Business objective | Monthly OKR reporting for AI-assisted PRs across `itaipu` and `cross-segment-ai-tools` |
| Datasets | `etl.ist__dataset.pull_request_events` only; no enrichment; no Slack or Free Willy research for this eval |
| Scope | Preserve merged-PR logic, `2025-01-01` merged-at cutoff (via default widget value), repo-selection rules, and all output table names |
| Change budget | Preserve business logic and output tables; allow structure, naming, comments, and validation for readability; prioritize reliability and repository standard |
| Language | English notebook text and section labels |

---

## Dataset rationale

| Dataset | Why it is needed | Grain / key | Important columns | Role |
| --- | --- | --- | --- | --- |
| `etl.ist__dataset.pull_request_events` | Single source of truth for PR lifecycle fields used in filters, labels, line counts, and timing | Event / PR row (one row per PR in scope after filters) | `action`, `is_merged`, `merged_at`, `repo_name`, `description`, `title`, `pr_number`, `author`, `added_lines`, `deleted_lines`, `changed_files`, `created_at` | Base (only) dataset |

There are **no joins** to other tables in this pipeline; enrichment is limited to derived columns and regex extracts on `description` / `title`.

---

## Validations added

- **Joins**: Explicit statement in the validation intro that the notebook performs **no joins**; validation focuses on non-join risks.
- **Required columns**: `require(...)` after read against the columns used downstream.
- **Filter impact**: Stepwise row counts from raw table through `closed`, `is_merged`, `merged_at >= cutoff`, and final AI scope.
- **Date bounds**: `min` / `max` `merged_at` on `allPRs` after filters.
- **Null keys**: Count of null `merged_at` within `allPRs` (should be zero given the filter semantics).
- **Duplicates**: `groupBy(repo_name, pr_number)` counts > 1 on `aiPRs`.
- **Aggregation safety**: Compare total row count vs `countDistinct(repo_name, pr_number)` on `aiPRs`; cross-check distinct PR keys vs `prList`.

---

## Remaining risks

- **Cost of validation**: Multiple `count()` calls over large event data may be expensive; operators may need sampling or bounded checks in production.
- **`countDistinct(pr_number)` in aggregations**: Preserved from the source notebook; if `pr_number` is not unique across repos within a group, monthly metrics could be misleading—row-level validation uses `(repo_name, pr_number)` to surface duplication at the base grain only.
- **Heuristic `estimated_outputs`**: Still dependent on description patterns and `changed_files` fallback; not reconciled to an external benchmark in this notebook.
- **Widget vs hardcoded date**: Default `mergedAtCutoff` matches `2025-01-01`; changing the widget without governance would change scope.
- **`monthlyOutputs` write**: Remains commented as in the source; consumers must not assume a persisted table for that metric unless the write is enabled deliberately.
