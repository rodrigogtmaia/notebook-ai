# Checkpoint review notes (pre-edit)

This document records the **default checkpoint workflow** from the Databricks Scala Notebook skill before any notebook edits, plus how answers would be summarized and confirmed.

---

## Exact checkpoint questions (in order)

1. **Notebook to review**  
   - Which notebook should be reviewed and standardized?  
   - Should the original stay untouched?  
   - Is there a preferred output filename under `outputs/`?

2. **Objective**  
   - Which business question should this notebook answer?  
   - Which decision will this notebook support?

3. **Datasets**  
   - Do you already have the datasets, or do you want me to research them?  
   - For each dataset: Why is it needed? What is its grain or primary key? Which columns are mandatory? Which keys are used in joins?

4. **Scope and reconciliation**  
   - Which period, date logic, filters, and segments should we preserve?  
   - Is there an official dashboard, reference table, or benchmark number?  
   - What constraints cannot be broken?

5. **Change budget**  
   - What must be preserved vs. what can be renamed or reorganized?  
   - Is the priority readability, performance, standardization, or result reliability?

6. **Structure approval**  
   - Does this proposed notebook outline (intro → imports → data sources → widgets → variable filters → functions → analysis → validation → display) work before editing?

7. **Final validation**  
   - What changed, which datasets were used and why, which sanity checks were added, and what open risks remain?  
   - Do you want any adjustments before the final version?

---

## Summary style after each answer

After each checkpoint answer, provide a **short bullet summary** that captures:

- **Decision**: what was agreed (one line).  
- **Implications**: what that means for structure, datasets, or validation (one to three bullets).  
- **Open items**: only if something is still ambiguous; otherwise state “none”.

Example (illustrative):

- **Decision**: Review `inputs/ai_pr_monitor_itaipu.scala`; keep source unchanged; deliver to the agreed `outputs/` path.  
- **Implications**: Intro and section order will follow the repo standard; logic and output table names stay as confirmed.  
- **Open items**: none.

---

## Confirmation format

After presenting a checkpoint summary, use an explicit **yes/no or choice** prompt so the user can confirm or correct before the next step.

**Template:**

> **Checkpoint N summary:** [1–3 bullets]  
> **Please confirm:** [A] Proceed as summarized  [B] Correct something (briefly what)

For dataset lists (when research applies), end with:

> Can I proceed with this dataset list before I reorganize the notebook and propose the standard structure?

For structure approval:

> Does this structure make sense before I edit the notebook? (yes / no / I want to correct or add something)

---

## Why dataset research is **not** triggered in this case

Dataset research (Slack, Free Willy, links for confirmation) is only used when the user **asks for help finding or confirming datasets** or answers **“research them”** to: *Do you already have the datasets, or do you want me to research them?*

Here, the benchmark **pre-confirms**:

- The **only** dataset is `etl.ist__dataset.pull_request_events`.  
- **No dataset research is needed.**

So the skill stays on the **user-confirmed dataset** path: document rationale, grain, and mandatory columns in the notebook intro and data-sources section, without opening the dataset research branch.

---

## Simulated confirmed answers (iteration 2 benchmark)

The following are treated as **confirmed** after the checkpoint flow:

| Topic | Confirmed answer |
| --- | --- |
| Objective | Monthly OKR reporting for AI-assisted PRs |
| Datasets | Only `etl.ist__dataset.pull_request_events`; no research |
| Change budget | Preserve business logic and output table names; reorganize to repository standard |
| Language | English for checkpoints, summaries, and notebook explanations |

Proceed to edit only under the benchmark output path, without modifying the source notebook.
