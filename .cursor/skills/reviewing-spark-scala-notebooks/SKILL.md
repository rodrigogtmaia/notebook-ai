---
name: reviewing-spark-scala-notebooks
description: Review and standardize Databricks Spark Scala notebooks with PT-BR user checkpoints, English notebook output, hybrid dataset validation, and strong sanity checks on duplicates, joins, columns, and grain. Use when the user mentions notebook review, exported `.scala` notebooks, Databricks, Spark Scala, notebook standardization, analysis quality, or wants a final notebook saved to `outputs/`.
---

# Reviewing Spark Scala Notebooks

## Overview

Use this skill to review or standardize an existing Databricks Spark Scala notebook step by step with the user. The goal is a final notebook in English that is readable, reproducible, aligned with the repository standard, and protected against silent data mistakes.

Do not jump to a one-shot rewrite. Align on objective, datasets, structure, and validation first.

## When To Use

- Reviewing an existing Databricks notebook exported as `.scala`
- Standardizing notebook structure, naming, sanity checks, or documentation
- Validating joins, counts, columns, filters, and grain preservation
- Reorganizing a notebook so it is understandable without oral context
- Delivering a final reviewed notebook in `outputs/`

## Repo Contract

When this skill runs inside this repository:

- Treat `README.md` as the source of truth for scope and quality bar
- Treat `templates/` as the reference standard
- Treat `inputs/` as candidate notebook inputs or benchmark targets
- Treat `outputs/` as the destination for final reviewed notebooks

## Language Contract

- All user checkpoints and consultation must be in PT-BR
- The notebook itself must be in English
- Section names, markdown explanations, `DBTITLE` names, and notebook comments should be in English
- Preserve PT-BR only for stable business terms, table names, official identifiers, or user-provided labels that should not be translated

## Quick Start

1. Confirm which notebook will be reviewed and whether the original should stay untouched.
2. Read `reference.md` before proposing structure or edits.
3. Read `examples.md` if you need PT-BR checkpoint phrasing or a notebook skeleton.
4. Ask only the next unanswered checkpoint.
5. Summarize the proposed structure before major edits.
6. Save the final notebook, plus any explicitly requested companion artifacts, in `outputs/`.

## Hard Rules

- Never rewrite the full notebook before aligning with the user.
- Treat `templates/` as references only, unless the user explicitly asks to review one of those files as the target notebook.
- Always capture the business objective before locking datasets.
- Every dataset must have an explicit rationale.
- Use a hybrid dataset validation flow:
  - if the user already has datasets, validate them first against the stated objective
  - search Slack threads and Free Willy only when the user asks for research or the dataset list has gaps, ambiguity, or obvious risk
- When datasets are researched, bring back the Free Willy link for user confirmation.
- Validate data correctness, not only formatting or style.
- Preserve business logic unless the user explicitly allows semantic changes.
- The final notebook must be understandable without oral context.
- The final notebook must keep the required section order unless the user explicitly approves a deviation.

Required section order:

0. Intro / README
1. Imports
2. Data sources
3. Widgets
4. Variable filters
5. Functions
6. Code / analysis
7. Validation
8. Display

## Required Workflow

1. Read the notebook and identify structure, readability, and correctness risks.
2. Ask the objective checkpoint in PT-BR.
3. Ask the dataset checkpoint in PT-BR.
4. If needed, branch into dataset research before editing analysis logic.
5. Ask scope and reconciliation plus change-budget checkpoints.
6. Propose the notebook outline in English section names and ask for approval.
7. Edit incrementally.
8. Add sanity checks for reads, joins, aggregations, and required columns.
9. Ask the final checkpoint before delivery.
10. Save the final notebook in `outputs/`.

## Companion Artifacts When Requested

If the user, eval, or benchmark prompt explicitly asks for companion artifacts, save them next to the final notebook in `outputs/`.

Common examples:

- `review-notes.md` in PT-BR with checkpoint assumptions, dataset rationale, validations added, and remaining risks
- `checkpoint-log.md` in PT-BR showing the open checkpoint question, the summary of what was understood, and the confirmation format `sim / não / quero corrigir ou complementar`
- `structure-approval.md` in PT-BR with the proposed notebook outline and validation plan before editing

## Required Checkpoint Pattern

Each checkpoint should be short and interactive. Prefer this sequence:

1. Ask an open PT-BR question to gather context.
2. Summarize what you understood.
3. Confirm with: `sim / não / quero corrigir ou complementar`

Use the correction before moving on. Do not batch all checkpoints unless the user explicitly asks for a faster one-shot flow.

## Mandatory Checkpoints

### 1. Notebook Target

Confirm:

- which notebook should be reviewed
- whether the original should stay untouched
- whether there is a preferred output filename in `outputs/`

### 2. Objective

Ask in PT-BR which business question the notebook needs to answer.

Examples:

- discover how many customers are eligible for a landing page
- calculate half-year risk metrics

Before moving on, summarize the interpreted objective and confirm it.

### 3. Datasets

Ask in PT-BR whether the user already has the datasets or wants research.

For every dataset used, confirm:

- why it is needed
- expected grain or primary key
- join keys
- mandatory columns
- whether it is base, enrichment, filter, benchmark, or output

### 4. Scope And Reconciliation

Confirm:

- period and date logic
- filters and segments
- official dashboard, reference table, or expected benchmark number
- constraints that cannot be broken

### 5. Change Budget

Confirm:

- what must be preserved
- what can be renamed or reorganized
- whether priority is readability, performance, standardization, or result reliability

### 6. Structure Approval

Before major edits, present the section order plus the planned validation cells and ask the user to approve or correct it.

### 7. Final Validation

Before final delivery, present:

- what changed
- which datasets were used and why
- which sanity checks were added
- any open risk or assumption that still needs user confirmation

## Dataset Research Branch

Use this branch only when research is necessary.

1. Search local references first.
2. Search relevant Slack threads for prior art, caveats, and join logic if tools are available.
3. Search Free Willy: `https://backoffice.nubank.com.br/free-willy/home/`
4. Return each candidate dataset with:
   - dataset name
   - why it is needed
   - expected grain/key
   - important columns
   - join role
   - Free Willy link
   - any unresolved risk
5. Ask the user to confirm the dataset list before editing analysis logic.

If Slack or Free Willy tools are unavailable, say so explicitly and ask the user to validate the datasets manually.

## Final Gate

Do not consider the notebook final until all of the following are true:

- the intro reads like a mini README
- the required section order is present
- datasets are justified
- duplicates, joins, columns, and grain are validated
- cell names are meaningful
- assumptions are explicit
- the final artifact is ready for `outputs/`

## Additional Resources

- Target structure, naming rules, and quality bar: `reference.md`
- PT-BR checkpoint phrasing and notebook skeleton: `examples.md`
