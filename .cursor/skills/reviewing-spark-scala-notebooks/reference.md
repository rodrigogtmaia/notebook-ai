# Reference

This reference defines the target standard for reviewing and standardizing Databricks Spark Scala notebooks in this repository.

Use it to decide:

- what the notebook structure should look like
- how to name sections, variables, and cells
- which data correctness checks are mandatory
- which anti-patterns block a notebook from being considered final

## Source Of Truth

When this skill runs inside this repository, use these references in this order:

1. `README.md`
2. `templates/boas_praticas.txt`
3. `templates/01. RAM Analysis by Global Holdout.scala`
4. `templates/Academia AI.md`
5. `templates/Repo CLO.scala` as a secondary catalog-style reference

If the user explicitly asks for a different structure, follow the user. Otherwise, use the standard below.

## Canonical Notebook Order

Use this order unless the user explicitly approves a deviation.

If one section is not needed, keep the section header and add a short markdown note instead of silently removing it. This keeps the notebook scannable and consistent.

| Section | What should be present | Why it exists |
| --- | --- | --- |
| Intro / README | context, objective, method, analysis structure, data sources, main assumptions | lets the reader understand the notebook before reading the code |
| Imports | Spark imports, `%run`, helper imports | centralizes dependencies |
| Data sources | all datasets declared with clear names and rationale | makes inputs visible early |
| Widgets | runtime parameters or a note saying there are no runtime widgets | avoids hidden edits later |
| Variable filters | period, segment, scope, benchmark, or metric filters | makes analytical scope explicit |
| Functions | reusable functions or a note saying helpers are not needed | keeps analysis cells readable |
| Code / analysis | main transformations and metric construction | contains the core analytical story |
| Validation | sanity checks after reads, joins, and aggregations | proves the logic is safe |
| Display | final tables, charts, or exported outputs | separates proof from presentation |

## Intro Requirements

The first markdown block should read like a mini README.

It should explain:

- why the notebook exists
- which business question it answers
- how the method works at a high level
- how the notebook is organized
- which datasets are used
- what assumptions or caveats the reader must know

The style target is the opening block in `templates/01. RAM Analysis by Global Holdout.scala`: enough context to understand the notebook without asking the author for a walkthrough.

If the notebook cannot be understood from the intro, it is not ready.

## Language And Tone

- User checkpoints must be in PT-BR
- The notebook must be in English
- Section labels should be in English
- `DBTITLE` names should be in English
- Code comments should explain business rationale, assumptions, and risks, not obvious syntax
- Preserve source-language terms only when they are official product names, dataset names, or stable business identifiers

## Naming Conventions

| Element | Convention | Notes |
| --- | --- | --- |
| objects and classes | `PascalCase` | use for Scala objects, case classes, and class names |
| variables and vals | `camelCase` | prefer explicit names like `eligibleCustomersBase` over `df1` |
| string values or identifiers | `kebab-case` | use for slug-like values, widget options, or human-readable identifiers |
| table columns | `snake_case` | preserve external dataset column conventions |

Additional rules:

- prefer domain names over generic names
- if a dataset is a base population, say so in the variable name
- if a DataFrame changes grain, reflect that in the variable name
- if a dataset is already filtered, signal that in the name
- do not shadow the same business concept with multiple similar names unless the difference is explicit

Good examples:

- `eligibilityBase`
- `eligibilityBaseFiltered`
- `customersAtCustomerGrain`
- `customerProfileEnrichment`

Bad examples:

- `df1`
- `tmp`
- `result2`
- `base2`

## Cell Naming Rationale

Cell names are part of the documentation. They should explain the analytical journey, not label arbitrary blocks.

Use these rules:

- markdown section titles explain why the next block exists
- `DBTITLE` names explain the single responsibility of the code cell
- validation cells should mention the check and the grain being validated
- section numbers should make the notebook readable top to bottom

Good examples:

- `## 2. Data sources`
- `DBTITLE 1,Load eligibility base`
- `DBTITLE 1,Check duplicates at customer grain`
- `DBTITLE 1,Validate join coverage against base population`
- `DBTITLE 1,Compare final metric with trusted benchmark`

Bad examples:

- `DBTITLE 1,teste`
- `DBTITLE 1,apagar`
- `DBTITLE 1,cell 1`
- `DBTITLE 1,join`
- `DBTITLE 1,validacao`

## Dataset Rationale

Every dataset must be justified before heavy editing starts.

For each dataset, document:

- dataset name
- why it is needed
- expected grain or primary key
- important columns used by the analysis
- join role: base, enrichment, filter, benchmark, or output
- user-confirmed source link when researched via Free Willy
- unresolved risk or caveat

If the user asks for help discovering datasets:

- search local references first
- search Slack threads for prior art, caveats, and join logic when tools are available
- search Free Willy: `https://backoffice.nubank.com.br/free-willy/home/`
- return the dataset rationale and Free Willy link for confirmation

## Data Correctness Checklist

The notebook is not correct just because the code looks organized. It must prove the data is being read and transformed correctly.

### After Reading Data

Check at least the relevant subset of:

- row count or a bounded count strategy
- `count` versus `countDistinct` on the intended grain
- nulls in join keys and business keys
- min and max date after filters
- required columns are present
- key filters changed the population as expected

Questions to answer:

- Are we counting the same customer more than once?
- Are all expected columns present?
- Did we forget a needed column early in the pipeline?
- Is the base population at the grain we think it is?

### After Joins

Check at least the relevant subset of:

- row count before and after join
- distinct customer count before and after join
- unmatched keys on both sides when meaningful
- unexpected nulls introduced by the join
- duplication ratio after join for the intended grain

Questions to answer:

- Did this join exclude customers unexpectedly?
- Did this join duplicate customers unexpectedly?
- Is `inner` correct here, or do we need `left` to preserve the population?
- Are enrichment columns actually coming through?
- Are we still at the intended grain after the join?

### After Aggregations

Check at least the relevant subset of:

- totals against a simpler reference query
- `count` versus `countDistinct` for the final metric grain
- expected segmentation totals
- benchmark reconciliation when an official number exists

Questions to answer:

- Did aggregation collapse the right grain?
- Are we double-counting after aggregation?
- Does the final number reconcile with a trusted source?
- Did we lose a required dimension or column before the final aggregation?

## Spark And Databricks Guardrails

Prefer:

- imports near the top
- parameters and widgets near the top
- clear dataset declarations
- one logical transformation block per cell or helper
- comments on business rationale, not obvious code mechanics
- idempotent write semantics when materializing outputs
- explicit write destinations and overwrite rationale

Be careful with:

- `collect()` or `toPandas()` on large data
- full-table `count()` with no operational reason
- hidden filters inside long chains
- unexplained `%python` sections inside a Scala notebook
- writes with `append` and no clear contract
- inline credentials or service account material

## Anti-Patterns To Flag

Flag these before calling the notebook finished:

- missing intro or weak notebook context
- dataset used without rationale
- generic or temporary cell names
- leftover debug cells or fragments such as `apagar`, `teste`, or partial expressions
- secrets or credentials inline in the notebook
- variable names like `df1`, `tmp`, or `result2`
- reused variable names that hide a grain change
- joins without validation on row impact
- dropped columns with no explanation
- sanity checks only at the end when earlier checks were needed
- final number presented without reconciliation or caveat
- mixed PT-BR and English section structure with no reason
- no explicit output or display section

## Final Quality Bar

Do not consider the notebook final until:

- the intro reads like a mini README
- the required section order is present
- the notebook is readable top to bottom
- datasets are justified
- joins and grain are validated
- duplicate-count risk is addressed
- required columns were checked
- assumptions are explicit
- cell names are meaningful
- the notebook can be read without oral context
- the final artifact is ready for `outputs/`
