# Examples

## Checkpoint Pattern

Use a two-step PT-BR checkpoint whenever possible:

1. Ask an open question
2. Summarize what you understood and confirm with:
   - `sim`
   - `não`
   - `quero corrigir ou complementar`

## Checkpoint Sequence

### 1. Notebook Target

Open question:

`Qual notebook você quer revisar e padronizar?`

Helpful follow-up:

Você quer manter o original intacto e gerar a versão final em `outputs/`?

Confirmation pattern:

Entendi que vou revisar `inputs/algum_notebook.scala` e gerar a saída em `outputs/algum_notebook_reviewed.scala`. Está correto? Responda com: `sim` / `não` / `quero corrigir ou complementar`

### 2. Objective

Open question:

`Qual objetivo de negócio esse notebook precisa responder?`

Examples of valid goals:

- `descobrir quantos clientes são elegíveis para a landing`
- `calcular semestralmente as métricas de risco`
- `acompanhar mensalmente os PRs gerados com apoio de IA`

Confirmation pattern:

`Entendi que o objetivo principal é <resumo do objetivo>. Está correto? Responda com: sim / não / quero corrigir ou complementar`

### 3. Datasets

Open question:

`Você já tem os datasets ou quer que eu pesquise?`

If the user already has datasets, follow up with:

- `Por que esse dataset entra na análise?`
- `Qual é o grão ou a chave principal dele?`
- `Quais colunas são obrigatórias para esta análise?`
- `Quais chaves você espera usar nos joins?`

If research is needed, come back with:

- dataset name
- why it is needed
- expected grain
- important columns
- Free Willy link
- open risks

Confirmation pattern:

`Entendi esta lista de datasets e o papel de cada um. Posso seguir com ela? Responda com: sim / não / quero corrigir ou complementar`

### 4. Scope And Reconciliation

Open question:

`Qual período, filtros, segmentos e número de referência eu preciso preservar?`

Examples:

- `jan/2025 em diante`
- `segmento core`
- `precisa bater com o dashboard oficial`

Confirmation pattern:

`Entendi que o escopo é <resumo do período, filtros e benchmark>. Está correto? Responda com: sim / não / quero corrigir ou complementar`

### 5. Change Budget

Open question:

`O que precisa ser preservado e o que eu posso reorganizar?`

Examples:

- `preservar a lógica e só melhorar a estrutura`
- `pode reorganizar e renomear`
- `prioridade máxima para confiabilidade do resultado`

Confirmation pattern:

`Entendi que devo <resumo da margem de mudança>. Está correto? Responda com: sim / não / quero corrigir ou complementar`

### 6. Structure Approval

Prompt:

`Esta estrutura faz sentido antes de eu editar o notebook? Responda com: sim / não / quero corrigir ou complementar`

Good structure-approval example:

`Entendi que a estrutura proposta é: 0. Intro / README, 1. Imports, 2. Data sources, 3. Widgets, 4. Variable filters, 5. Functions, 6. Code / analysis, 7. Validation, 8. Display. Também vou adicionar validações para colunas obrigatórias, duplicidade no grão de PR, escopo de datas e segurança de agregação. Está correto? Responda com: sim / não / quero corrigir ou complementar`

### 7. Final Validation

Prompt:

`Revisei a estrutura, os datasets e os sanity checks. Você quer ajustar algo antes da versão final? Responda com: sim / não / quero corrigir ou complementar`

## Dataset Rationale Example

Use a compact rationale like this before editing the analysis:

| Dataset | Why it is needed | Grain / key | Important columns | Join role | Validation source |
| --- | --- | --- | --- | --- | --- |
| `br__dataset.customer_eligibility_daily` | base population for the analysis | one row per `customer__id` per day | `customer__id`, `reference_date`, `is_eligible` | base | user confirmation or Free Willy |
| `br__dataset.customer_profile_snapshot` | segment and profile enrichment | one row per `customer__id` | `customer__id`, `segment_name`, `income_band` | enrichment | user confirmation or Free Willy |

## Example Standardized Structure

```scala
// Databricks notebook source
// MAGIC %md
// MAGIC # Analysis Title
// MAGIC
// MAGIC **Context**: Explain why this notebook exists and which decision it supports.
// MAGIC
// MAGIC **Objective**: State the business question in one sentence.
// MAGIC
// MAGIC **Method**:
// MAGIC 1. Read the base population
// MAGIC 2. Apply filters and enrichments
// MAGIC 3. Validate joins, duplicates, and required columns
// MAGIC 4. Display the final metrics
// MAGIC
// MAGIC **Analysis structure**:
// MAGIC - 0. Intro / README (this block)
// MAGIC - 1. Imports
// MAGIC - 2. Data sources
// MAGIC - 3. Widgets
// MAGIC - 4. Variable filters
// MAGIC - 5. Functions
// MAGIC - 6. Code / analysis
// MAGIC - 7. Validation
// MAGIC - 8. Display
// MAGIC
// MAGIC **Data sources**:
// MAGIC - `br__dataset.customer_eligibility_daily` - eligibility base
// MAGIC - `br__dataset.customer_profile_snapshot` - profile enrichment
// MAGIC
// MAGIC **Main assumptions**:
// MAGIC - One row per customer per reference date in the base dataset
// MAGIC - Left joins preserve the base population unless explicitly documented otherwise

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Imports

// COMMAND ----------

// DBTITLE 1,Imports
// MAGIC %run "/data-analysts/utils-uc"
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Data sources

// COMMAND ----------

// DBTITLE 1,Load eligibility base
val eligibilityBase = spark.table("br__dataset.customer_eligibility_daily")

// COMMAND ----------

// DBTITLE 1,Load customer profile enrichment
val customerProfile = spark.table("br__dataset.customer_profile_snapshot")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Widgets

// COMMAND ----------

// DBTITLE 1,Create runtime widgets
dbutils.widgets.text("startDate", "2025-01-01")
dbutils.widgets.text("endDate", "2025-12-31")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Variable filters

// COMMAND ----------

// DBTITLE 1,Declare notebook scope
val startDate = dbutils.widgets.get("startDate")
val endDate = dbutils.widgets.get("endDate")

val eligibilityBaseFiltered = eligibilityBase
  .filter(col("reference_date").between(startDate, endDate))

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5. Functions

// COMMAND ----------

// DBTITLE 1,Build eligible population
def buildEligiblePopulation(base: DataFrame, profile: DataFrame): DataFrame = {
  base.join(profile, Seq("customer__id"), "left")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6. Code / analysis

// COMMAND ----------

// DBTITLE 1,Create final analytical base
val eligiblePopulation = buildEligiblePopulation(eligibilityBaseFiltered, customerProfile)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 7. Validation

// COMMAND ----------

// DBTITLE 1,Check required columns after read
val requiredColumns = Seq("customer__id", "reference_date", "is_eligible")
val missingColumns = requiredColumns.filterNot(eligibilityBaseFiltered.columns.contains)

require(missingColumns.isEmpty, s"Missing columns: ${missingColumns.mkString(", ")}")

// COMMAND ----------

// DBTITLE 1,Check duplicates at customer grain
val duplicateCustomers = eligiblePopulation
  .groupBy("customer__id")
  .count()
  .filter(col("count") > 1)

display(duplicateCustomers)

// COMMAND ----------

// DBTITLE 1,Validate join coverage against base population
val baseCustomers = eligibilityBaseFiltered.select("customer__id").distinct().count()
val finalCustomers = eligiblePopulation.select("customer__id").distinct().count()

println(s"Base customers: $baseCustomers")
println(s"Final customers: $finalCustomers")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 8. Display

// COMMAND ----------

// DBTITLE 1,Display final metrics
display(
  eligiblePopulation
    .groupBy("segment_name")
    .agg(countDistinct("customer__id").as("eligible_customers"))
)
```

## Validation Cell Name Examples

Good validation cell names:

- `DBTITLE 1,Check required columns after read`
- `DBTITLE 1,Check duplicates at customer grain`
- `DBTITLE 1,Validate left join coverage`
- `DBTITLE 1,Compare final total with trusted benchmark`

Avoid:

- `DBTITLE 1,checks`
- `DBTITLE 1,validacao`
- `DBTITLE 1,join test`
- `DBTITLE 1,apagar`
