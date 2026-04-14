// Databricks notebook source
// MAGIC %md
// MAGIC ## 0. Intro / README
// MAGIC
// MAGIC # Project Coco - Lending Suboptimality Analysis
// MAGIC
// MAGIC **Context**: This notebook estimates the size and profile of the Project Coco target segment by identifying customers who signed a Lending product while a cheaper alternative was available for the same ticket.
// MAGIC
// MAGIC **Objective**: Find customers with multi-eligibility in Lending who contracted a more expensive product despite having a cheaper alternative available, then build the Treatment B target segment.
// MAGIC
// MAGIC **Method**:
// MAGIC 1. Read recent Lending contracts in the last configured months and standardize contracted products.
// MAGIC 2. Read current eligibility sources and split public payroll into `inss` vs `public_payroll` with explicit monitoring flags.
// MAGIC 3. Compare contracted vs available alternatives on rate, eligible amount, and total debt cost.
// MAGIC 4. Keep the best alternative per customer and validate keys, joins, product coverage, and output grain.
// MAGIC
// MAGIC **Analysis structure**:
// MAGIC - 0. Intro / README
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
// MAGIC - `etl.br__dataset.personal_loan_parameters_without_models_current_snapshot` - recent Lending contracts
// MAGIC - `dataset/lending-customer-monitoring` - current eligibility flags and personal loan offer data
// MAGIC - `br__dataset.lending_customer_fgts_loan_eligibility_daily_snapshot` - FGTS offer rate and principal cap
// MAGIC - `br__policy.payroll_loan_new_contract_policy_selector` - public payroll offer rate and principal cap
// MAGIC - `etl.br__dataset.private_payroll_customer_monitoring_daily_snapshot` - private payroll offer rate and principal cap
// MAGIC
// MAGIC **Main assumptions and caveats**:
// MAGIC - `interest_rate` is already monthly in the contracts source.
// MAGIC - `created_at_date` is the contract reference date used for the rolling scope.
// MAGIC - `inss` uses `is_eligible__public_payroll_provider_dataprev`, while `public_payroll` uses the explicit `is_eligible__public_payroll_provider*` columns excluding Dataprev.
// MAGIC - The notebook excludes renegotiations and keeps the original 15% minimum suboptimality threshold.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Imports

// COMMAND ----------

// DBTITLE 1,Load notebook helpers and Spark imports
// MAGIC %run "/data-analysts/utils-uc"
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import spark.implicits._

// COMMAND ----------

// DBTITLE 1,Create early schema guard helper
def assertRequiredColumns(df: DataFrame, datasetName: String, requiredColumns: Seq[String]): Unit = {
  val missingColumns = requiredColumns.filterNot(df.columns.contains)
  require(
    missingColumns.isEmpty,
    s"[$datasetName] Missing required columns: ${missingColumns.mkString(", ")}"
  )
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Data sources
// MAGIC
// MAGIC The sources are declared explicitly before any transformation so the reader can validate the analytical inputs and their role in the flow.

// COMMAND ----------

// DBTITLE 1,Load source datasets
val contractsSource = spark.table("etl.br__dataset.personal_loan_parameters_without_models_current_snapshot")
val monitoringSource = datasets("dataset/lending-customer-monitoring")
val fgtsEligibilitySource = spark.table("br__dataset.lending_customer_fgts_loan_eligibility_daily_snapshot")
val publicPayrollEligibilitySource = spark.table("br__policy.payroll_loan_new_contract_policy_selector")
val privatePayrollEligibilitySource = spark.table("etl.br__dataset.private_payroll_customer_monitoring_daily_snapshot")

// COMMAND ----------

// DBTITLE 1,Validate source schemas before transformations
assertRequiredColumns(
  contractsSource,
  "contractsSource",
  Seq(
    "customer__id",
    "is_renegotiation",
    "is_valid_loan",
    "product",
    "credit_program",
    "created_at_date",
    "principal",
    "interest_rate",
    "installments"
  )
)

assertRequiredColumns(
  monitoringSource,
  "monitoringSource",
  Seq(
    "customer__id",
    "is_eligible__personal_loan",
    "min_loan_rate",
    "personal_loan_max_principal",
    "fgts_backed_loan__eligible",
    "is_eligible__public_payroll_provider_dataprev",
    "private_payroll_loan__eligible"
  )
)

assertRequiredColumns(
  fgtsEligibilitySource,
  "fgtsEligibilitySource",
  Seq(
    "customer__id",
    "fgts_backed_loan__eligible",
    "fgts_backed_loan__interest_rate",
    "fgts_backed_loan__max_principal_cap"
  )
)

assertRequiredColumns(
  publicPayrollEligibilitySource,
  "publicPayrollEligibilitySource",
  Seq("customer__id", "eligible", "interest_rate", "max_principal_cap")
)

assertRequiredColumns(
  privatePayrollEligibilitySource,
  "privatePayrollEligibilitySource",
  Seq(
    "customer__id",
    "private_payroll_loan__eligible",
    "private_payroll_loan__interest_rate",
    "private_payroll_loan__max_principal_cap"
  )
)

val publicPayrollProviderEligibilityColumns = monitoringSource.columns
  .filter(_.startsWith("is_eligible__public_payroll_provider"))
  .filterNot(_ == "is_eligible__public_payroll_provider_dataprev")
  .sorted

require(
  publicPayrollProviderEligibilityColumns.nonEmpty,
  "[monitoringSource] No explicit public payroll provider eligibility columns found after excluding is_eligible__public_payroll_provider_dataprev."
)

println(
  s"Explicit public payroll provider flags: ${publicPayrollProviderEligibilityColumns.mkString(", ")}"
)

println("All required source columns are available before transformations.")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Widgets
// MAGIC
// MAGIC This notebook does not require runtime widgets. Update the variable filters section when you need to change the analytical scope.

// COMMAND ----------

// DBTITLE 1,Document the absence of runtime widgets
println("No runtime widgets are required for this notebook.")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Variable filters
// MAGIC
// MAGIC The analytical scope is centralized here to avoid hidden filters downstream.

// COMMAND ----------

// DBTITLE 1,Declare notebook scope
val monthsLookback = 12
val minCostDiffPct = 0.15
val productFilter = Seq("personal_loan", "inss", "fgts", "private_payroll", "public_payroll")

println(s"Contract scope: last $monthsLookback months")
println(s"Minimum suboptimality threshold: ${minCostDiffPct * 100}%")
println(s"Configured products: ${productFilter.mkString(", ")}")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5. Functions
// MAGIC
// MAGIC Small helpers keep the validation cells readable and make the quality checks explicit.

// COMMAND ----------

// DBTITLE 1,Create notebook validation helpers
def exactDuplicateRows(df: DataFrame, keyColumns: Seq[String]): DataFrame = {
  df.groupBy(keyColumns.map(col): _*)
    .count()
    .filter(col("count") > 1)
    .orderBy(desc("count"))
}

def anyTrue(columnNames: Seq[String]): Column = {
  columnNames
    .map(columnName => coalesce(col(columnName).cast("boolean"), lit(false)))
    .reduce(_ || _)
}

def assertNoDuplicateKeys(df: DataFrame, datasetName: String, keyColumns: Seq[String]): Unit = {
  val duplicateKeyRows = exactDuplicateRows(df, keyColumns).count()
  require(
    duplicateKeyRows == 0,
    s"[$datasetName] Duplicate rows found at grain (${keyColumns.mkString(", ")}): $duplicateKeyRows"
  )
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6. Code / analysis
// MAGIC
// MAGIC The analytical flow is organized from contracts, to eligibility, to suboptimality detection, and finally to the target segment.

// COMMAND ----------

// DBTITLE 1,Build recent lending contracts
val recentLendingContracts = contractsSource
  .filter(col("is_renegotiation") === false)
  .filter(col("is_valid_loan"))
  .withColumn(
    "contracted_product_type",
    when(col("product") === "personal_loan_product__fgts_backed_loan", lit("fgts"))
      .when(col("product") === "personal_loan_product__personal_loan", lit("personal_loan"))
      .when(col("credit_program") === "private_payroll", lit("private_payroll"))
      .when(col("credit_program") === "public_payroll_agreements", lit("public_payroll"))
      .when(col("credit_program") === "public_payroll_inss", lit("inss"))
  )
  .filter(col("contracted_product_type").isin(productFilter: _*))
  .filter(col("created_at_date") >= add_months(current_date(), -monthsLookback))
  .select(
    col("customer__id"),
    col("contracted_product_type").as("product_type"),
    col("principal").as("principal_amount"),
    col("interest_rate").as("interest_rate_monthly"),
    col("installments").as("num_installments"),
    col("created_at_date").as("contract_date"),
    round(col("principal") * pow(lit(1) + col("interest_rate"), col("installments")), 2).as("total_debt_cost")
  )

recentLendingContracts.createOrReplaceTempView("recent_lending_contracts")

// COMMAND ----------

// DBTITLE 1,Build eligibility source views
val monitoringBase = monitoringSource
  .select(
    col("customer__id"),
    col("is_eligible__personal_loan"),
    col("min_loan_rate").as("pl_offered_rate"),
    col("personal_loan_max_principal").as("pl_max_principal"),
    col("fgts_backed_loan__eligible"),
    col("is_eligible__public_payroll_provider_dataprev").as("is_eligible__inss"),
    anyTrue(publicPayrollProviderEligibilityColumns).as("is_eligible__public_payroll"),
    col("private_payroll_loan__eligible")
  )

val fgtsEligibilityRates = fgtsEligibilitySource
  .filter(col("fgts_backed_loan__eligible"))
  .selectExpr(
    "customer__id",
    "fgts_backed_loan__interest_rate AS fgts_offered_rate",
    "fgts_backed_loan__max_principal_cap AS fgts_max_principal"
  )

val publicPayrollEligibilityRates = publicPayrollEligibilitySource
  .filter(col("eligible"))
  .selectExpr(
    "customer__id",
    "interest_rate AS public_payroll_offered_rate",
    "max_principal_cap AS public_payroll_max_principal"
  )

val privatePayrollEligibilityRates = privatePayrollEligibilitySource
  .filter(col("private_payroll_loan__eligible"))
  .selectExpr(
    "customer__id",
    "private_payroll_loan__interest_rate AS private_offered_rate",
    "private_payroll_loan__max_principal_cap AS private_max_principal"
  )

assertNoDuplicateKeys(monitoringBase, "monitoringBase", Seq("customer__id"))
assertNoDuplicateKeys(fgtsEligibilityRates, "fgtsEligibilityRates", Seq("customer__id"))
assertNoDuplicateKeys(publicPayrollEligibilityRates, "publicPayrollEligibilityRates", Seq("customer__id"))
assertNoDuplicateKeys(privatePayrollEligibilityRates, "privatePayrollEligibilityRates", Seq("customer__id"))

monitoringBase.createOrReplaceTempView("monitoring_base")
fgtsEligibilityRates.createOrReplaceTempView("fgts_eligibility_rates")
publicPayrollEligibilityRates.createOrReplaceTempView("public_payroll_eligibility_rates")
privatePayrollEligibilityRates.createOrReplaceTempView("private_payroll_eligibility_rates")

// COMMAND ----------

// DBTITLE 1,Create the unified current eligibility view
spark.sql("""
CREATE OR REPLACE TEMP VIEW current_eligibility AS

SELECT
  customer__id,
  'personal_loan' AS product_type,
  pl_offered_rate AS offered_rate,
  pl_max_principal AS max_eligible_amount
FROM monitoring_base
WHERE is_eligible__personal_loan = true

UNION ALL

SELECT
  m.customer__id,
  'fgts' AS product_type,
  f.fgts_offered_rate AS offered_rate,
  f.fgts_max_principal AS max_eligible_amount
FROM monitoring_base m
INNER JOIN fgts_eligibility_rates f
  ON m.customer__id = f.customer__id
WHERE m.fgts_backed_loan__eligible = true

UNION ALL

SELECT
  m.customer__id,
  'inss' AS product_type,
  p.public_payroll_offered_rate AS offered_rate,
  p.public_payroll_max_principal AS max_eligible_amount
FROM monitoring_base m
INNER JOIN public_payroll_eligibility_rates p
  ON m.customer__id = p.customer__id
WHERE m.is_eligible__inss = true

UNION ALL

SELECT
  m.customer__id,
  'public_payroll' AS product_type,
  p.public_payroll_offered_rate AS offered_rate,
  p.public_payroll_max_principal AS max_eligible_amount
FROM monitoring_base m
INNER JOIN public_payroll_eligibility_rates p
  ON m.customer__id = p.customer__id
WHERE m.is_eligible__public_payroll = true

UNION ALL

SELECT
  m.customer__id,
  'private_payroll' AS product_type,
  pp.private_offered_rate AS offered_rate,
  pp.private_max_principal AS max_eligible_amount
FROM monitoring_base m
INNER JOIN private_payroll_eligibility_rates pp
  ON m.customer__id = pp.customer__id
WHERE m.private_payroll_loan__eligible = true
""")

assertNoDuplicateKeys(spark.table("current_eligibility"), "current_eligibility", Seq("customer__id", "product_type"))

// COMMAND ----------

// DBTITLE 1,Identify suboptimal contracts
spark.sql(s"""
CREATE OR REPLACE TEMP VIEW suboptimal_contracts AS
SELECT
  c.customer__id,
  c.product_type AS contracted_product,
  c.principal_amount,
  c.num_installments,
  c.interest_rate_monthly AS contracted_rate,
  c.total_debt_cost AS contracted_total_cost,
  c.contract_date,
  e.product_type AS alternative_product,
  e.offered_rate AS alternative_rate,
  e.max_eligible_amount,
  ROUND(
    c.principal_amount * POW(1 + e.offered_rate, c.num_installments),
    2
  ) AS alternative_total_cost,
  ROUND(
    c.total_debt_cost -
    (c.principal_amount * POW(1 + e.offered_rate, c.num_installments)),
    2
  ) AS saving_brl,
  ROUND(
    (
      c.total_debt_cost /
      (c.principal_amount * POW(1 + e.offered_rate, c.num_installments))
    ) - 1,
    4
  ) AS saving_pct
FROM recent_lending_contracts c
INNER JOIN current_eligibility e
  ON c.customer__id = e.customer__id
 AND e.product_type != c.product_type
WHERE e.max_eligible_amount >= c.principal_amount
  AND e.offered_rate < c.interest_rate_monthly
  AND (
    c.total_debt_cost /
    (c.principal_amount * POW(1 + e.offered_rate, c.num_installments))
  ) >= ${1 + minCostDiffPct}
""")

// COMMAND ----------

// DBTITLE 1,Select the best alternative per customer
spark.sql("""
CREATE OR REPLACE TEMP VIEW ranked_suboptimal_contracts AS
SELECT
  *,
  ROW_NUMBER() OVER (
    PARTITION BY customer__id
    ORDER BY saving_brl DESC, contract_date DESC, alternative_product ASC
  ) AS saving_rank
FROM suboptimal_contracts
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW target_segment AS
SELECT
  customer__id,
  contracted_product,
  alternative_product AS best_alternative_product,
  alternative_rate AS best_alternative_rate,
  saving_brl AS max_saving_brl,
  saving_pct AS max_saving_pct,
  contract_date AS last_contract_date
FROM ranked_suboptimal_contracts
WHERE saving_rank = 1
""")

assertNoDuplicateKeys(spark.table("target_segment"), "target_segment", Seq("customer__id"))

// COMMAND ----------

// DBTITLE 1,Prepare final display views
spark.sql("""
CREATE OR REPLACE TEMP VIEW product_pair_metrics AS
SELECT
  contracted_product,
  alternative_product,
  COUNT(DISTINCT customer__id) AS num_customers,
  ROUND(AVG(saving_brl), 2) AS avg_saving_brl,
  ROUND(AVG(saving_pct) * 100, 1) AS avg_saving_pct,
  ROUND(MIN(saving_brl), 2) AS min_saving_brl,
  ROUND(MAX(saving_brl), 2) AS max_saving_brl,
  ROUND(AVG(principal_amount), 2) AS avg_ticket
FROM suboptimal_contracts
GROUP BY contracted_product, alternative_product
ORDER BY num_customers DESC
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW savings_bucket_distribution AS
SELECT
  FLOOR(max_saving_brl / 500) * 500 AS saving_bucket,
  COUNT(*) AS num_customers
FROM target_segment
GROUP BY saving_bucket
ORDER BY saving_bucket
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW target_segment_summary AS
SELECT
  contracted_product,
  best_alternative_product,
  COUNT(*) AS num_customers,
  ROUND(AVG(max_saving_brl), 2) AS avg_saving_brl,
  ROUND(AVG(max_saving_pct) * 100, 1) AS avg_saving_pct
FROM target_segment
GROUP BY contracted_product, best_alternative_product
ORDER BY num_customers DESC
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 7. Validation
// MAGIC
// MAGIC These checks are meant to catch the silent data risks that matter most here: missing columns, incorrect scope, unexpected duplication, product-mapping gaps, and output grain drift.

// COMMAND ----------

// DBTITLE 1,Check recent contract scope and exact duplicate rows
display(
  spark.sql("""
  SELECT
    COUNT(*) AS contract_rows,
    COUNT(DISTINCT customer__id) AS customers,
    MIN(contract_date) AS min_contract_date,
    MAX(contract_date) AS max_contract_date,
    SUM(CASE WHEN customer__id IS NULL THEN 1 ELSE 0 END) AS null_customer_id_rows,
    SUM(CASE WHEN product_type IS NULL THEN 1 ELSE 0 END) AS null_product_rows,
    SUM(CASE WHEN principal_amount IS NULL THEN 1 ELSE 0 END) AS null_principal_rows,
    SUM(CASE WHEN interest_rate_monthly IS NULL THEN 1 ELSE 0 END) AS null_rate_rows
  FROM recent_lending_contracts
  """)
)

display(
  exactDuplicateRows(
    recentLendingContracts,
    Seq(
      "customer__id",
      "product_type",
      "principal_amount",
      "interest_rate_monthly",
      "num_installments",
      "contract_date"
    )
  )
)

// COMMAND ----------

// DBTITLE 1,Check eligibility grain and product coverage
display(
  exactDuplicateRows(
    spark.table("current_eligibility"),
    Seq("customer__id", "product_type")
  )
)

val contractedProductCoverage = spark.table("recent_lending_contracts")
  .groupBy("product_type")
  .agg(
    count(lit(1)).as("contract_rows"),
    countDistinct("customer__id").as("contract_customers")
  )

val eligibilityProductCoverage = spark.table("current_eligibility")
  .groupBy("product_type")
  .agg(
    count(lit(1)).as("eligibility_rows"),
    countDistinct("customer__id").as("eligibility_customers")
  )

display(
  spark.createDataset(productFilter)
    .toDF("product_type")
    .join(contractedProductCoverage, Seq("product_type"), "left")
    .join(eligibilityProductCoverage, Seq("product_type"), "left")
    .orderBy("product_type")
)

// COMMAND ----------

// DBTITLE 1,Check configured products against contract and eligibility coverage
val contractAlternativeJoinPreview = spark.sql("""
SELECT
  c.customer__id,
  c.product_type AS contracted_product,
  c.principal_amount,
  c.num_installments,
  c.contract_date,
  e.product_type AS alternative_product
FROM recent_lending_contracts c
LEFT JOIN current_eligibility e
  ON c.customer__id = e.customer__id
 AND e.product_type != c.product_type
""")

contractAlternativeJoinPreview.createOrReplaceTempView("contract_alternative_join_preview")

val contractRows = recentLendingContracts.count()
val joinPreviewRows = contractAlternativeJoinPreview.count()
val expansionFactor = if (contractRows == 0) 0.0 else joinPreviewRows.toDouble / contractRows.toDouble

println(s"Recent contract rows: $contractRows")
println(s"Contract to eligibility join preview rows: $joinPreviewRows")
println(f"Join preview expansion factor: $expansionFactor%.2f")

display(
  spark.sql("""
  SELECT
    COUNT(*) AS joined_rows,
    COUNT(DISTINCT customer__id) AS customers,
    COUNT(DISTINCT CONCAT_WS(
      '||',
      customer__id,
      contracted_product,
      CAST(principal_amount AS STRING),
      CAST(num_installments AS STRING),
      CAST(contract_date AS STRING)
    )) AS distinct_contract_proxy_keys,
    SUM(CASE WHEN alternative_product IS NULL THEN 1 ELSE 0 END) AS rows_without_alternative_product
  FROM contract_alternative_join_preview
  """)
)

// COMMAND ----------

// DBTITLE 1,Check target segment uniqueness at customer grain
display(
  exactDuplicateRows(
    spark.table("target_segment"),
    Seq("customer__id")
  )
)

display(
  spark.sql("""
  SELECT
    COUNT(*) AS target_segment_rows,
    COUNT(DISTINCT customer__id) AS target_segment_customers,
    MIN(max_saving_brl) AS min_saving_brl,
    MAX(max_saving_brl) AS max_saving_brl,
    MIN(max_saving_pct) AS min_saving_pct,
    MAX(max_saving_pct) AS max_saving_pct,
    SUM(CASE WHEN max_saving_brl <= 0 OR max_saving_pct <= 0 THEN 1 ELSE 0 END) AS non_positive_savings_rows
  FROM target_segment
  """)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 8. Display
// MAGIC
// MAGIC Final outputs are separated from validation so the reader can consume the results after checking that the pipeline is structurally safe.

// COMMAND ----------

// DBTITLE 1,Display pair-level suboptimality metrics
display(spark.table("product_pair_metrics"))

// COMMAND ----------

// DBTITLE 1,Display savings distribution for the final target segment
display(spark.table("savings_bucket_distribution"))

// COMMAND ----------

// DBTITLE 1,Display final target segment summary
display(spark.table("target_segment_summary"))
