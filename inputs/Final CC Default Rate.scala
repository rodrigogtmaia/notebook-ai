// Databricks notebook source
// DBTITLE 1,README
// MAGIC %md
// MAGIC # CC Risk Monitoring Metrics: Default Rate 
// MAGIC
// MAGIC **Context:** Part of the [2nd Line Program Approval](https://docs.google.com/presentation/d/1-NptYIKh2POlZmRQUY6Km0TR10ckW1MvNjlvp5MubiY/edit?slide=id.g3d1e1b29e3e_2_121#slide=id.g3d1e1b29e3e_2_121) for CLM risk monitoring. We track two agreed-upon metrics comparing **treatment vs. control** across CC segments (Core, HI). PJ is excluded because it uses a different dataset and model.
// MAGIC
// MAGIC Metrics built by: @Cora Cecchini and @Renan Critelli
// MAGIC
// MAGIC > ⚠️ **Do NOT edit this notebook directly.** Create a personal copy via `File → Clone` before making changes. This is the canonical version for the CLM Program Approval.
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### Metrics
// MAGIC
// MAGIC | Program Name | Internal Name | Formula | What it measures |
// MAGIC | --- | --- | --- | --- |
// MAGIC | Δ Unit Risk Rate | `unit_risk_rate` | unique defaulted customers / total holdout (treat vs. control) | Share of holdout customers who defaulted (65+ days late) on at least one bill |
// MAGIC | Δ Balance Risk Ratio | `balance_risk_ratio` | Σ outstanding balance / Σ total balance at bill due date (treat vs. control) | Difference in default exposure relative to total billed balance |
// MAGIC
// MAGIC Statistical significance: 95% CI (z = 1.96).
// MAGIC SE formulas:
// MAGIC proportion (√(p(1−p)/N));
// MAGIC ratio (delta method).
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### Data Sources
// MAGIC
// MAGIC | Dataset | Variable | Description |
// MAGIC | --- | --- | --- |
// MAGIC | `br__dataset.proactive_monitoring_dataset` | `ccDefaultMonitoring` → `defaultBase` | N rows per account once it shows all its bills × M `bills_after_score` (0–40) per bill. Filtered to `bills_after_score = 0`. |
// MAGIC | `br__dataset.account_flags` | `accountFlags` | 1 row per `account__id` → `customer__id` mapping + `is_secured_card`, `is_republish`, `is_clipublish` → `account_tag` |
// MAGIC | `holdouts_consolidado_2025` | `holdoutsAll` → `holdouts` (PJ filtered) | Holdout assignments: Core, HI × marketing-active / marketing-universal-control |
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### Filters & Assumptions
// MAGIC
// MAGIC | Parameter | Value | Rationale |
// MAGIC | --- | --- | --- |
// MAGIC | Period | `bill__due_date` between **2025-07-01** and **2025-12-31** | H2 2025 |
// MAGIC | Bills after score | `bills_after_score = 0` | Only the bill active at score date (not subsequent bills) |
// MAGIC | Computable default | `computable_default = true` | 65+ days elapsed; default is observable |
// MAGIC | Outstanding balance | `outstanding_balance_bill_due_date > 0` | Excludes negatives or zero-exposure customers |
// MAGIC | Default criteria | 65+ days past due | Standard criteria used by the team |
// MAGIC | Segments | **Core, HI** only | PJ uses a different dataset and model — filtered from holdouts |
// MAGIC
// MAGIC ### Limitations
// MAGIC
// MAGIC * Default requires 65+ days to be observable — only periods at least 65 days ago can be analyzed
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### How to Update for a New Period
// MAGIC
// MAGIC > **Always create a copy first** — do not modify this notebook directly.
// MAGIC
// MAGIC 1. **Create a copy**: `File → Clone` to your personal workspace
// MAGIC 2. **Update period filters**: cell **"Data Sources: defaultBase"** → change the `bill__due_date` date range
// MAGIC 3. **Update holdouts** (if needed): cell **"External Sources"** → point to the correct holdout table. Global holdouts for 2025 and 2026 are available in the [`Holdouts Consolidado`](https://nubank-e2-general.cloud.databricks.com/editor/notebooks/593935908159212?o=2093534396923660) notebook
// MAGIC 4. **Run sequentially**: Re-run from cell **"External Sources"** downward — all intermediate tables overwrite automatically. Do not run this notebook in parallel with other CLM notebooks that share the same schema to avoid race conditions on intermediate tables.
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### Notebook Structure
// MAGIC
// MAGIC 1. **Imports** — Shared utilities
// MAGIC 2. **External Sources** — `ccDefaultMonitoring`, `accountFlags`, `holdoutsAll` (raw reads, no business filters)
// MAGIC 3. **Data Sources** — `defaultBase` (period + computable_default filters + account_tag), `holdouts` (PJ filtered from `holdoutsAll`), `defaultWithHoldout`, `holdoutByVariant`
// MAGIC 4. **Functions** — `calcMetrics` (bill level → client level → group level), `withStatisticalSignificance` (proportion or ratio method)
// MAGIC 5. **Metrics Calculation** — One granularity, 2 metrics each (unit_risk_rate, balance_risk_ratio):
// MAGIC    * **Per Segment**: `metricsSegment` → `statSigSegment` → `slimSegment`
// MAGIC 6. **CC Default Rate Metrics** — `ccDefaultMetrics`: Summary of the CLM Program Approval Metrics: Unit Risk Rate and Balance Risk Ratio
// MAGIC 7. **Sanity Checks** — Pipeline funnel
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### Variable Naming
// MAGIC
// MAGIC | Stage | Variable | Description |
// MAGIC | --- | --- | --- |
// MAGIC | External | `ccDefaultMonitoring` | Raw proactive monitoring dataset (unfiltered) |
// MAGIC | External | `accountFlags` | account__id → customer__id mapping + account flags (is_secured_card, is_republish, is_clipublish) |
// MAGIC | External | `holdoutsAll` | Raw holdout assignments (unfiltered) |
// MAGIC | Data | `holdouts` | Holdouts filtered: PJ excluded |
// MAGIC | Data | `defaultBase` | Monitoring filtered (period, bills_after_score=0, computable_default, outstanding>0), joined with account_flags + account_tag |
// MAGIC | Data | `defaultWithHoldout` | Join defaultBase × holdouts |
// MAGIC | Data | `holdoutByVariant` | Holdout count per segmento × variant |
// MAGIC | Metrics | `metricsSegment` | calcMetrics + unit_risk_rate |
// MAGIC | Stat Sig | `statSigSegment` | 2× withStatisticalSignificance (unit_risk_rate, balance_risk_ratio) |
// MAGIC | Viz Cut | `slimSegment` | Treatment vs control side-by-side |
// MAGIC | Final | `ccDefaultMetrics` | Summary: 2 metrics × 2 segments |
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### Column Naming Convention
// MAGIC
// MAGIC | Column | Description |
// MAGIC | --- | --- |
// MAGIC | `total_segment_holdout` | Total holdout customers in segment (treatment + control) |
// MAGIC | `unique_total_customers` | Distinct customers with bills in the period (per group) |
// MAGIC | `unique_defaulted_customers` | Distinct customers who defaulted (65+ days late) on at least one bill |
// MAGIC | `account_tag` | Account type: secured-card, republish, clipublish, or empty |
// MAGIC | `*_treatment` / `*_control` | Metric value for each variant |
// MAGIC | `delta_abs_*` | Absolute delta: treatment − control |
// MAGIC | `delta_relative_*` | Relative delta: delta / control |
// MAGIC | `margin_abs_*` | Margin of error (absolute) |
// MAGIC | `margin_relative_*` | Margin of error (relative) |
// MAGIC | `IC_lower_abs_*` / `IC_upper_abs_*` | 95% CI bounds (absolute) |
// MAGIC | `IC_lower_rel_*` / `IC_upper_rel_*` | 95% CI bounds (relative: %) |
// MAGIC | `*_stat_sig` | "stat-sig" or "non-stat-sig" |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Imports

// COMMAND ----------

// MAGIC %run "/data-analysts/utils-uc"

// COMMAND ----------

// MAGIC %md
// MAGIC ## External Sources

// COMMAND ----------

// Raw table reads — no business filters applied here

val ccDefaultMonitoring = spark.table("br__dataset.proactive_monitoring_dataset")
// N rows per account ID × M bills_after_score (0–40) per bill_due_date

val accountFlags = spark.table("br__dataset.account_flags")
// 1 row per account__id → customer__id mapping (+ account_flag)
// Replaces account_key_integrity — same mapping, consistent with CC Risk Monitoring

val ccDefaultMonitoringEnriched = ccDefaultMonitoring.join(accountFlags, Seq("account__id"))

val holdoutsAll = spark.table("usr.br__customer_lifecycle_optimization.holdouts_consolidado_2025")
// Holdout assignments for 2025. PJ filter applied in Data Sources section below.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Functions

// COMMAND ----------

// DBTITLE 1,calcMetrics function
// =============================================================================
// calcMetrics — Default metrics for treatment vs control comparison
//
// Aggregates bill-level data into group-level metrics in 2 steps:
//   1) Client: had_default_in_period (max), outstanding_balance, total_balance
//   2) Group:  counts + stddev/cov between clients for inference
//
// Output columns:
//   unique_total_customers, unique_defaulted_customers,
//   total_outstanding_balance, total_balance, balance_risk_ratio,
//   stddev_outstanding_balance_per_client, stddev_total_balance_per_client,
//   cov_outstanding_total_per_client
// =============================================================================

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

def calcMetrics(groupingColumn: Array[String])(df: DataFrame): DataFrame = {
  val keys = groupingColumn

  val withDerived = df
    .withColumn("is_in_default",
      when($"defaults_now" === true, lit(1)).otherwise(lit(0)))
    .withColumn("outstanding_balance",
      when($"defaults_now" === true, $"outstanding_balance_bill_due_date").otherwise(lit(0.0))) //assumption that outstanding balance at default 65+ days equats to total outstanding balance at due date - assumption given by @Renan critelli and common to the team
    .withColumn("total_balance", $"outstanding_balance_bill_due_date")

  // Level 1 — Client: one row per customer per group
  val clientLevel = withDerived
    .groupBy((col("customer__id") +: keys.map(col)): _*)
    .agg(
      max($"is_in_default").as("had_default_in_period"), //if customer had at least one default in the priod it is considered defaulted
      sum($"outstanding_balance").as("outstanding_balance_client"),
      sum($"total_balance").as("total_balance_client"))

  // Level 2 — Group: aggregate across clients
  clientLevel
    .groupBy(keys.map(col): _*)
    .agg(
      count("customer__id").as("unique_total_customers_converted"),
      sum("had_default_in_period").as("unique_defaulted_customers"),
      sum("outstanding_balance_client").as("total_outstanding_balance"),
      sum("total_balance_client").as("total_balance"),
      stddev("outstanding_balance_client").as("stddev_outstanding_balance_per_client"),
      stddev("total_balance_client").as("stddev_total_balance_per_client"),
      covar_samp("outstanding_balance_client", "total_balance_client").as("cov_outstanding_total_per_client"))
    .withColumn("balance_risk_ratio",
      when($"total_balance" > 0, $"total_outstanding_balance" / $"total_balance")
        .otherwise(lit(null)))
}

// COMMAND ----------

// DBTITLE 1,withStatisticalSignificance function
// =============================================================================
// withStatisticalSignificance
//
// Compares each variant against control (marketing-universal-control) within
// the same cell (defined by windowSpec). Adds delta, 95% CI, and stat sig flag.
//
// Output columns (suffixed by metricName):
//   delta_abs, delta_relative, margin_abs, margin_relative,
//   IC_lower_abs, IC_upper_abs, IC_lower_rel, IC_upper_rel,
//   <metricName>_stat_sig
//
// Metric types (set exactly one flag to true):
//   isProportion  → SE(p) = √(p(1−p)/N)                                      e.g. conversion
//   isSumMetric   → SE(Σ) = √(N×σ²)                                          e.g. total_amount_lent
//   isRatioMetric → SE(R) = (1/den)×√(Var_num + R²·Var_den − 2R·Cov)   e.g. default_rate
//   isMeanMetric  → SE(μ) = σ/√N                                               e.g. avg per client
//
// SE(delta) = √( SE(treatment)² + SE(control)² )
// =============================================================================

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

def withStatisticalSignificance(
    df: DataFrame,
    metricName: String,
    metricCol: String,
    nCol: String,
    windowSpec: org.apache.spark.sql.expressions.WindowSpec,
    isProportion: Boolean = false,
    isSumMetric: Boolean = false,
    isRatioMetric: Boolean = false,
    isMeanMetric: Boolean = false,
    stddevCol: String = "",
    numeratorSumCol: String = "",
    denominatorSumCol: String = "",
    stddevNumeratorCol: String = "",
    stddevDenominatorCol: String = "",
    covCol: String = "",
    zForCI: Double = 1.96
): DataFrame = {

  // --- Output column names (all suffixed by metricName) ---
  val marginAbsCol   = s"margin_abs_$metricName"
  val marginRelCol   = s"margin_relative_$metricName"
  val deltaAbsCol    = s"delta_abs_$metricName"
  val deltaRelCol    = s"delta_relative_$metricName"
  val icLowerAbsCol  = s"IC_lower_abs_$metricName"
  val icUpperAbsCol  = s"IC_upper_abs_$metricName"
  val icLowerRelCol  = s"IC_lower_rel_$metricName"
  val icUpperRelCol  = s"IC_upper_rel_$metricName"

  // --- Control values: fetched via window from the control row in the same cell ---
  val ctrlMetric = max(when($"variant" === "marketing-universal-control", col(metricCol))).over(windowSpec)
  val ctrlN      = max(when($"variant" === "marketing-universal-control", col(nCol))).over(windowSpec)

  // --- SE(delta) calculation: depends on metric type ---
  val seDelta = if (isProportion) {
    // PROPORTION: SE(p) = √(p(1−p)/N)
    val ctrlStd  = sqrt(max(when($"variant" === "marketing-universal-control",
      col(metricCol) * (lit(1) - col(metricCol)))).over(windowSpec))
    val varTreat = col(metricCol) * (lit(1) - col(metricCol))
    val varCtrl  = pow(ctrlStd, 2)
    sqrt(varTreat / col(nCol) + varCtrl / ctrlN)

  } else if (isSumMetric) {
    // SUM: SE(Σ) = √(N × σ²)
    val ctrlStd  = max(when($"variant" === "marketing-universal-control", col(stddevCol))).over(windowSpec)
    val varTreat = col(nCol) * pow(col(stddevCol), 2)
    val varCtrl  = ctrlN * pow(ctrlStd, 2)
    sqrt(varTreat + varCtrl)

  } else if (isRatioMetric) {
    // RATIO: SE(R) via delta method = (1/den) × √(Var_num + R²·Var_den − 2R·Cov)
    val ctrlR      = max(when($"variant" === "marketing-universal-control", col(metricCol))).over(windowSpec)
    val ctrlDen    = max(when($"variant" === "marketing-universal-control", col(denominatorSumCol))).over(windowSpec)
    val ctrlStdNum = max(when($"variant" === "marketing-universal-control", col(stddevNumeratorCol))).over(windowSpec)
    val ctrlStdDen = max(when($"variant" === "marketing-universal-control", col(stddevDenominatorCol))).over(windowSpec)
    val ctrlCov    = max(when($"variant" === "marketing-universal-control", col(covCol))).over(windowSpec)

    // SE for treatment
    val varNumT = col(nCol) * pow(col(stddevNumeratorCol), 2)
    val varDenT = col(nCol) * pow(col(stddevDenominatorCol), 2)
    val covT    = col(nCol) * col(covCol)
    val seT     = (lit(1) / col(denominatorSumCol)) * sqrt(
      varNumT + pow(col(metricCol), 2) * varDenT - lit(2) * col(metricCol) * covT)

    // SE for control
    val varNumC = ctrlN * pow(ctrlStdNum, 2)
    val varDenC = ctrlN * pow(ctrlStdDen, 2)
    val covC    = ctrlN * ctrlCov
    val seC     = (lit(1) / ctrlDen) * sqrt(
      varNumC + pow(ctrlR, 2) * varDenC - lit(2) * ctrlR * covC)

    // SE(delta) = √(SE_treat² + SE_ctrl²)
    sqrt(pow(seT, 2) + pow(seC, 2))

  } else {
    // MEAN: SE(μ) = σ/√N
    val ctrlStd  = max(when($"variant" === "marketing-universal-control", col(stddevCol))).over(windowSpec)
    val varTreat = pow(col(stddevCol), 2) / col(nCol)
    val varCtrl  = pow(ctrlStd, 2) / ctrlN
    sqrt(varTreat + varCtrl)
  }

  // --- Build output: margin, delta, CI bounds (abs + relative), significance ---
  df
    .withColumn(marginAbsCol,  lit(zForCI) * seDelta)
    .withColumn(marginRelCol,  when(ctrlMetric =!= 0, col(marginAbsCol) / ctrlMetric).otherwise(lit(null)))
    .withColumn(deltaAbsCol,   col(metricCol) - ctrlMetric)
    .withColumn(deltaRelCol,   when(ctrlMetric =!= 0, col(deltaAbsCol) / ctrlMetric).otherwise(lit(null)))
    .withColumn(icLowerAbsCol, col(deltaAbsCol) - col(marginAbsCol))
    .withColumn(icUpperAbsCol, col(deltaAbsCol) + col(marginAbsCol))
    .withColumn(icLowerRelCol, when(ctrlMetric =!= 0, col(icLowerAbsCol) / ctrlMetric).otherwise(lit(null)))
    .withColumn(icUpperRelCol, when(ctrlMetric =!= 0, col(icUpperAbsCol) / ctrlMetric).otherwise(lit(null)))
    .withColumn(s"${metricName}_stat_sig",
      when(col(icLowerAbsCol) > 0 || col(icUpperAbsCol) < 0, "stat-sig")
        .otherwise("non-stat-sig"))
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Data Sources

// COMMAND ----------

// DBTITLE 1,Data Sources: defaultBase
// Filter proactive_monitoring_dataset to H2 2025 bills, join with account_flags for customer__id + account_tag
//
// Filters:
//   bills_after_score = 0       → only the bill active at score date (not subsequent bills)
//   bill__due_date H2 2025      → experiment period (2025-07-01 to 2025-12-31)
//   computable_default = true   → 65+ days elapsed; default is observable
//   outstanding_balance > 0     → excludes overpayers (negative) and zero-exposure
//
// Why outstanding_balance_bill_due_date (not outstanding_balance_at_default)?
// outstanding_balance_at_default has ~12% nulls when defaults_now=true
// (likely default_already_started cases with days_late=5–8).

// =========================================================================
// FILTERS — update these when running for a new period
// =========================================================================
val defaultBase = ccDefaultMonitoringEnriched 
  .filter(
    $"bills_after_score"  === 0 &&
    $"bill__due_date"     >= "2025-07-01" &&  // <-- update period start
    $"bill__due_date"     <= "2025-12-31" &&  // <-- update period end
    $"computable_default" === true &&
    $"outstanding_balance_bill_due_date" > 0)
  .select(
    $"customer__id",
    $"account__id",
    $"bill__due_date",
    $"defaults_now",
    $"outstanding_balance_bill_due_date",
    $"secured_card_flag",
    $"republish_flag",
    $"clipublish_flag")

  .withColumn("account_tag",
    when($"secured_card_flag" === true, lit("secured-card"))
      .when($"republish_flag" === true, lit("republish"))
      .when($"clipublish_flag" === true, lit("clipublish"))
      .otherwise(lit("other")))

defaultBase.write
  .mode("overwrite")
  .format("delta")
  .option("overwriteSchema", "true")
  .saveAsTable("usr.br__customer_lifecycle_optimization.default_Recorte_v2")

// COMMAND ----------

// DBTITLE 1,Val defaultBase
val defaultBaseTable = spark.table("usr.br__customer_lifecycle_optimization.default_Recorte_v2")

// COMMAND ----------

// DBTITLE 1,Data Sources: Holdouts + Join

// PJ uses a different dataset and model — filtered out from this analysis
val holdouts = holdoutsAll.filter($"segmento" =!= "PJ")

// Join default base with holdout assignments
val defaultWithHoldout = defaultBase
  .join(holdouts.select("customer__id", "variant", "segmento"), Seq("customer__id"), "inner")

defaultWithHoldout.write
  .mode("overwrite")
  .format("delta")
  .option("overwriteSchema", "true")
  .saveAsTable("usr.br__customer_lifecycle_optimization.join_default_v2")



// COMMAND ----------

val defaultWithHoldoutTable = spark.table ("usr.br__customer_lifecycle_optimization.join_default_v2")

// COMMAND ----------

// DBTITLE 1,Holdout Count
// Holdout counts: used as denominator for unit_risk_rate (proportion)
val holdoutByVariant = holdouts
  .groupBy("segmento", "variant")
  .agg(countDistinct("customer__id").as("total_holdout"))

// COMMAND ----------

// DBTITLE 1,Total holdout Customer
// MAGIC %md
// MAGIC ## Metrics Calculation

// COMMAND ----------

// MAGIC %md
// MAGIC ### Segment and Variant

// COMMAND ----------

// DBTITLE 1,calcMetrics: Segment
// Metrics at segmento × variant granularity to calculate defaulted balance
// unit_risk_rate = unique_defaulted_customers / total_holdout (proportion of holdout that defaulted)

//outstanding balance risk 
val metricsSegment = defaultWithHoldout
  .transform(calcMetrics(Array("segmento", "variant")))
  .join(holdoutByVariant, Seq("segmento", "variant"), "inner")
  //unit risk
  .withColumn("unit_risk_rate",
    when($"total_holdout" =!= 0, $"unique_defaulted_customers" / $"total_holdout")
      .otherwise(lit(null)))

display (metricsSegment)

// COMMAND ----------

// DBTITLE 1,Stat Sig: Segment
// Stat sig: compares treatment vs control within each segment
// Input:  metricsSegment
// Output: statSigSegment

import org.apache.spark.sql.expressions.Window

val w = Window.partitionBy("segmento")

// Δ Unit Risk Rate (proportion)
val s1 = withStatisticalSignificance(metricsSegment,
  "unit_risk_rate", "unit_risk_rate", "total_holdout", w,
  isProportion = true)

// Δ Balance Risk Ratio (ratio — delta method)
val statSigSegment = withStatisticalSignificance(s1,
  "balance_risk_ratio", "balance_risk_ratio", "unique_total_customers_converted", w,
  isRatioMetric = true,
  numeratorSumCol = "total_outstanding_balance",
  denominatorSumCol = "total_balance",
  stddevNumeratorCol = "stddev_outstanding_balance_per_client",
  stddevDenominatorCol = "stddev_total_balance_per_client",
  covCol = "cov_outstanding_total_per_client")
  .orderBy("segmento", "variant")

// COMMAND ----------

// DBTITLE 1,CC Default Rate Deepdive
// Deepdive: treatment vs control side-by-side per segment with all the metrics
// Input:  statSigSegment
// Output: slimSegment

val controlSegment = statSigSegment
  .filter($"variant" === "marketing-universal-control")
  .select(
    $"segmento",
    $"total_holdout".as("total_holdout_control"),
    $"unique_total_customers_converted".as("unique_customers_converted_control"),
    $"unique_defaulted_customers".as("unique_defaulted_customers_control"),
    $"unit_risk_rate".as("unit_risk_rate_control"),
    $"balance_risk_ratio".as("balance_risk_ratio_control"),
    $"total_outstanding_balance".as("outstanding_balance_control"),
    $"total_balance".as("total_balance_control"))

val slimSegment = statSigSegment
  .filter($"variant" === "marketing-active")
  .join(controlSegment, Seq("segmento"), "left")
  .select(
    $"segmento",
    $"total_holdout".as("total_holdout_treatment"),
    $"total_holdout_control",
    $"unique_total_customers_converted".as("unique_customers_converted_treatment"),
    $"unique_customers_converted_control",
    // unit risk rate
    $"unique_defaulted_customers".as("unique_defaulted_customers_treatment"),
    $"unique_defaulted_customers_control",
    ($"unique_defaulted_customers" - $"unique_defaulted_customers_control").as("delta_defaulted_customers"),
    $"unit_risk_rate".as("unit_risk_rate_treatment"),
    $"unit_risk_rate_control",
    $"delta_abs_unit_risk_rate",
    $"delta_relative_unit_risk_rate",
    $"margin_relative_unit_risk_rate",
    $"IC_lower_rel_unit_risk_rate",
    $"IC_upper_rel_unit_risk_rate",
    $"unit_risk_rate_stat_sig",
    // balance risk ratio
    $"total_balance".as("total_balance_treatment"),
    $"total_balance_control",
    $"total_outstanding_balance".as("outstanding_balance_treatment"),
    $"outstanding_balance_control",
    $"balance_risk_ratio".as("balance_risk_ratio_treatment"),
    $"balance_risk_ratio_control",
    $"delta_abs_balance_risk_ratio",
    $"delta_relative_balance_risk_ratio",
    $"margin_relative_balance_risk_ratio",
    $"IC_lower_rel_balance_risk_ratio",
    $"IC_upper_rel_balance_risk_ratio",
    $"balance_risk_ratio_stat_sig")
  .orderBy("segmento")

display(slimSegment)

// COMMAND ----------

// DBTITLE 1,Segment, Variant and Account Tag
// MAGIC %md
// MAGIC ### Segment, Variant and Account Tag

// COMMAND ----------

// DBTITLE 1,calcMetrics: Account Tag
// Account tag granularity: segmento × variant × account_tag
// Input:  defaultWithHoldout
// Output: metricas_default_por_account_tag (Delta table)

val defaultWithHoldoutBase = spark.table("usr.br__customer_lifecycle_optimization.join_default_v2")

val metricsAccountTag = defaultWithHoldoutBase
  .transform(calcMetrics(Array("segmento", "variant", "account_tag")))

  // same as cell 16: join holdoutByVariant for total_holdout
  .join(holdoutByVariant, Seq("segmento", "variant"), "inner")

  // unit_risk_rate = unique_defaulted_customers / total_holdout
  .withColumn("unit_risk_rate",
    when($"total_holdout" =!= 0, $"unique_defaulted_customers" / $"total_holdout")
      .otherwise(lit(null)))

metricsAccountTag.write
  .mode("overwrite")
  .format("delta")
  .option("overwriteSchema", "true")
  .saveAsTable("usr.br__customer_lifecycle_optimization.metricas_default_por_account_tag")

display(metricsAccountTag)

// COMMAND ----------

// DBTITLE 1,Stat Sig: Account Tag
// Stat sig: compares treatment vs control within each (segmento, account_tag)
// Input:  metricas_default_por_account_tag (Delta table)
// Output: statSigAccountTag

val metricsAccountTagBase = spark.table("usr.br__customer_lifecycle_optimization.metricas_default_por_account_tag")

val w = Window.partitionBy("segmento", "account_tag")

// Δ Unit Risk Rate (proportion)
val s1 = withStatisticalSignificance(metricsAccountTagBase,
  "unit_risk_rate", "unit_risk_rate", "total_holdout", w,
  isProportion = true)

// Δ Balance Risk Ratio (ratio — delta method)
val statSigAccountTag = withStatisticalSignificance(s1,
  "balance_risk_ratio", "balance_risk_ratio", "unique_total_customers_converted", w,
  isRatioMetric = true,
  numeratorSumCol = "total_outstanding_balance",
  denominatorSumCol = "total_balance",
  stddevNumeratorCol = "stddev_outstanding_balance_per_client",
  stddevDenominatorCol = "stddev_total_balance_per_client",
  covCol = "cov_outstanding_total_per_client")
  .orderBy("segmento", "account_tag")

display(statSigAccountTag)

// COMMAND ----------

// DBTITLE 1,Deep Dive per Account Tag
// Viz cut: account tag — treatment vs control side-by-side
// Input:  statSigAccountTag
// Output: slimAccountTag

val segmentTotals = slimSegment
  .select(
    $"segmento",
    $"delta_abs_unit_risk_rate".as("delta_abs_unit_risk_rate_total"))

val controlAccountTag = statSigAccountTag
  .filter($"variant" === "marketing-universal-control")
  .select(
    $"segmento",
    $"account_tag",
    $"total_holdout".as("total_holdout_control"),
    $"unique_total_customers_converted".as("unique_customers_converted_control"),
    $"unique_defaulted_customers".as("unique_defaulted_customers_control"),
    $"unit_risk_rate".as("unit_risk_rate_control"),
    $"balance_risk_ratio".as("balance_risk_ratio_control"),
    $"total_outstanding_balance".as("outstanding_balance_control"),
    $"total_balance".as("total_balance_control"))

val slimAccountTag = statSigAccountTag
  .filter($"variant" === "marketing-active")
  .join(controlAccountTag, Seq("segmento", "account_tag"), "left")
  .join(segmentTotals, Seq("segmento"), "left")
  .withColumn("group", $"account_tag")
  .withColumn("share_defaulted", $"delta_abs_unit_risk_rate" / $"delta_abs_unit_risk_rate_total")
  .select(
    $"segmento",
    $"group",
    $"total_holdout".as("total_holdout_treatment"),
    $"total_holdout_control",
    $"unique_total_customers_converted".as("unique_customers_converted_treatment"),
    $"unique_customers_converted_control",
    // unit risk rate
    $"unique_defaulted_customers".as("unique_defaulted_customers_treatment"),
    $"unique_defaulted_customers_control",
    ($"unique_defaulted_customers" - $"unique_defaulted_customers_control").as("delta_defaulted_customers"),
    $"unit_risk_rate".as("unit_risk_rate_treatment"),
    $"unit_risk_rate_control",
    $"delta_abs_unit_risk_rate",
    $"share_defaulted",
    $"delta_relative_unit_risk_rate",
    $"margin_relative_unit_risk_rate",
    $"IC_lower_rel_unit_risk_rate",
    $"IC_upper_rel_unit_risk_rate",
    $"unit_risk_rate_stat_sig",
    // balance risk ratio
    $"total_balance".as("total_balance_treatment"),
    $"total_balance_control",
    $"total_outstanding_balance".as("outstanding_balance_treatment"),
    $"outstanding_balance_control",
    $"balance_risk_ratio".as("balance_risk_ratio_treatment"),
    $"balance_risk_ratio_control",
    $"delta_abs_balance_risk_ratio",
    $"delta_relative_balance_risk_ratio",
    $"margin_relative_balance_risk_ratio",
    $"IC_lower_rel_balance_risk_ratio",
    $"IC_upper_rel_balance_risk_ratio",
    $"balance_risk_ratio_stat_sig")
  .orderBy("segmento", "group")

display(slimAccountTag)

// COMMAND ----------

// MAGIC %md
// MAGIC ## CC Default Rate Metrics for CLM Program Approval

// COMMAND ----------

// DBTITLE 1,Summary CC Default Rate Metrics
// CC Default Rate Metrics — CLM Program Approval
// 2 metrics × 2 segments = 4 rows
// Input: slimSegment
// Output: ccDefaultMetrics

// Row 1: Δ Unit Risk Rate (proportion)
val unitRiskRows = slimSegment.select(
  $"segmento",
  lit("total").as("group"),
  lit("unit_risk_rate").as("metric_name"),
  $"total_segment_holdout",
  $"unique_defaulted_customers".as("relevant_customers"),
  $"delta_relative_unit_risk_rate".as("delta_relative"),
  $"IC_lower_rel_unit_risk_rate".as("IC_lower"),
  $"IC_upper_rel_unit_risk_rate".as("IC_upper"),
  $"unit_risk_rate_stat_sig".as("stat_sig"))

// Row 2: Δ Balance Risk Ratio (ratio)
val balanceRiskRows = slimSegment.select(
  $"segmento",
  lit("total").as("group"),
  lit("balance_risk_ratio").as("metric_name"),
  $"total_segment_holdout",
  $"unique_total_customers".as("relevant_customers"),
  $"delta_relative_balance_risk_ratio".as("delta_relative"),
  $"IC_lower_rel_balance_risk_ratio".as("IC_lower"),
  $"IC_upper_rel_balance_risk_ratio".as("IC_upper"),
  $"balance_risk_ratio_stat_sig".as("stat_sig"))

// Union: 2 metrics × 2 segments = 4 rows
val ccDefaultMetrics = unitRiskRows
  .unionByName(balanceRiskRows)
  .orderBy(
    $"segmento",
    when($"metric_name" === "unit_risk_rate", 1)
      .otherwise(2))

display(ccDefaultMetrics)

// COMMAND ----------

// DBTITLE 1,Forest Plot: Setup
// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import matplotlib.ticker as mtick
// MAGIC import numpy as np
// MAGIC from matplotlib.lines import Line2D
// MAGIC
// MAGIC # ---------------------------------------------------------------------------
// MAGIC # Reusable forest plot function
// MAGIC # ---------------------------------------------------------------------------
// MAGIC def forest_plot(segment, data, metric_names, title):
// MAGIC     colors = {True: "#2563EB", False: "#94A3B8"}
// MAGIC     fig, ax = plt.subplots(figsize=(10, 3.5))
// MAGIC     y_pos = np.arange(len(metric_names))
// MAGIC
// MAGIC     for i, m in enumerate(metric_names):
// MAGIC         d = data[m]
// MAGIC         color = colors[d["sig"]]
// MAGIC         ax.plot([d["lo"], d["hi"]], [i, i], color=color, linewidth=3, solid_capstyle="round")
// MAGIC         ax.plot([d["lo"], d["lo"]], [i - 0.1, i + 0.1], color=color, linewidth=2.5)
// MAGIC         ax.plot([d["hi"], d["hi"]], [i - 0.1, i + 0.1], color=color, linewidth=2.5)
// MAGIC         ax.scatter(d["delta"], i, color=color, s=90, zorder=5, edgecolors="white", linewidths=1)
// MAGIC         ax.annotate(f"{d['delta']:+.2f}%", (d["delta"], i), textcoords="offset points",
// MAGIC                     xytext=(0, 14), ha="center", fontsize=11, fontweight="bold", color=color)
// MAGIC         ax.annotate(f"{d['lo']:+.2f}%", (d["lo"], i), textcoords="offset points",
// MAGIC                     xytext=(0, -16), ha="center", fontsize=9, color=color, alpha=0.75)
// MAGIC         ax.annotate(f"{d['hi']:+.2f}%", (d["hi"], i), textcoords="offset points",
// MAGIC                     xytext=(0, -16), ha="center", fontsize=9, color=color, alpha=0.75)
// MAGIC
// MAGIC     ax.axvline(0, color="#1E293B", linewidth=1, linestyle="--", alpha=0.6)
// MAGIC     ax.set_yticks(y_pos)
// MAGIC     ax.set_yticklabels(metric_names, fontsize=12)
// MAGIC     ax.set_xlabel("Δ Relative (%)", fontsize=11)
// MAGIC     ax.xaxis.set_major_formatter(mtick.FormatStrFormatter("%.0f%%"))
// MAGIC     ax.invert_yaxis()
// MAGIC     ax.spines["top"].set_visible(False)
// MAGIC     ax.spines["right"].set_visible(False)
// MAGIC     ax.grid(axis="x", alpha=0.2)
// MAGIC     ax.set_title(title, fontsize=14, fontweight="bold", pad=14)
// MAGIC
// MAGIC     legend_elements = [
// MAGIC         Line2D([0], [0], marker="o", color=colors[True], label="Stat-sig", markersize=8, linewidth=3),
// MAGIC         Line2D([0], [0], marker="o", color=colors[False], label="Non-stat-sig", markersize=8, linewidth=3),
// MAGIC     ]
// MAGIC     fig.legend(handles=legend_elements, loc="lower center", ncol=2, fontsize=10,
// MAGIC               frameon=False, bbox_to_anchor=(0.5, -0.05))
// MAGIC     fig.subplots_adjust(bottom=0.25)
// MAGIC     return fig, ax
// MAGIC
// MAGIC # ---------------------------------------------------------------------------
// MAGIC # Data from ccDefaultMetrics (cell above)
// MAGIC # ---------------------------------------------------------------------------
// MAGIC metrics = {
// MAGIC     "Core": {
// MAGIC         "Δ Unit Risk Rate":       {"delta": 4.24, "lo": 3.69, "hi": 4.80, "sig": True},
// MAGIC         "Δ Balance Risk Ratio":   {"delta": 1.41, "lo": 0.27, "hi": 2.54, "sig": True},
// MAGIC     },
// MAGIC     "HI": {
// MAGIC         "Δ Unit Risk Rate":       {"delta": 1.37, "lo": 0.08, "hi": 2.67, "sig": True},
// MAGIC         "Δ Balance Risk Ratio":   {"delta": 1.51, "lo": -0.91, "hi": 3.92, "sig": False},
// MAGIC     },
// MAGIC }
// MAGIC metric_names = ["Δ Unit Risk Rate", "Δ Balance Risk Ratio"]

// COMMAND ----------

// DBTITLE 1,View Core
// MAGIC %python
// MAGIC fig, ax = forest_plot("Core", metrics["Core"], metric_names,
// MAGIC                       "CC Default Rate — Core — H2 2025")
// MAGIC plt.show()

// COMMAND ----------

// DBTITLE 1,View HI
// MAGIC %python
// MAGIC fig, ax = forest_plot("HI", metrics["HI"], metric_names,
// MAGIC                       "CC Default Rate — HI — H2 2025")
// MAGIC plt.show()

// COMMAND ----------

// DBTITLE 1,val tableOverallMetrics
// MAGIC %md
// MAGIC ## Sanity Checks

// COMMAND ----------

// DBTITLE 1,Unique defaulted customers
// Sanity check: total rows vs unique customers per segment
// If total_rows > unique_customers → multiple bills per customer (expected)

val pipelineFunnel = defaultWithHoldoutTable
  .groupBy("segmento")
  .agg(
    count("*").as("total_rows"),
    countDistinct("customer__id").as("unique_customers"),
    countDistinct(when($"defaults_now" === true, $"customer__id")).as("unique_customers_defaulted"))

display(pipelineFunnel)

// COMMAND ----------

// DBTITLE 1,Sanity: bills per customer
// Explore source of duplication: how many rows per customer?
// Hypothesis: multiple bill__due_date per customer in H2 2025

val defaultData = spark.table("usr.br__customer_lifecycle_optimization.join_default_v2")

// 1. Distribution of rows per customer
val billsPerCustomer = defaultData
  .groupBy("segmento", "customer__id")
  .agg(
    count("*").as("n_bills"),
    countDistinct("bill__due_date").as("n_distinct_bill_dates"),
    countDistinct("account__id").as("n_accounts"))

// Summary: min, avg, max bills per customer
val billsSummary = billsPerCustomer
  .groupBy("segmento")
  .agg(
    count("*").as("total_customers"),
    round(avg("n_bills"), 2).as("avg_bills"),
    min("n_bills").as("min_bills"),
    max("n_bills").as("max_bills"),
    round(avg("n_distinct_bill_dates"), 2).as("avg_distinct_dates"),
    sum(when($"n_accounts" > 1, 1).otherwise(0)).as("customers_multi_account"))

display(billsSummary)

// COMMAND ----------

// DBTITLE 1,Unit Risk & Balance Risk
// Pipeline funnel: total rows and unique customers per segment
// If total_rows > unique_customers → bills not fully collapsed by calcMetrics

val pipelineFunnel = defaultWithHoldout
  .groupBy("segmento")
  .agg(
    count("*").as("total_rows"),
    countDistinct("customer__id").as("unique_customers"),
    countDistinct("account__id").as("unique_accounts"))

display(pipelineFunnel)

// COMMAND ----------

val sanityFlagOverlap = defaultBase
  .withColumn("flag_count",
    when($"secured_card_flag" === true, 1).otherwise(0) +
    when($"republish_flag" === true, 1).otherwise(0) +
    when($"clipublish_flag" === true, 1).otherwise(0))
  .filter($"flag_count" > 1)

display(sanityFlagOverlap)