// Databricks notebook source
// MAGIC %md
// MAGIC    
// MAGIC # CLM Value Proof — RAM Deep Dive | Global Holdout 2025
// MAGIC
// MAGIC **Objetivo**: Quantificar o impacto econômico de **CLM** (Customer Lifecycle Management) usando o **Global Holdout 2025** como grupo de controle, medido via **RAM** (Revenue Anticipated at Origination).
// MAGIC
// MAGIC **Método**:
// MAGIC 1. Separar clientes em **Test** (4.66M, expostos a CLM) vs **Control** (4.66M, holdout)
// MAGIC 2. Cruzar com RAM por `customer__id` e mês, considerando apenas datas ≥ primeira exposição de cada cliente ao experimento
// MAGIC 3. Calcular uplift (Test − Control) com IC 95% e teste de significância (Welch's t-test)
// MAGIC
// MAGIC **Estrutura da análise** — drill-down progressivo:
// MAGIC
// MAGIC | Seção | Métrica | Breakdown |
// MAGIC | --- | --- | --- |
// MAGIC | 3 | **Total RAM** | CC, Lending, Conta, NuPay |
// MAGIC | 4 | **CC** | CC Financing, CC Interchange, (−) Expected Losses |
// MAGIC | 4.1 | CC Financing | Transactional, Bill Financing |
// MAGIC | 4.2 | Transactional Fin. | Cash In, Boleto, Purchase, Pix |
// MAGIC | 4.3 | Bill Financing | Refinancing, Mandatory Default, Revolving, Late, ... |
// MAGIC | 4.4 | CC Interchange | Leaf |
// MAGIC | 4.5 | CC PV | Leaf (present value) |
// MAGIC | 5 | **Lending** | Unsecured, Secured |
// MAGIC | 5.1 | Unsecured Lending | Leaf |
// MAGIC | 5.2 | Secured Lending | Leaf |
// MAGIC | 5.3 | Unsecured Principal | Leaf (volume originado) |
// MAGIC | 5.4 | Secured Principal | Leaf (volume originado) |
// MAGIC | 6 | **Conta** | Money Boxes, Conta Interest, Locked Deposits, Debit Interchange |
// MAGIC | 7 | **NuPay** | NuPay CC Interchange, NuPay Debit Interchange, NuPay PCJ |
// MAGIC
// MAGIC **Fontes**:
// MAGIC - `usr.br__customer_lifecycle_optimization.financial_framework_main_table_last_saved_version` — RAM at Origination
// MAGIC - `usr.luisabeltramini.global_holdout_2025` — Test/Control do Global Holdout 2025

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Setup

// COMMAND ----------

// DBTITLE 1,Import
// MAGIC %run "/data-analysts/utils-uc"

// COMMAND ----------

// DBTITLE 1,Sources
val ramTable = spark.table("usr.br__customer_lifecycle_optimization.financial_framework_main_table_last_saved_version")
val globalHoldout = spark.table("usr.luisabeltramini.global_holdout_2025")
  .withColumnRenamed("global_holdout", "variant")
  .withColumn("exposition_date", lit("2025-03-01").cast("date")) // como não tinha data de exposição, colocamos o início do holdout.

// COMMAND ----------

// DBTITLE 1,apagar
ramTable.d

// COMMAND ----------

// DBTITLE 1,Product Hierarchy Definitions
// ── Hierarquia de Produtos do RAM ──
// Cada val é Seq[(alias, colunaOrigem)] passável diretamente como subProducts em computeMetricUplift.
//
// total_anticipated_npv
// ├── cc_anticipated_npv
// │   ├── cc_financing_anticipated_npv
// │   │   ├── transactional_financing_booked_interest
// │   │   │   ├── cash_in_financing_booked_interest
// │   │   │   ├── boleto_payment_booked_interest
// │   │   │   ├── purchase_financing_booked_interest
// │   │   │   └── pix_payment_booked_interest
// │   │   └── bill_financing_booked_interest
// │   │       ├── refinancing_booked_interest
// │   │       ├── mandatory_default_booked_interest
// │   │       ├── revolving_booked_interest
// │   │       ├── late_booked_interest
// │   │       ├── mandatory_custom_booked_interest
// │   │       ├── voluntary_booked_interest
// │   │       └── late_fee_value
// │   ├── cc_interchange (= mc_cc_interchange_amount)
// │   └── (−) sl_expected_losses  ⚠ custo — subtraído na composição
// ├── lending_anticipated_npv
// │   ├── unsecured_ram_prediction_lending
// │   ├── secured_net_present_value_lending
// │   └── pj_net_present_value_lending
// ├── conta_anticipated_npv
// │   ├── money_boxes_net_income_interest
// │   ├── conta_net_income_interest
// │   ├── locked_deposits_net_income_interest
// │   └── mc_debit_interchange_amount
// └── nupay_anticipated_npv
//     ├── nupay_cc_interchange_amount
//     ├── nupay_debit_interchange_amount
//     └── nupay_pcj_income

// ═══════════════════════════════════════════
// L1 — RAM Total → 4 produtos
// ═══════════════════════════════════════════
val ramProducts = Seq(
  "cc"      -> "cc_anticipated_npv",
  "lending" -> "lending_anticipated_npv",
  "conta"   -> "conta_anticipated_npv",
  "nupay"   -> "nupay_anticipated_npv"
)

// ═══════════════════════════════════════════
// L2 — Sub-produtos de cada produto
// ═══════════════════════════════════════════

// CC → Financing + Interchange − Expected Losses
val ccSubProducts = Seq(
  "cc_financing"       -> "cc_financing_anticipated_npv",
  "cc_interchange"     -> "cc_interchange",
  "sl_expected_losses" -> "sl_expected_losses"
)

// Lending → Unsecured + Secured + PJ
val lendingSubProducts = Seq(
  "unsecured" -> "unsecured_ram_prediction_lending",
  "secured"   -> "secured_net_present_value_lending"
  // ,"pj"        -> "pj_net_present_value_lending"
)

// Conta → Money Boxes + Conta Interest + Locked Deposits + Debit Interchange
val contaSubProducts = Seq(
  "money_boxes"       -> "money_boxes_net_income_interest",
  "conta_interest"    -> "conta_net_income_interest",
  "locked_deposits"   -> "locked_deposits_net_income_interest",
  "debit_interchange" -> "mc_debit_interchange_amount"
)

// NuPay → CC Interchange + Debit Interchange + PCJ
val nupaySubProducts = Seq(
  "nupay_cc_interchange"    -> "nupay_cc_interchange_amount",
  "nupay_debit_interchange" -> "nupay_debit_interchange_amount",
  "nupay_pcj"               -> "nupay_pcj_income"
)

// ═══════════════════════════════════════════
// L3 — CC Financing breakdown
// ═══════════════════════════════════════════
val ccFinancingSubProducts = Seq(
  "transactional_financing" -> "transactional_financing_booked_interest",
  "bill_financing"          -> "bill_financing_booked_interest"
)

// Transactional Financing breakdown
val transactionalFinancingSubProducts = Seq(
  "cash_in"  -> "cash_in_financing_booked_interest",
  "boleto"   -> "boleto_payment_booked_interest",
  "purchase" -> "purchase_financing_booked_interest",
  "pix"      -> "pix_payment_booked_interest"
)

// Bill Financing breakdown
val billFinancingSubProducts = Seq(
  "refinancing"       -> "refinancing_booked_interest",
  "mandatory_default" -> "mandatory_default_booked_interest",
  "revolving"         -> "revolving_booked_interest",
  "late"              -> "late_booked_interest",
  "mandatory_custom"  -> "mandatory_custom_booked_interest",
  "voluntary"         -> "voluntary_booked_interest",
  "late_fee"          -> "late_fee_value"
)

// COMMAND ----------

// DBTITLE 1,RAM TxC
case class RamUpliftResult(
  cumResults:     DataFrame,
  cumUplift:      DataFrame,
  periodicResults: DataFrame,
  periodicUplift:  DataFrame
)

/**
 * Computa uplift TxC genérico para qualquer métrica do RAM.
 *
 * @param mainMetric   coluna principal do RAM (e.g. "total_anticipated_npv", "lending_anticipated_npv")
 * @param subProducts  breakdown: Seq de (alias, colunaOrigem) — gera colunas uplift_{alias}
 * @param timeGrain    "month" (default) ou "day" para análise diária
 * @param endDate      data fim opcional (formato "yyyy-MM-dd")
 */
def computeMetricUplift(
  exposureTable: DataFrame,
  ramSource:     DataFrame,
  rolloutDate:   String,
  viewPrefix:    String,
  mainMetric:    String,
  subProducts:   Seq[(String, String)] = Seq.empty,
  timeGrain:     String = "day",
  endDate:       String = ""
): RamUpliftResult = {

  // 1. Exposição: primeira data de exposição por cliente dentro do período
  var exposureFiltered = exposureTable.filter($"exposition_date" >= rolloutDate)
  if (endDate.nonEmpty) exposureFiltered = exposureFiltered.filter($"exposition_date" <= endDate)
  val exposureWithDate = exposureFiltered
    .groupBy($"customer__id", $"variant")
    .agg(min($"exposition_date").as("first_exposition_date"))

  // 2. Filtrar RAM por período
  var ramFiltered = ramSource.filter($"date" >= rolloutDate)
  if (endDate.nonEmpty) ramFiltered = ramFiltered.filter($"date" <= endDate)

  // 3. Expressão de período: usa date diretamente ou trunca para mês
  val timeExpr = if (timeGrain == "day") col("date") else date_trunc("month", col("date")).cast("date")

  // 4. Join + filtrar apenas datas >= primeira exposição + Agregar por cliente/período/variante
  val aggExprs = Seq(sum(col(mainMetric)).as("metric_total")) ++
    subProducts.map { case (alias, srcCol) => sum(col(srcCol)).as(alias) }

  val periodicRaw = ramFiltered
    .join(exposureWithDate, Seq("customer__id"), "inner")
    .filter($"date" >= $"first_exposition_date")
    .groupBy($"customer__id", timeExpr.as("period"), $"variant")
    .agg(aggExprs.head, aggExprs.tail: _*)

  // 5. Cumulativo por cliente
  val wCum = Window.partitionBy($"customer__id", $"variant").orderBy($"period")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  var cumRaw = periodicRaw.withColumn("cum_metric_total", sum($"metric_total").over(wCum))
  for ((alias, _) <- subProducts) {
    cumRaw = cumRaw.withColumn(s"cum_$alias", sum(col(alias)).over(wCum))
  }

  // 6. Agregar por período/variante + Calcular uplift TxC
  def aggregateAndUplift(
    source: DataFrame, metricCol: String, subCols: Seq[(String, String)]
  ): (DataFrame, DataFrame) = {

    val subAvgs = subCols.map { case (alias, srcCol) => avg(col(srcCol)).as(s"avg_$alias") }
    val allAggs = Seq(
      countDistinct($"customer__id").as("n_customers"),
      avg(col(metricCol)).as("avg_ram"),
      stddev(col(metricCol)).as("std_ram")
    ) ++ subAvgs

    val results = source.groupBy($"period", $"variant")
      .agg(allAggs.head, allAggs.tail: _*)
      .withColumn("se", $"std_ram" / sqrt($"n_customers"))
      .withColumn("moe_95", lit(1.96) * $"se")
      .orderBy($"period", $"variant")

    // Pivotar Treatment vs Control
    val tSubs = subCols.map { case (alias, _) => col(s"avg_$alias").as(s"avg_${alias}_t") }
    val treatment = results.filter($"variant" =!= "control")
      .select((Seq(
        $"period", $"n_customers".as("n_t"),
        $"avg_ram".as("avg_ram_t"), $"std_ram".as("std_ram_t")
      ) ++ tSubs): _*)

    val cSubs = subCols.map { case (alias, _) => col(s"avg_$alias").as(s"avg_${alias}_c") }
    val control = results.filter($"variant" === "control")
      .select((Seq(
        $"period", $"n_customers".as("n_c"),
        $"avg_ram".as("avg_ram_c"), $"std_ram".as("std_ram_c")
      ) ++ cSubs): _*)

    // Uplift com IC 95% (Welch's t-test)
    var uplift = treatment.join(control, Seq("period"), "inner")
      .withColumn("uplift", $"avg_ram_t" - $"avg_ram_c")
      .withColumn("se_uplift", sqrt(pow($"std_ram_t", 2) / $"n_t" + pow($"std_ram_c", 2) / $"n_c"))
      .withColumn("moe_95", lit(1.96) * $"se_uplift")
      .withColumn("uplift_pct",
        when($"avg_ram_c" =!= 0, ($"uplift" / abs($"avg_ram_c")) * 100).otherwise(lit(null)))
      .withColumn("ci_lower", $"uplift" - $"moe_95")
      .withColumn("ci_upper", $"uplift" + $"moe_95")
      .withColumn("stat_sig",
        when($"ci_lower" > 0 || $"ci_upper" < 0, lit("YES")).otherwise(lit("NO")))

    for ((alias, _) <- subCols) {
      uplift = uplift.withColumn(s"uplift_$alias", col(s"avg_${alias}_t") - col(s"avg_${alias}_c"))
    }

    (results, uplift.orderBy($"period"))
  }

  // Periódico: usa métricas por período
  val periodicSubCols = subProducts.map { case (alias, _) => (alias, alias) }
  val (periodicResults, periodicUplift) = aggregateAndUplift(periodicRaw, "metric_total", periodicSubCols)

  // Acumulado: usa métricas cumulativas
  val cumSubCols = subProducts.map { case (alias, _) => (alias, s"cum_$alias") }
  val (cumResults, cumUplift) = aggregateAndUplift(cumRaw, "cum_metric_total", cumSubCols)

  // Registrar temp views para gráficos Python
  cumUplift.createOrReplaceTempView(s"${viewPrefix}_cum_uplift")
  periodicUplift.createOrReplaceTempView(s"${viewPrefix}_periodic_uplift")

  RamUpliftResult(cumResults, cumUplift, periodicResults, periodicUplift)
}

// COMMAND ----------

// DBTITLE 1,Plot Charts
// MAGIC %python
// MAGIC        
// MAGIC import matplotlib.pyplot as plt
// MAGIC import matplotlib.dates as mdates
// MAGIC import matplotlib.gridspec as gridspec
// MAGIC import matplotlib.ticker as mticker
// MAGIC import pandas as pd
// MAGIC import numpy as np
// MAGIC
// MAGIC # Aliases conhecidos para display names bonitos
// MAGIC _DISPLAY_NAMES = {
// MAGIC     "cc": "CC", "pj": "PJ", "nupay": "NuPay",
// MAGIC     "secured_npv": "Secured NPV",
// MAGIC     "cc_financing": "CC Financing", "cc_interchange": "CC Interchange",
// MAGIC     "sl_expected_losses": "Expected Losses SL (custo)",
// MAGIC     "nupay_cc_interchange": "NuPay CC Interchange",
// MAGIC     "nupay_debit_interchange": "NuPay Debit Interchange",
// MAGIC     "nupay_pcj": "NuPay PCJ",
// MAGIC }
// MAGIC _DEFAULT_PALETTE = ["#820AD1", "#E85D04", "#0D9488", "#2563EB", "#7C3AED", "#DC2626", "#0369A1", "#B45309"]
// MAGIC
// MAGIC def _format_alias(alias):
// MAGIC     if alias in _DISPLAY_NAMES:
// MAGIC         return _DISPLAY_NAMES[alias]
// MAGIC     return alias.replace("_", " ").title()
// MAGIC
// MAGIC def plot_uplift_charts(
// MAGIC     cum_view, periodic_view, experiment_name,
// MAGIC     color="#820AD1",
// MAGIC     metric_label="RAM",
// MAGIC     products=None, prod_cols=None, prod_colors=None
// MAGIC ):
// MAGIC     """
// MAGIC     Comprehensive uplift dashboard. Auto-detects daily vs monthly.
// MAGIC     When products is None, auto-detects from uplift_* columns in the view.
// MAGIC     When no products are found, contribution charts/tables are skipped.
// MAGIC     """
// MAGIC     # ── Load data ──
// MAGIC     cum      = spark.table(cum_view).toPandas()
// MAGIC     periodic = spark.table(periodic_view).toPandas()
// MAGIC
// MAGIC     # Auto-detect products from view columns if not provided
// MAGIC     if products is None:
// MAGIC         skip = {"uplift", "uplift_pct"}
// MAGIC         prod_cols = [c for c in cum.columns if c.startswith("uplift_") and c not in skip]
// MAGIC         products  = [_format_alias(c.replace("uplift_", "")) for c in prod_cols]
// MAGIC         prod_colors = [_DEFAULT_PALETTE[i % len(_DEFAULT_PALETTE)] for i in range(len(products))]
// MAGIC
// MAGIC     has_products = bool(products)
// MAGIC
// MAGIC     num_cols = ["uplift", "ci_lower", "ci_upper", "uplift_pct",
// MAGIC                 "avg_ram_t", "avg_ram_c", "std_ram_t", "std_ram_c"] + (prod_cols or [])
// MAGIC     for df in [cum, periodic]:
// MAGIC         df["period"] = pd.to_datetime(df["period"])
// MAGIC         for c in num_cols:
// MAGIC             if c in df.columns:
// MAGIC                 df[c] = df[c].astype(float)
// MAGIC         df["n_t"] = df["n_t"].astype(float).astype(int)
// MAGIC         df["n_c"] = df["n_c"].astype(float).astype(int)
// MAGIC
// MAGIC     # ── Auto-detect granularity ──
// MAGIC     if len(cum) > 1:
// MAGIC         avg_gap = (cum["period"].max() - cum["period"].min()).days / (len(cum) - 1)
// MAGIC         is_daily = avg_gap < 15
// MAGIC     else:
// MAGIC         is_daily = False
// MAGIC
// MAGIC     date_fmt     = mdates.DateFormatter("%d/%m" if is_daily else "%b/%y")
// MAGIC     bar_w        = 0.8 if is_daily else 20
// MAGIC     bar_w_td     = pd.Timedelta(days=0.8) if is_daily else pd.Timedelta(days=15)
// MAGIC     period_label = "Dia" if is_daily else "Mês"
// MAGIC     tbl_date_fmt = "%d/%m" if is_daily else "%b/%y"
// MAGIC
// MAGIC     def fmt_n(x):
// MAGIC         if x >= 1e6: return f"{x/1e6:.2f}M"
// MAGIC         if x >= 1e3: return f"{x/1e3:.0f}K"
// MAGIC         return f"{x:.0f}"
// MAGIC
// MAGIC     # ── Dynamic grid layout ──
// MAGIC     if has_products:
// MAGIC         n_rows = 8
// MAGIC         height_ratios = [0.7, 1, 0.9, 0.35, 1, 1, 0.9, 0.35]
// MAGIC         fig_h = 44
// MAGIC     else:
// MAGIC         n_rows = 4
// MAGIC         height_ratios = [0.7, 1, 1, 1]
// MAGIC         fig_h = 24
// MAGIC
// MAGIC     fig = plt.figure(figsize=(20, fig_h))
// MAGIC     gs  = gridspec.GridSpec(n_rows, 2, figure=fig,
// MAGIC                             height_ratios=height_ratios,
// MAGIC                             hspace=0.40, wspace=0.3)
// MAGIC     fig.suptitle(f"{experiment_name} — {metric_label} Uplift (Treatment − Control)",
// MAGIC                  fontsize=18, fontweight="bold", y=1.0)
// MAGIC
// MAGIC     # ═ Row 0 ─ Clientes TxC (full width)
// MAGIC     ax = fig.add_subplot(gs[0, :])
// MAGIC     ax.plot(cum["period"], cum["n_t"], marker="o", color=color,
// MAGIC             linewidth=2, label="Treatment")
// MAGIC     ax.plot(cum["period"], cum["n_c"], marker="s", color="gray",
// MAGIC             linewidth=2, label="Control")
// MAGIC     for _, row in cum.iterrows():
// MAGIC         ax.annotate(fmt_n(row["n_t"]), (row["period"], row["n_t"]),
// MAGIC                     textcoords="offset points", xytext=(0, 8),
// MAGIC                     fontsize=7, ha="center", color=color, fontweight="bold")
// MAGIC         ax.annotate(fmt_n(row["n_c"]), (row["period"], row["n_c"]),
// MAGIC                     textcoords="offset points", xytext=(0, -12),
// MAGIC                     fontsize=7, ha="center", color="gray", fontweight="bold")
// MAGIC     ax.set_title(f"Clientes por Variante (por {period_label.lower()})", fontsize=14, fontweight="bold")
// MAGIC     ax.set_ylabel("# Clientes")
// MAGIC     ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_n(x)))
// MAGIC     ax.legend(fontsize=10)
// MAGIC     ax.xaxis.set_major_formatter(date_fmt)
// MAGIC     ax.tick_params(axis="x", rotation=45)
// MAGIC     ax.grid(axis="y", alpha=0.3)
// MAGIC     ymin, ymax = ax.get_ylim()
// MAGIC     margin = (ymax - ymin) * 0.12
// MAGIC     ax.set_ylim(ymin - margin, ymax + margin)
// MAGIC
// MAGIC     # ── Helper: uplift R$ ──
// MAGIC     def plot_uplift_rs(ax, df, title, marker="o", line_color=color):
// MAGIC         ax.plot(df["period"], df["uplift"], marker=marker, color=line_color,
// MAGIC                 linewidth=2, label="Uplift")
// MAGIC         ax.fill_between(df["period"], df["ci_lower"], df["ci_upper"],
// MAGIC                         alpha=0.18, color=line_color, label="IC 95%")
// MAGIC         ax.axhline(0, color="gray", linestyle="--", linewidth=0.8)
// MAGIC         ax.set_title(title, fontsize=13, fontweight="bold")
// MAGIC         ax.set_ylabel("R$ (avg per customer)")
// MAGIC         ax.legend(fontsize=9)
// MAGIC         ax.xaxis.set_major_formatter(date_fmt)
// MAGIC         ax.tick_params(axis="x", rotation=45)
// MAGIC         ax.grid(axis="y", alpha=0.2)
// MAGIC         for _, row in df.iterrows():
// MAGIC             val = row["uplift"]
// MAGIC             sig = row["stat_sig"] == "YES"
// MAGIC             lbl = f"R${val:+.2f}" + (" *" if sig else "")
// MAGIC             ax.annotate(lbl, (row["period"], val),
// MAGIC                         textcoords="offset points",
// MAGIC                         xytext=(0, 12 if val >= 0 else -14),
// MAGIC                         fontsize=7, ha="center",
// MAGIC                         color="#16A34A" if sig else "#333",
// MAGIC                         fontweight="bold" if sig else "normal")
// MAGIC
// MAGIC     # ── Helper: uplift % ──
// MAGIC     def plot_uplift_pct(ax, df, title):
// MAGIC         bar_colors = [color if v >= 0 else "#DC2626" for v in df["uplift_pct"]]
// MAGIC         ax.bar(df["period"], df["uplift_pct"], width=bar_w,
// MAGIC                color=bar_colors, alpha=0.7)
// MAGIC         ax.axhline(0, color="gray", linestyle="--", linewidth=0.8)
// MAGIC         ax.set_title(title, fontsize=13, fontweight="bold")
// MAGIC         ax.set_ylabel("Uplift %")
// MAGIC         ax.xaxis.set_major_formatter(date_fmt)
// MAGIC         ax.tick_params(axis="x", rotation=45)
// MAGIC         ax.grid(axis="y", alpha=0.2)
// MAGIC         for _, row in df.iterrows():
// MAGIC             val = row["uplift_pct"]
// MAGIC             if not np.isnan(val):
// MAGIC                 ax.text(row["period"],
// MAGIC                         val + (0.03 if val >= 0 else -0.03),
// MAGIC                         f"{val:+.2f}%", ha="center",
// MAGIC                         va="bottom" if val >= 0 else "top", fontsize=7.5)
// MAGIC
// MAGIC     # ── Helper: product contribution (stacked bars, sem labels) ──
// MAGIC     def plot_product_contribution(ax, df, title):
// MAGIC         pos_bottom = np.zeros(len(df))
// MAGIC         neg_bottom = np.zeros(len(df))
// MAGIC         for prod, col_name, pc in zip(products, prod_cols, prod_colors):
// MAGIC             vals = df[col_name].values.astype(float)
// MAGIC             pos = np.where(vals > 0, vals, 0)
// MAGIC             neg = np.where(vals < 0, vals, 0)
// MAGIC             ax.bar(df["period"], pos, width=bar_w_td, bottom=pos_bottom,
// MAGIC                    label=prod, color=pc, alpha=0.8)
// MAGIC             ax.bar(df["period"], neg, width=bar_w_td, bottom=neg_bottom,
// MAGIC                    color=pc, alpha=0.8)
// MAGIC             pos_bottom += pos
// MAGIC             neg_bottom += neg
// MAGIC         ax.axhline(0, color="gray", linestyle="--", linewidth=0.8)
// MAGIC         ax.set_title(title, fontsize=13, fontweight="bold")
// MAGIC         ax.set_ylabel("R$ (avg per customer)")
// MAGIC         if products:
// MAGIC             ax.legend(fontsize=9, loc="best", ncol=len(products))
// MAGIC         ax.xaxis.set_major_formatter(date_fmt)
// MAGIC         ax.tick_params(axis="x", rotation=45)
// MAGIC         ax.grid(axis="y", alpha=0.2)
// MAGIC         ymin, ymax = ax.get_ylim()
// MAGIC         margin = (ymax - ymin) * 0.10
// MAGIC         ax.set_ylim(ymin - margin, ymax + margin)
// MAGIC
// MAGIC     # ── Helper: contribution table (dias = colunas, produtos = linhas) ──
// MAGIC     def plot_contribution_table(ax, df):
// MAGIC         ax.axis("off")
// MAGIC         if not products:
// MAGIC             return
// MAGIC         col_headers = df["period"].dt.strftime(tbl_date_fmt).tolist()
// MAGIC         cell_text = []
// MAGIC         row_colors_list = []
// MAGIC         for prod, col_name, pc in zip(products, prod_cols, prod_colors):
// MAGIC             vals = df[col_name].values.astype(float)
// MAGIC             row = [f"R${v:+.4f}" if abs(v) < 0.01 else f"R${v:+.3f}" for v in vals]
// MAGIC             cell_text.append(row)
// MAGIC             row_colors_list.append(pc)
// MAGIC         # Total row
// MAGIC         totals = df["uplift"].values.astype(float)
// MAGIC         cell_text.append([f"R${v:+.4f}" if abs(v) < 0.01 else f"R${v:+.3f}" for v in totals])
// MAGIC         row_labels = products + ["Total"]
// MAGIC         row_colors_list.append("#333333")
// MAGIC
// MAGIC         table = ax.table(
// MAGIC             cellText=cell_text,
// MAGIC             rowLabels=row_labels,
// MAGIC             colLabels=col_headers,
// MAGIC             loc="center",
// MAGIC             cellLoc="center"
// MAGIC         )
// MAGIC         table.auto_set_font_size(False)
// MAGIC         table.set_fontsize(7)
// MAGIC         table.scale(1, 1.4)
// MAGIC         # Style: header row
// MAGIC         for j in range(len(col_headers)):
// MAGIC             table[0, j].set_facecolor("#E5E7EB")
// MAGIC             table[0, j].set_text_props(fontweight="bold")
// MAGIC         # Style: row labels + cell coloring
// MAGIC         for i in range(len(row_labels)):
// MAGIC             table[i + 1, -1].set_text_props(fontweight="bold", color=row_colors_list[i])
// MAGIC             for j in range(len(col_headers)):
// MAGIC                 val = float(cell_text[i][j].replace("R$", "").replace("+", ""))
// MAGIC                 if i < len(products):
// MAGIC                     table[i + 1, j].set_facecolor("#F0FDF4" if val >= 0 else "#FEF2F2")
// MAGIC                 else:  # Total row
// MAGIC                     table[i + 1, j].set_facecolor("#D1FAE5" if val >= 0 else "#FECACA")
// MAGIC                     table[i + 1, j].set_text_props(fontweight="bold")
// MAGIC
// MAGIC     # ── Helper: TxC curves with CI ──
// MAGIC     def plot_txc_curves(ax, df, title):
// MAGIC         se_t = df["std_ram_t"].values / np.sqrt(df["n_t"].values.astype(float))
// MAGIC         se_c = df["std_ram_c"].values / np.sqrt(df["n_c"].values.astype(float))
// MAGIC         ax.plot(df["period"], df["avg_ram_t"], marker="o", color=color,
// MAGIC                 linewidth=2, label="Treatment", zorder=3)
// MAGIC         ax.fill_between(df["period"],
// MAGIC                         df["avg_ram_t"].values - 1.96 * se_t,
// MAGIC                         df["avg_ram_t"].values + 1.96 * se_t,
// MAGIC                         alpha=0.20, color=color, label="IC 95% Treatment", zorder=2)
// MAGIC         ax.plot(df["period"], df["avg_ram_c"], marker="s", color="#555555",
// MAGIC                 linewidth=2, label="Control", zorder=3)
// MAGIC         ax.fill_between(df["period"],
// MAGIC                         df["avg_ram_c"].values - 1.96 * se_c,
// MAGIC                         df["avg_ram_c"].values + 1.96 * se_c,
// MAGIC                         alpha=0.25, color="#AAAAAA", hatch="//",
// MAGIC                         label="IC 95% Control", zorder=1)
// MAGIC         ax.set_title(title, fontsize=14, fontweight="bold")
// MAGIC         ax.set_ylabel("R$ (avg per customer)")
// MAGIC         ax.legend(fontsize=10, loc="upper left")
// MAGIC         ax.xaxis.set_major_formatter(date_fmt)
// MAGIC         ax.tick_params(axis="x", rotation=45)
// MAGIC         ax.grid(axis="y", alpha=0.2)
// MAGIC         for _, row in df.iterrows():
// MAGIC             ax.annotate(f"R${row['avg_ram_t']:.2f}", (row["period"], row["avg_ram_t"]),
// MAGIC                         textcoords="offset points", xytext=(0, 10),
// MAGIC                         fontsize=7, ha="center", color=color, fontweight="bold")
// MAGIC             ax.annotate(f"R${row['avg_ram_c']:.2f}", (row["period"], row["avg_ram_c"]),
// MAGIC                         textcoords="offset points", xytext=(0, -14),
// MAGIC                         fontsize=7, ha="center", color="#555555", fontweight="bold")
// MAGIC
// MAGIC     # ═══════════════════════════════════════
// MAGIC     # Layout with products (8 rows)
// MAGIC     # ═══════════════════════════════════════
// MAGIC     if has_products:
// MAGIC         # ═ Row 1 ─ Periodo: Uplift R$ | Uplift %
// MAGIC         plot_uplift_rs(fig.add_subplot(gs[1, 0]), periodic,
// MAGIC                        f"Uplift {metric_label} por {period_label} (R$)", marker="s", line_color="#E85D04")
// MAGIC         plot_uplift_pct(fig.add_subplot(gs[1, 1]), periodic,
// MAGIC                         f"Uplift {metric_label} por {period_label} (%)")
// MAGIC
// MAGIC         # ═ Row 2 ─ Periodo: Contribuição por Produto (chart)
// MAGIC         plot_product_contribution(fig.add_subplot(gs[2, :]), periodic,
// MAGIC                                   f"Contribuição por Produto — Uplift por {period_label}")
// MAGIC
// MAGIC         # ═ Row 3 ─ Periodo: Tabela de contribuição
// MAGIC         plot_contribution_table(fig.add_subplot(gs[3, :]), periodic)
// MAGIC
// MAGIC         # ═ Row 4 ─ Acumulado: TxC com IC (full width)
// MAGIC         plot_txc_curves(fig.add_subplot(gs[4, :]), cum,
// MAGIC                         f"{metric_label} Acumulado — Treatment vs Control (IC 95%)")
// MAGIC
// MAGIC         # ═ Row 5 ─ Acumulado: Uplift R$ | Uplift %
// MAGIC         plot_uplift_rs(fig.add_subplot(gs[5, 0]), cum,
// MAGIC                        f"Uplift {metric_label} Acumulado (R$)")
// MAGIC         plot_uplift_pct(fig.add_subplot(gs[5, 1]), cum,
// MAGIC                         f"Uplift {metric_label} Acumulado (%)")
// MAGIC
// MAGIC         # ═ Row 6 ─ Acumulado: Contribuição por Produto (chart)
// MAGIC         plot_product_contribution(fig.add_subplot(gs[6, :]), cum,
// MAGIC                                   f"Contribuição por Produto — Uplift Acumulado")
// MAGIC
// MAGIC         # ═ Row 7 ─ Acumulado: Tabela de contribuição
// MAGIC         plot_contribution_table(fig.add_subplot(gs[7, :]), cum)
// MAGIC
// MAGIC     # ═══════════════════════════════════════
// MAGIC     # Layout without products (4 rows)
// MAGIC     # ═══════════════════════════════════════
// MAGIC     else:
// MAGIC         # ═ Row 1 ─ Periodo: Uplift R$ | Uplift %
// MAGIC         plot_uplift_rs(fig.add_subplot(gs[1, 0]), periodic,
// MAGIC                        f"Uplift {metric_label} por {period_label} (R$)", marker="s", line_color="#E85D04")
// MAGIC         plot_uplift_pct(fig.add_subplot(gs[1, 1]), periodic,
// MAGIC                         f"Uplift {metric_label} por {period_label} (%)")
// MAGIC
// MAGIC         # ═ Row 2 ─ Acumulado: TxC com IC (full width)
// MAGIC         plot_txc_curves(fig.add_subplot(gs[2, :]), cum,
// MAGIC                         f"{metric_label} Acumulado — Treatment vs Control (IC 95%)")
// MAGIC
// MAGIC         # ═ Row 3 ─ Acumulado: Uplift R$ | Uplift %
// MAGIC         plot_uplift_rs(fig.add_subplot(gs[3, 0]), cum,
// MAGIC                        f"Uplift {metric_label} Acumulado (R$)")
// MAGIC         plot_uplift_pct(fig.add_subplot(gs[3, 1]), cum,
// MAGIC                         f"Uplift {metric_label} Acumulado (%)")
// MAGIC
// MAGIC     plt.tight_layout(rect=[0, 0, 1, 0.98])
// MAGIC     plt.show()

// COMMAND ----------

// DBTITLE 1,Section 2 Header
// MAGIC %md
// MAGIC ## 2. Cálculo de Todos os Uplifts
// MAGIC
// MAGIC Uma única célula computa **todos os níveis de drill-down** de uma vez. Cada chamada a `computeMetricUplift` gera views temporárias (`gh_*_cum_uplift` / `gh_*_periodic_uplift`) consumidas pelas células de gráfico abaixo.
// MAGIC
// MAGIC Granularidade: **mensal** | Período: **mar/2025 em diante**

// COMMAND ----------

// DBTITLE 1,Compute All Uplifts
// ══ Parâmetros do Global Holdout 2025 ══
val rolloutDate = "2025-03-01"
val grain       = "month"

// L0: Total RAM → 4 produtos
val totalRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_total",
  "total_anticipated_npv", ramProducts, grain)

// L1: CC → Financing + Interchange − Expected Losses
val ccRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_cc",
  "cc_anticipated_npv", ccSubProducts, grain)

// L1: Lending → Unsecured + Secured
val lendingRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_lending",
  "lending_anticipated_npv", lendingSubProducts, grain)

// L1: Conta → Money Boxes + Conta Interest + Locked Deposits + Debit Interchange
val contaRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_conta",
  "conta_anticipated_npv", contaSubProducts, grain)

// L1: NuPay → CC Interchange + Debit Interchange + PCJ
val nupayRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_nupay",
  "nupay_anticipated_npv", nupaySubProducts, grain)

// L2: CC Financing → Transactional + Bill
val ccFinRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_cc_fin",
  "cc_financing_anticipated_npv", ccFinancingSubProducts, grain)

// L2: CC Interchange (leaf — sem sub-produtos)
val ccInterchangeRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_cc_interchange",
  "cc_interchange", Seq.empty, grain)

// L2: CC PV (leaf — sem sub-produtos)
val ccPvRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_cc_pv",
  "mc_cc_pv_amount", Seq.empty, grain)

// L2: Unsecured Lending (leaf — sem sub-produtos)
val unsecuredRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_unsecured",
  "unsecured_ram_prediction_lending", Seq.empty, grain)

// L2: Secured Lending (leaf — sem sub-produtos)
val securedRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_secured",
  "secured_net_present_value_lending", Seq.empty, grain)

// L2: Unsecured Principal (leaf — sem sub-produtos)
val unsecuredPrincipalRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_unsecured_principal",
  "unsecured_principal", Seq.empty, grain)

// L2: Secured Principal (leaf — sem sub-produtos)
val securedPrincipalRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_secured_principal",
  "secured_principal", Seq.empty, grain)

// L3: Transactional Financing → Cash In + Boleto + Purchase + Pix
val transFinRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_trans_fin",
  "transactional_financing_booked_interest", transactionalFinancingSubProducts, grain)

// L3: Bill Financing → Refinancing + Mandatory Default + Revolving + Late + ...
val billFinRam = computeMetricUplift(
  globalHoldout, ramTable, rolloutDate, "gh_bill_fin",
  "bill_financing_booked_interest", billFinancingSubProducts, grain)

// COMMAND ----------

// DBTITLE 1,Section 3 Header
// MAGIC %md
// MAGIC ## 3. Total RAM — Visão Geral
// MAGIC
// MAGIC Uplift no **RAM total** (`total_anticipated_npv`) com breakdown pelos 4 grandes produtos: CC, Lending, Conta e NuPay.
// MAGIC
// MAGIC Este é o nível mais alto da análise — responde à pergunta: **CLM gera mais receita por cliente?**

// COMMAND ----------

// DBTITLE 1,Total RAM: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_total_cum_uplift",
// MAGIC     "gh_total_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Total RAM",
// MAGIC     color="#820AD1"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 4 Header
// MAGIC %md
// MAGIC ## 4. Credit Card (CC)
// MAGIC
// MAGIC `cc_anticipated_npv = cc_financing_anticipated_npv + cc_interchange − sl_expected_losses`
// MAGIC
// MAGIC Analisa o uplift no produto **Cartão de Crédito**, separando a contribuição de **Financing** (juros de parcelamento/rotativo), **Interchange** (taxa sobre transações) e **(−) Expected Losses** (perdas esperadas).

// COMMAND ----------

// DBTITLE 1,CC: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_cc_cum_uplift",
// MAGIC     "gh_cc_periodic_uplift",
// MAGIC     "Global Holdout 2025 — CC",
// MAGIC     color="#820AD1",
// MAGIC     metric_label="CC"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 4.1 Header
// MAGIC %md
// MAGIC ### 4.1 CC Financing
// MAGIC
// MAGIC `cc_financing = transactional_financing + bill_financing`
// MAGIC
// MAGIC Detalha a receita de **financing do cartão**:
// MAGIC - **Transactional**: juros de parcelamento em transações (compras, Pix, boleto, cash-in)
// MAGIC - **Bill Financing**: juros sobre financiamento de fatura (rotativo, refinanciamento, atraso)

// COMMAND ----------

// DBTITLE 1,CC Financing: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_cc_fin_cum_uplift",
// MAGIC     "gh_cc_fin_periodic_uplift",
// MAGIC     "Global Holdout 2025 — CC Financing",
// MAGIC     color="#E85D04",
// MAGIC     metric_label="CC Financing"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 4.2 Header
// MAGIC %md
// MAGIC ### 4.2 Transactional Financing
// MAGIC
// MAGIC `transactional_financing = cash_in + boleto + purchase + pix`
// MAGIC
// MAGIC Receita de juros sobre **parcelamento de transações** no cartão de crédito.

// COMMAND ----------

// DBTITLE 1,Transactional Financing: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_trans_fin_cum_uplift",
// MAGIC     "gh_trans_fin_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Transactional Financing",
// MAGIC     color="#0D9488",
// MAGIC     metric_label="Transactional Financing"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 4.3 Header
// MAGIC %md
// MAGIC ### 4.3 Bill Financing
// MAGIC
// MAGIC `bill_financing = refinancing + mandatory_default + revolving + late + mandatory_custom + voluntary + late_fee`
// MAGIC
// MAGIC Receita de juros sobre **financiamento de fatura**: rotativo, refinanciamento, atraso e suas variantes.

// COMMAND ----------

// DBTITLE 1,Bill Financing: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_bill_fin_cum_uplift",
// MAGIC     "gh_bill_fin_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Bill Financing",
// MAGIC     color="#DC2626",
// MAGIC     metric_label="Bill Financing"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 4.4 Header
// MAGIC %md
// MAGIC ### 4.4 CC Interchange
// MAGIC
// MAGIC `cc_interchange = mc_cc_interchange_amount`
// MAGIC
// MAGIC Receita de **interchange do cartão de crédito** — taxa cobrada sobre transações. Métrica leaf (sem sub-produtos).

// COMMAND ----------

// DBTITLE 1,CC Interchange: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_cc_interchange_cum_uplift",
// MAGIC     "gh_cc_interchange_periodic_uplift",
// MAGIC     "Global Holdout 2025 — CC Interchange",
// MAGIC     color="#2563EB",
// MAGIC     metric_label="CC Interchange"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 4.5 Header
// MAGIC %md
// MAGIC ### 4.5 CC PV
// MAGIC
// MAGIC `mc_cc_pv_amount`
// MAGIC
// MAGIC Evolução do **present value do cartão de crédito**. Complementa a visão de receita do CC com o valor presente. Métrica leaf — sem sub-produtos.

// COMMAND ----------

// DBTITLE 1,CC PV: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_cc_pv_cum_uplift",
// MAGIC     "gh_cc_pv_periodic_uplift",
// MAGIC     "Global Holdout 2025 — CC PV",
// MAGIC     color="#7C3AED",
// MAGIC     metric_label="CC PV"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 5 Header
// MAGIC %md
// MAGIC ## 5. Lending
// MAGIC
// MAGIC `lending_anticipated_npv = unsecured_ram_prediction + secured_net_present_value`
// MAGIC
// MAGIC Uplift no produto **Empréstimos**, separando:
// MAGIC - **Unsecured**: empréstimos pessoais sem garantia
// MAGIC - **Secured**: empréstimos com garantia (ex: FGTS)

// COMMAND ----------

// DBTITLE 1,Lending: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_lending_cum_uplift",
// MAGIC     "gh_lending_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Lending",
// MAGIC     color="#E85D04",
// MAGIC     metric_label="Lending"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 5.1 Header
// MAGIC %md
// MAGIC ### 5.1 Unsecured Lending
// MAGIC
// MAGIC `unsecured_ram_prediction_lending`
// MAGIC
// MAGIC Uplift na receita de **empréstimos pessoais sem garantia** (Personal Loans). Métrica leaf — sem sub-produtos.

// COMMAND ----------

// DBTITLE 1,Unsecured Lending: Charts
// MAGIC %python
// MAGIC        
// MAGIC plot_uplift_charts(
// MAGIC     "gh_unsecured_cum_uplift",
// MAGIC     "gh_unsecured_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Unsecured Lending",
// MAGIC     color="#E85D04",
// MAGIC     metric_label="Unsecured Lending"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 5.2 Header
// MAGIC %md
// MAGIC ### 5.2 Secured Lending
// MAGIC
// MAGIC `secured_net_present_value_lending`
// MAGIC
// MAGIC Uplift na receita de **empréstimos com garantia** (ex: FGTS, veículos). Métrica leaf — sem sub-produtos.

// COMMAND ----------

// DBTITLE 1,Secured Lending: Charts
// MAGIC %python
// MAGIC        
// MAGIC plot_uplift_charts(
// MAGIC     "gh_secured_cum_uplift",
// MAGIC     "gh_secured_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Secured Lending",
// MAGIC     color="#0D9488",
// MAGIC     metric_label="Secured Lending"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 5.3 Header
// MAGIC %md
// MAGIC ### 5.3 Unsecured Principal
// MAGIC
// MAGIC `unsecured_principal`
// MAGIC
// MAGIC Evolução do **principal (saldo originádo)** de empréstimos pessoais sem garantia. Complementa a visão de receita (5.1) com o volume originádo.

// COMMAND ----------

// DBTITLE 1,Unsecured Principal: Charts
// MAGIC %python
// MAGIC        
// MAGIC plot_uplift_charts(
// MAGIC     "gh_unsecured_principal_cum_uplift",
// MAGIC     "gh_unsecured_principal_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Unsecured Principal",
// MAGIC     color="#7C3AED",
// MAGIC     metric_label="Unsecured Principal"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 5.4 Header
// MAGIC %md
// MAGIC ### 5.4 Secured Principal
// MAGIC
// MAGIC `secured_principal`
// MAGIC
// MAGIC Evolução do **principal (saldo originádo)** de empréstimos com garantia. Complementa a visão de receita (5.2) com o volume originádo.

// COMMAND ----------

// DBTITLE 1,Secured Principal: Charts
// MAGIC %python
// MAGIC        
// MAGIC plot_uplift_charts(
// MAGIC     "gh_secured_principal_cum_uplift",
// MAGIC     "gh_secured_principal_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Secured Principal",
// MAGIC     color="#2563EB",
// MAGIC     metric_label="Secured Principal"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 6 Header
// MAGIC %md
// MAGIC ## 6. Conta
// MAGIC
// MAGIC `conta_anticipated_npv = money_boxes + conta_interest + locked_deposits + debit_interchange`
// MAGIC
// MAGIC Uplift no produto **Conta**, composto por receita de juros (caixinhas, conta corrente, depósitos bloqueados) e interchange de débito.

// COMMAND ----------

// DBTITLE 1,Conta: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_conta_cum_uplift",
// MAGIC     "gh_conta_periodic_uplift",
// MAGIC     "Global Holdout 2025 — Conta",
// MAGIC     color="#2563EB",
// MAGIC     metric_label="Conta"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Section 7 Header
// MAGIC %md
// MAGIC ## 7. NuPay
// MAGIC
// MAGIC `nupay_anticipated_npv = nupay_cc_interchange + nupay_debit_interchange + nupay_pcj`
// MAGIC
// MAGIC Uplift no produto **NuPay**, composto pelas receitas de interchange (crédito e débito) e PCJ (parcelamento com juros).

// COMMAND ----------

// DBTITLE 1,NuPay: Charts
// MAGIC %python
// MAGIC plot_uplift_charts(
// MAGIC     "gh_nupay_cum_uplift",
// MAGIC     "gh_nupay_periodic_uplift",
// MAGIC     "Global Holdout 2025 — NuPay",
// MAGIC     color="#7C3AED",
// MAGIC     metric_label="NuPay"
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Sanity Checks Header
// MAGIC %md
// MAGIC ## Sanity Checks
// MAGIC
// MAGIC Validações antes da análise de uplift:
// MAGIC 1. **Variantes disponíveis** — quantos clientes únicos por variante no experimento
// MAGIC 2. **Unicidade de variante** — algum cliente aparece em mais de uma variante?
// MAGIC 3. **Balance pré-experimento** — Treatment e Control tinham RAM semelhante antes do rollout? (uplift ≈ 0 e não significativo = grupos comparáveis)

// COMMAND ----------

// DBTITLE 1,Check 1 — Variantes Disponíveis
// ── Check 1: Variantes disponíveis e contagem de clientes únicos ──
display(
  globalHoldout
    .select($"customer__id", $"variant").distinct()
    .groupBy($"variant")
    .agg(countDistinct($"customer__id").as("n_customers"))
    .orderBy($"variant")
)

// COMMAND ----------

// DBTITLE 1,Check 2 — Unicidade de Variante
// ── Check 2: Clientes que aparecem em mais de uma variante ──
val multiVariantCustomers = globalHoldout
  .select($"customer__id", $"variant").distinct()
  .groupBy($"customer__id")
  .agg(
    countDistinct($"variant").as("n_variants"),
    collect_set($"variant").as("variants")
  )
  .filter($"n_variants" > 1)

val nMulti = multiVariantCustomers.count()
println(s"Clientes com múltiplas variantes: $nMulti")
if (nMulti > 0) display(multiVariantCustomers.limit(20))

// COMMAND ----------

// DBTITLE 1,Check 3 — Balance Pré-Experimento (Jan–Fev/25)
// ── Check 3: Balance pré-experimento (Jan/25 – Fev/25) ──
// Se a randomização foi correta, uplift ≈ 0 e NÃO significativo.
//
// ⚠ Usa assignment de variante sem filtro temporal — queremos comparar
//   o comportamento dos grupos ANTES do experimento começar.

// 1. Variant assignment
val variantAssignment = globalHoldout
  .select($"customer__id", $"variant").distinct()

// 2. RAM pré-experimento agregado por cliente/dia/variante
val preRam = ramTable
  .filter($"date" >= "2025-01-01" && $"date" < "2025-03-01")
  .join(variantAssignment, Seq("customer__id"), "inner")
  .withColumn("period", $"date".cast("date"))
  .groupBy($"customer__id", $"period", $"variant")
  .agg(
    sum($"total_anticipated_npv").as("metric_total"),
    sum($"cc_anticipated_npv").as("cc"),
    sum($"lending_anticipated_npv").as("lending"),
    sum($"conta_anticipated_npv").as("conta"),
    sum($"nupay_anticipated_npv").as("nupay")
  )

// 3. Agregar por período/variante
val preResults = preRam.groupBy($"period", $"variant")
  .agg(
    countDistinct($"customer__id").as("n_customers"),
    avg($"metric_total").as("avg_ram"),
    stddev($"metric_total").as("std_ram"),
    avg($"cc").as("avg_cc"),
    avg($"lending").as("avg_lending"),
    avg($"conta").as("avg_conta"),
    avg($"nupay").as("avg_nupay")
  )

val preTreatment = preResults.filter($"variant" =!= "control")
  .select($"period",
    $"n_customers".as("n_t"), $"avg_ram".as("avg_ram_t"), $"std_ram".as("std_ram_t"),
    $"avg_cc".as("avg_cc_t"), $"avg_lending".as("avg_lending_t"),
    $"avg_conta".as("avg_conta_t"), $"avg_nupay".as("avg_nupay_t"))

val preControl = preResults.filter($"variant" === "control")
  .select($"period",
    $"n_customers".as("n_c"), $"avg_ram".as("avg_ram_c"), $"std_ram".as("std_ram_c"),
    $"avg_cc".as("avg_cc_c"), $"avg_lending".as("avg_lending_c"),
    $"avg_conta".as("avg_conta_c"), $"avg_nupay".as("avg_nupay_c"))

// 4. Uplift com IC 95%
val preUplift = preTreatment.join(preControl, Seq("period"), "inner")
  .withColumn("uplift", $"avg_ram_t" - $"avg_ram_c")
  .withColumn("se_uplift", sqrt(pow($"std_ram_t", 2) / $"n_t" + pow($"std_ram_c", 2) / $"n_c"))
  .withColumn("moe_95", lit(1.96) * $"se_uplift")
  .withColumn("uplift_pct",
    when($"avg_ram_c" =!= 0, ($"uplift" / abs($"avg_ram_c")) * 100).otherwise(lit(null)))
  .withColumn("ci_lower", $"uplift" - $"moe_95")
  .withColumn("ci_upper", $"uplift" + $"moe_95")
  .withColumn("stat_sig",
    when($"ci_lower" > 0 || $"ci_upper" < 0, lit("YES")).otherwise(lit("NO")))
  .withColumn("uplift_cc", $"avg_cc_t" - $"avg_cc_c")
  .withColumn("uplift_lending", $"avg_lending_t" - $"avg_lending_c")
  .withColumn("uplift_conta", $"avg_conta_t" - $"avg_conta_c")
  .withColumn("uplift_nupay", $"avg_nupay_t" - $"avg_nupay_c")

display(
  preUplift.select(
    $"period", $"n_t", $"n_c",
    round($"avg_ram_t", 4).as("avg_ram_treatment"),
    round($"avg_ram_c", 4).as("avg_ram_control"),
    round($"uplift", 4).as("uplift_R$"),
    round($"uplift_pct", 2).as("uplift_pct"),
    $"stat_sig",
    round($"uplift_cc", 4).as("uplift_cc"),
    round($"uplift_lending", 4).as("uplift_lending"),
    round($"uplift_conta", 4).as("uplift_conta"),
    round($"uplift_nupay", 4).as("uplift_nupay")
  ).orderBy($"period")
)