// Databricks notebook source
// MAGIC %md
// MAGIC # Project Coco — Análise de Subotimalidade em Lending
// MAGIC
// MAGIC **Objetivo:** Identificar Credit Users com multi-elegibilidade em Lending que contrataram um produto mais caro quando tinham uma opção mais barata disponível.
// MAGIC
// MAGIC **Output:** Tamanho e perfil do segmento-alvo para o Tratamento B do teste.
// MAGIC
// MAGIC ---
// MAGIC **Adaptar antes de rodar:**
// MAGIC - `<tabela_de_contratos>` → nome real da tabela de contratos de crédito
// MAGIC - `<tabela_de_elegibilidade>` → nome real da tabela de elegibilidade
// MAGIC - Campos de taxa, status e produto → ajustar conforme schema real

// COMMAND ----------

// MAGIC %run "/data-analysts/utils-uc"

// COMMAND ----------

// MAGIC %md
// MAGIC ## 0. Configuração

// COMMAND ----------

import org.apache.spark.sql.functions._

// Janela de análise (contratos dos últimos N meses)
val monthsLookback = 12

// Threshold mínimo de subotimalidade:
// cliente pagaria pelo menos X% a mais no custo total da dívida
val minCostDiffPct = 0.15

// ⚠️ Verificar os valores reais da coluna `product` na tabela de contratos
// antes de rodar (ex: "personal_loan", "fgts_backed_loan", etc.)
val productFilter = Seq("personal_loan", "inss", "fgts", "private_payroll","public_payroll")

println(s"Janela: últimos $monthsLookback meses")
println(s"Threshold: >${(minCostDiffPct * 100).toInt}% mais caro no custo total")
println(s"Produtos em escopo: ${productFilter.mkString(", ")}")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Contratos recentes de Lending
// MAGIC
// MAGIC Filtra contratos dos últimos 12 meses (excluindo renegociações) e calcula o custo total da dívida.
// MAGIC
// MAGIC > **Custo total = principal × (1 + taxa_mensal)^parcelas**
// MAGIC > ⚠️ `interest_rate` precisa estar em formato **mensal** — se estiver anual, dividir por 12.

// COMMAND ----------

spark.table("etl.br__dataset.personal_loan_parameters_without_models_current_snapshot")
  .where($"is_renegotiation" === false)
  .where($"is_valid_loan")
  .withColumn("product", when('product === "personal_loan_product__fgts_backed_loan", lit("fgts"))
  .when('product === "personal_loan_product__personal_loan", lit("personal_loan"))
  .when('credit_program === "private_payroll", lit("private_payroll"))
  .when('credit_program === "public_payroll_agreements", lit("public_payroll"))
  .when('credit_program === "public_payroll_inss", lit("inss")))

  .where($"product".isin(productFilter: _*))
  .where($"created_at_date" >= add_months(current_date(), -monthsLookback))
  .selectExpr(
    "customer__id",
    "product            AS product_type",
    "principal          AS principal_amount",
    "interest_rate      AS interest_rate_monthly",
    "installments       AS num_installments",
    "created_at_date    AS contract_date",
    "ROUND(principal * POW(1 + interest_rate, installments), 2) AS total_debt_cost"
  )
  .createOrReplaceTempView("recent_lending_contracts")

display(spark.sql("""
  SELECT product_type,
         COUNT(*)                    AS contratos,
         COUNT(DISTINCT customer__id) AS clientes
  FROM recent_lending_contracts
  GROUP BY product_type
  ORDER BY clientes DESC
"""))

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Fontes de elegibilidade
// MAGIC
// MAGIC `lending-customer-monitoring` é a base única para flags de elegibilidade (inclui o split INSS via `is_eligible__public_payroll_provider_dataprev`).
// MAGIC As três tabelas de produto são joinadas **apenas para puxar taxa e limite**, que não estão no monitoring.

// COMMAND ----------

// Base de elegibilidade: lending-customer-monitoring
// Resolve flags de todos os produtos + taxa e limite do PL diretamente
datasets("dataset/lending-customer-monitoring")
  .selectExpr(
    "customer__id",
    // PL
    "is_eligible__personal_loan",
    "min_loan_rate                AS pl_offered_rate",
    "personal_loan_max_principal  AS pl_max_principal",
    // FGTS
    "fgts_backed_loan__eligible",
    // INSS (Dataprev) — split via flag, sem precisar de payroll_provider
    "is_eligible__public_payroll_provider_dataprev AS is_eligible__inss",
    // Privado
    "private_payroll_loan__eligible"
  )
  .createOrReplaceTempView("monitoring")

// FGTS — taxa e limite
spark.table("br__dataset.lending_customer_fgts_loan_eligibility_daily_snapshot")
  .where($"fgts_backed_loan__eligible")
  .selectExpr(
    "customer__id",
    "fgts_backed_loan__interest_rate  AS fgts_offered_rate",
    "fgts_backed_loan__max_principal_cap AS fgts_max_principal"
  )
  .createOrReplaceTempView("fgts_elig")

// INSS / Consignado Público — taxa e limite
// Filtra elegíveis; o split INSS vs demais públicos é feito via monitoring
spark.table("br__policy.payroll_loan_new_contract_policy_selector")
  .where($"eligible")
  .selectExpr(
    "customer__id",
    "interest_rate    AS public_payroll_offered_rate",
    "max_principal_cap AS public_payroll_max_principal"
  )
  .createOrReplaceTempView("public_elig")

// Consignado Privado — taxa e limite
spark.table("etl.br__dataset.private_payroll_customer_monitoring_daily_snapshot")
  .where($"private_payroll_loan__eligible")
  .selectExpr(
    "customer__id",
    "private_payroll_loan__interest_rate    AS private_offered_rate",
    "private_payroll_loan__max_principal_cap AS private_max_principal"
  )
  .createOrReplaceTempView("private_payroll_elig")

println("Fontes de elegibilidade registradas como temp views.")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Visão unificada de elegibilidade
// MAGIC
// MAGIC Um registro por `(customer_id, product_type)` com taxa ofertada e limite — base para a comparação de custo.

// COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW current_eligibility AS

-- Personal Loan: taxa e limite direto do monitoring
SELECT customer__id, 'personal_loan' AS product_type,
       pl_offered_rate AS offered_rate, pl_max_principal AS max_eligible_amount
FROM monitoring
WHERE is_eligible__personal_loan = true

UNION ALL

-- FGTS: join para taxa e limite
SELECT m.customer__id, 'fgts' AS product_type,
       f.fgts_offered_rate AS offered_rate, f.fgts_max_principal AS max_eligible_amount
FROM monitoring m
JOIN fgts_elig f ON m.customer__id = f.customer__id
WHERE m.fgts_backed_loan__eligible = true

UNION ALL

-- INSS (Dataprev): usa flag is_eligible__inss do monitoring para split
SELECT m.customer__id, 'inss' AS product_type,
       i.public_payroll_offered_rate AS offered_rate, i.public_payroll_max_principal AS max_eligible_amount
FROM monitoring m
JOIN inss_elig i ON m.customer__id = i.customer__id
WHERE m.is_eligible__inss = true

UNION ALL

-- Consignado Privado: join para taxa e limite
SELECT m.customer__id, 'private_payroll' AS product_type,
       pp.private_offered_rate AS offered_rate, pp.private_max_principal AS max_eligible_amount
FROM monitoring m
JOIN private_payroll_elig pp ON m.customer__id = pp.customer__id
WHERE m.private_payroll_loan__eligible = true
""")

display(spark.sql("""
  SELECT product_type, COUNT(DISTINCT customer__id) AS clientes_elegiveis
  FROM current_eligibility
  GROUP BY product_type ORDER BY clientes_elegiveis DESC
"""))

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Identificar contratos subótimos
// MAGIC
// MAGIC Para cada contrato, verifica se o cliente tinha uma alternativa mais barata disponível que cobria o mesmo ticket.
// MAGIC
// MAGIC Critérios:
// MAGIC - Produto diferente do contratado
// MAGIC - Alternativa cobre o ticket (`max_eligible_amount >= principal_amount`)
// MAGIC - Alternativa tem taxa menor
// MAGIC - Diferença de custo total > 15%

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("""
// MAGIC CREATE OR REPLACE TEMP VIEW suboptimal_contracts AS
// MAGIC SELECT
// MAGIC   c.customer__id,
// MAGIC   c.product_type                                            AS contracted_product,
// MAGIC   c.principal_amount,
// MAGIC   c.num_installments,
// MAGIC   c.interest_rate_monthly                                   AS contracted_rate,
// MAGIC   c.total_debt_cost                                         AS contracted_total_cost,
// MAGIC   c.contract_date,
// MAGIC
// MAGIC   e.product_type                                            AS alternative_product,
// MAGIC   e.offered_rate                                            AS alternative_rate,
// MAGIC   e.max_eligible_amount,
// MAGIC
// MAGIC   -- Custo total se tivesse usado a alternativa (mesmo ticket e prazo)
// MAGIC   ROUND(
// MAGIC     c.principal_amount * POW(1 + e.offered_rate, c.num_installments),
// MAGIC   2)                                                        AS alternative_total_cost,
// MAGIC
// MAGIC   -- Diferença em R$
// MAGIC   ROUND(
// MAGIC     c.total_debt_cost -
// MAGIC     (c.principal_amount * POW(1 + e.offered_rate, c.num_installments)),
// MAGIC   2)                                                        AS saving_brl,
// MAGIC
// MAGIC   -- Diferença em %
// MAGIC   ROUND(
// MAGIC     (c.total_debt_cost /
// MAGIC     (c.principal_amount * POW(1 + e.offered_rate, c.num_installments))) - 1,
// MAGIC   4)                                                        AS saving_pct
// MAGIC
// MAGIC FROM recent_lending_contracts c
// MAGIC JOIN current_eligibility e
// MAGIC   ON  c.customer__id  = e.customer__id
// MAGIC   AND e.product_type != c.product_type           -- produto diferente do contratado
// MAGIC
// MAGIC WHERE
// MAGIC   e.max_eligible_amount >= c.principal_amount    -- alternativa cobre o mesmo ticket
// MAGIC   AND e.offered_rate    <  c.interest_rate_monthly -- alternativa é mais barata
// MAGIC   AND (
// MAGIC     c.total_debt_cost /
// MAGIC     (c.principal_amount * POW(1 + e.offered_rate, c.num_installments))
// MAGIC   ) >= 1.15                                      -- diferença mínima de 15%
// MAGIC """)
// MAGIC
// MAGIC display(spark.sql("SELECT COUNT(DISTINCT customer__id) AS clientes_subotimos FROM suboptimal_contracts"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## 5. Métricas por par de produto
// MAGIC
// MAGIC Quem usou qual produto quando tinha uma alternativa melhor? E quanto poderiam ter economizado?

// COMMAND ----------

// MAGIC %python
// MAGIC display(spark.sql("""
// MAGIC SELECT
// MAGIC   contracted_product,
// MAGIC   alternative_product,
// MAGIC   COUNT(DISTINCT customer__id)     AS num_customers,
// MAGIC   ROUND(AVG(saving_brl), 2)        AS avg_saving_brl,
// MAGIC   ROUND(AVG(saving_pct) * 100, 1)  AS avg_saving_pct,
// MAGIC   ROUND(MIN(saving_brl), 2)        AS min_saving_brl,
// MAGIC   ROUND(MAX(saving_brl), 2)        AS max_saving_brl,
// MAGIC   ROUND(AVG(principal_amount), 2)  AS avg_ticket
// MAGIC FROM suboptimal_contracts
// MAGIC GROUP BY contracted_product, alternative_product
// MAGIC ORDER BY num_customers DESC
// MAGIC """))

// COMMAND ----------

// MAGIC %md
// MAGIC ## 6. Distribuição de economia (histograma)
// MAGIC
// MAGIC Configure o gráfico como **Bar Chart** no Databricks (eixo X: `saving_bucket`, eixo Y: `num_customers`).

// COMMAND ----------

// MAGIC %python
// MAGIC // Histograma: buckets de R$500
// MAGIC display(spark.sql("""
// MAGIC SELECT
// MAGIC   FLOOR(saving_brl / 500) * 500  AS saving_bucket,
// MAGIC   COUNT(*)                       AS num_customers
// MAGIC FROM suboptimal_contracts
// MAGIC GROUP BY saving_bucket
// MAGIC ORDER BY saving_bucket
// MAGIC """))
// MAGIC
// MAGIC // Estatísticas descritivas
// MAGIC spark.sql("""
// MAGIC SELECT
// MAGIC   ROUND(percentile_approx(saving_brl, 0.50), 2) AS mediana_brl,
// MAGIC   ROUND(AVG(saving_brl), 2)                      AS media_brl,
// MAGIC   ROUND(percentile_approx(saving_brl, 0.25), 2) AS p25_brl,
// MAGIC   ROUND(percentile_approx(saving_brl, 0.75), 2) AS p75_brl
// MAGIC FROM suboptimal_contracts
// MAGIC """).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## 7. Segmento final para o teste (Tratamento B)
// MAGIC
// MAGIC Um registro por cliente com a melhor alternativa disponível (maior economia potencial em R$).

// COMMAND ----------

// MAGIC %md
// MAGIC ## 8. Checklist de validação antes de usar o segmento
// MAGIC
// MAGIC - [ ] Os valores da coluna `product` na tabela de contratos batem com os usados no `productFilter`?
// MAGIC - [ ] `interest_rate` está em formato **mensal**? (se anual: dividir por 12)
// MAGIC - [ ] O tamanho do segmento é suficiente para significância estatística com 3 grupos?
// MAGIC - [ ] Há clientes no segmento em outro experimento de Lending ativo? (checar overlap)
// MAGIC - [ ] O campo `is_eligible__inss` reflete elegibilidade real (aprovada pelo modelo) ou pré-elegibilidade?
// MAGIC - [ ] Clientes com contratos em atraso ou renegociação devem ser excluídos? (checar `status`)
// MAGIC - [ ] O `lending-customer-monitoring` usado é do snapshot mais recente? (verificar coluna `date`)
// MAGIC
// MAGIC ---
// MAGIC *Project Coco — CLO Core | abril 2026*

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("""
// MAGIC CREATE OR REPLACE TEMP VIEW target_segment AS
// MAGIC SELECT DISTINCT
// MAGIC   customer__id,
// MAGIC   contracted_product,
// MAGIC   FIRST_VALUE(alternative_product) OVER w AS best_alternative_product,
// MAGIC   FIRST_VALUE(alternative_rate)    OVER w AS best_alternative_rate,
// MAGIC   FIRST_VALUE(saving_brl)          OVER w AS max_saving_brl,
// MAGIC   FIRST_VALUE(saving_pct)          OVER w AS max_saving_pct,
// MAGIC   FIRST_VALUE(contract_date)       OVER w AS last_contract_date
// MAGIC FROM suboptimal_contracts
// MAGIC WINDOW w AS (PARTITION BY customer__id ORDER BY saving_brl DESC)
// MAGIC """)
// MAGIC
// MAGIC display(spark.sql("""
// MAGIC SELECT
// MAGIC   contracted_product,
// MAGIC   best_alternative_product,
// MAGIC   COUNT(*)                          AS num_customers,
// MAGIC   ROUND(AVG(max_saving_brl), 2)     AS avg_saving_brl,
// MAGIC   ROUND(AVG(max_saving_pct)*100, 1) AS avg_saving_pct
// MAGIC FROM target_segment
// MAGIC GROUP BY contracted_product, best_alternative_product
// MAGIC ORDER BY num_customers DESC
// MAGIC """))