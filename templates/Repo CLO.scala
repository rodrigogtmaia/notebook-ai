// Databricks notebook source
// MAGIC %md
// MAGIC O objetivo desse notebook é ser um compilado dos datasets mais usados em CLO. Fique a vontade para adicionar um novo dataset (preferencialmente com alguma descrição) ou retirar algum que já tenha sido deprecado

// COMMAND ----------

// MAGIC %run "/data-analysts/utils-uc"

// COMMAND ----------

// MAGIC %md
// MAGIC https://nudatacatalog.atlan.com/

// COMMAND ----------

// MAGIC %md
// MAGIC ##Lista de datasets 

// COMMAND ----------

// DBTITLE 1,Purple Loop
val assignments_deduplicated = datasets("dataset/purple-loop-assignments-deduplicated") // dados de assignments, sem info de comms

val push_communications = spark.table("br__dataset.purple_loop_assignments_push_communications") // todos pushes de PPLP (con info de click) + linkado com assignment
val email_communications = spark.table("br__dataset.purple_loop_assignments_email_communications") // todos emails de PPLP (con info de open) + linkado com assignment
val announcement_communications = spark.table("br__dataset.purple_loop_assignments_announcement_communications") // todos announcement de PPLP (con info de click) + linkado com assignment

val operators_current = datasets("dataset/br-purple-loop-operators") // current operators of all customer
val operators_archive = datasets("series-contract/dataset-br-purple-loop-operators") // archive

val early_journey_current = datasets("dataset/nu-core-br-canonical-view-current-early-journey")
val early_journey_daily = datasets("dataset/nu-core-br-canonical-view-daily-early-journey") //60dias de historico

val journey_moments = spark.table("br__contract.purple_journeys__journey_moments") // status, nome, produto etc...
val campaigns = datasets("contract-no-pipoca/campaigns") // status, nome, etc...
val monitoring_journey_moment_campaign = datasets("dataset/purple-loop-monitoring-journey-moment-to-campaign-dimension") // map JM, campanha, branches, narrativa, stage names e ids
val monitoring_cip_envelope = datasets("dataset/purple-loop-monitoring-cip-branch-to-envelope-dimension") // map branches, actions, templates
val mdss = datasets("series-contract/purple-loop-monitoring-rollout-control") // data de rollout dos JMs 

val non_assignemtns = spark.table(" etl.br__data_source.purple_loop_non_assignments_inc") // para deepdives e entender pq um assignment não aconteceu

// dados de elegibilidade  e stage (mais infos sobre o que cada coluna representa https://nubank.atlassian.net/wiki/spaces/FCB/pages/263994048815/Core+Operator+Filters) :
val jm_name = "growth-underage-acquisition"
val campaign_elegibility = datasets(s"dataset/purple-loop-core-campaign-eligibility-${jm_name}")
val campaign_elegibility_archive = datasets(s"series-contract/dataset-purple-loop-core-campaign-eligibility-${jm_name}")
val stage = datasets(s"dataset/purple-loop-core-jm-stages-${jm_name}")
val stage_archive = datasets(s"series-contract/dataset-purple-loop-core-jm-stages-${jm_name}")



// COMMAND ----------

// DBTITLE 1,remy (contexrual rataria)
// Todos os datasets do remy moram aqui: https://github.com/nubank/itaipu/tree/master/subprojects/data-domains/core-brazil/src/main/scala/nu/data_domain/core_brazil/br/core_cross_sell_customer_lifecycle_management/datasets/purple_loop/bp_campaign_eligibility_factory/config/cross_product/bp_cross_product_event_triggered/acquisition

// COMMAND ----------

// DBTITLE 1,Channels
val push_comms = spark.table("br__dataset.push_communications") // todos os pushes enviados -> necessário filtrar categoria promotion/shopping etc
val push_comms_metrics = spark.table("br__dataset.push_communications_metrics")/// todos os pushes agregados por template
val email_comms = spark.table("br__dataset.email_communications")
val email_comms_metrics = spark.table("br__dataset.email_communications_metrics")
val whatsapp = spark.table("br__dataset.whatsapp_communications")

val no_studio = datasets("nu-br/dataset/no-studio-template-history") // linka template com outras varias infos (texto, href, nujourney funnel, product, etc)
val no_churros = datasets("contract-no-churros/templates")  // linka template com outras varias infos (texto, href, nujourney funnel, product, etc)

// OPT OUT, mais infos https://nubank.atlassian.net/wiki/spaces/PurpleHub/pages/262800736342/Comms+Datasets
val email_push_opt_out_current = spark.table("br__contract.panama__channel_category_opt_outs") //foto atual
val email_push_opt_out_history = spark.table("br__series_contract.channel_category_opt_out") // histórico


// in app channels 
val announcement = spark.table("br__dataset.announcement_communications")
val now = spark.table("br__dataset.now_dashboard_communications_events")
val now_templates = spark.table("br__dataset.now_dashboard_instances_templates")
val highlight_comms = spark.table("br__dataset.highlights_communications")

// COMMAND ----------

// DBTITLE 1,Ranking Strategies
// Checar quais os datasets das RS ativas no isolator de Core: https://nubank-e2-general.cloud.databricks.com/editor/notebooks/2956707692870710/dashboards/6e49cf11-8068-4bb3-a5c5-e28bd7d0e4e3/present?o=2093534396923660

//  Tornar o dataset da RS mais amigável:
import org.apache.spark.sql.types.{StringType, DoubleType, ArrayType, StructType, StructField}  
  def withUniformDistributionScoreFromJson(df: DataFrame): DataFrame = {
    val campaignSchema = StructType(
      Seq(
        StructField("id", StringType, nullable = true),
        StructField("score", DoubleType, true)
      )
    )

    val narrativeSchema = StructType(
      Seq(
        StructField("id", StringType, nullable = true),
        StructField("score", DoubleType, nullable = true),
        StructField("campaigns", ArrayType(campaignSchema), nullable = true)
      )
    )

    val journeyMomentSchema = StructType(
      Seq(
        StructField("journey_moment__product", StringType, nullable = true),
        StructField("id", StringType, nullable = true),
        StructField("score", DoubleType, nullable = true),
        StructField("narratives", ArrayType(narrativeSchema), nullable = true)
      )
    )

    val uniformDistributionScoresSchema = ArrayType(journeyMomentSchema)

    df.withColumn("rank",
                  from_json($"rank", uniformDistributionScoresSchema))
      .withColumn("jm", explode($"rank"))
      .withColumn("journey_moment", $"jm.id")
      .withColumn("journey_moment_score", $"jm.score")
      .withColumn("narr", explode($"jm.narratives"))
      .withColumn("narrative", $"narr.id")
      .withColumn("narrative_score", $"narr.score")
      .withColumn("cpg", explode($"narr.campaigns"))
      .withColumn("campaign", $"cpg.id")
      .withColumn("campaign_score", $"cpg.score".cast("double"))
      .select('customer__id, 'journey_moment, 'journey_moment_score, 'campaign, 'campaign_score)  

      .join(spark.table("br__contract.purple_journeys__journey_moments")
              .select('journey_moment__identifier.as("journey_moment"), 'journey_moment__product, 'journey_moment__name, 'journey_moment__status)
              .distinct
            ,Seq("journey_moment"), "left")

      .join(datasets("contract-no-pipoca/campaigns")
              .select('campaign__id.as("campaign"), 'campaign__name, 'campaign__status)
            ,Seq("campaign"), "left")  

  }

datasets("dataset/playground-serving-layer-ranking-strategy")
  .transform(withUniformDistributionScoreFromJson)

.d  

// COMMAND ----------

// DBTITLE 1,holdouts 2025
val highlights = spark.table("br__xp.etl_experiment_assignments_highlights_roxinho_mass_market_holdout_group_2025_v2")

val controlao = spark.table("br__dataset.clo_global_cross_sell_holdout_only_control_group")

// COMMAND ----------

// DBTITLE 1,NuJourney
val entrypoint_conversions  = datasets("dataset/nujourney_entrypoint_conversions_union")
val conversions = datasets("dataset/nujourney_conversions_union")

// COMMAND ----------

// DBTITLE 1,Cubo / high value customers

val cubo = spark.table("br__dataset.core_clo_customer_profiling_v0") 

//nu-br/dataset/core-clo-customer-profiling-clm   -> cubinho

val high_value_customers = datasets("dataset/high-value-customers")

// COMMAND ----------

// DBTITLE 1,Geralzão
val engagement_monthly = spark.table("etl.br__dataset.engagement") 
val engagement_groups = datasets("dataset/engagement-segments-current")

val cross_product = datasets("dataset/cross-product-customer-activity_v1")
val customer_snapshot = datasets("nu-br/core/customer-current-snapshot")
val customer_customers = datasets("contract-customers/customers")
val cust_key_integrity = datasets("dataset/customer-key-integrity")
val account_key_integrity = datasets("dataset/account-key-integrity")

val income_segments = datasets("dataset/br-segments-v5")
val income_segments_archive = spark.table("br__series_contract.dataset_br_segments_v5")

val date = datasets("dataset/date")

// COMMAND ----------

// DBTITLE 1,Eligibilidade por Operator
//Underage

val customerCustomers = spark.table("contract-customers/customers")
val cutCustomers = spark.table("nu-br/dataset/core-ranking-strategy-under-age-eligibility-cut")

// Eligible customers = All customers MINUS those in the cut list
val eligibleCustomers = customerCustomers
  .join(cutCustomers, Seq("customer__id"), "leftanti")

val totalEligible = eligibleCustomers.count()
println(s"Total eligible customers: $totalEligible")



//UV

val customerCustomers = datasets("contract-customers/customers")
val cutCustomers = datasets("core-ranking-strategy-under-age-eligibility-cut")
val customerTier = datasets("nu-br/dataset/br-customer-segment-tier")


val eligibleCustomers = customerCustomers
  .join(customerTier, Seq("customer__id"), "inner")
  .filter(col("is_ultraviolet") === false)
  .join(cutCustomers, Seq("customer__id"), "leftanti")

val totalEligible = eligibleCustomers.count()
println(s"Total eligible customers (excluding ultraviolet): $totalEligible")

// COMMAND ----------

// DBTITLE 1,Audiencias que usam Atributos
// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM etl.br__contract.celebi__audiences
// MAGIC WHERE
// MAGIC    audience__query LIKE "%parent-flow-input-name-screen%"
// MAGIC    OR audience__query LIKE "%parent-flow-underage-success-screen%"
// MAGIC ORDER BY audience__last_processing_request desc

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

// DBTITLE 1,Canonical layer
// dado de atividade dos produtos CLM data
// https://docs.google.com/spreadsheets/d/1lJtngGHo9xFdPJrpaihzS-B_9JDyuESt2d5gVTMCSe0/edit?gid=1112940521#gid=1112940521

// COMMAND ----------

// DBTITLE 1,job para ligar cluster de fds
//https://nubank-e2-general.cloud.databricks.com/jobs/591249484011368/runs?o=2093534396923660

// COMMAND ----------

// MAGIC %md
// MAGIC ### Product Datasets

// COMMAND ----------

// DBTITLE 1,Lending
// conso
val loanParameters = datasets("dataset/personal-loan-parameters-current-snapshot")

// unsecured datasets
val unsecuredEligibles = datasets("nu-br/dataset/lending-customer-public-info")

// private payroll datasets
val ppOriginations = spark.table("br__contract.farfalle__payroll_loans")
val ppEligibility = datasets("dataset/private-payroll-customer-monitoring-daily-snapshot")
val ppOfferParameters = spark.table("br__dataset.private_payroll_loan_offer_parameters")

// public payroll datasets
val payrollEligibility = datasets("dataset/lending-customer-payroll-loan-eligibility-daily-snapshot")

// fgts datasets
val fgtsEligibility = spark.table("br__dataset.lending_customer_fgts_loan_eligibility_daily_snapshot")

// ibl datasets
val iblEligibility = datasets("dataset/investment-backed-loan-daily-eligibility")


// COMMAND ----------

// MAGIC %md
// MAGIC ##Queries uteis

// COMMAND ----------

// DBTITLE 1,Conversoes pelo COCKPIT
// consultar eahart documentation

// import nu.data_domain.experimentation.shared.avionics.earhart.v2.api.design._
// import nu.data_domain.experimentation.shared.avionics.earhart.v2.api

// val exposureFirsSend = 
//  api.pipeline.getAllExposureTimestamps(
//  country = "br",
//  experimentName = "pplp-eventtriggered-underage-funnel-droppers"
//  )
//  .withColumnRenamed("exposition_at", "elegibility_at")
// .withColumnRenamed("recipient_id", "customer__id")

// val country = "br"
// val metric1 = "u18-releases-volume"
// val metric2 = "u18-consents-users" 
// val experimentName = "pplp-eventtriggered-underage-funnel-droppers"
// val startDate = "2025-09-19"
// val endDate = "2025-10-28"
// val exposures = exposureFirsSend
// // val baselineName = "control"
// // val variantName = "treatment"

// val metric1Results =
//   api.pipeline.getExperimentRawMetricDataWithView(country, experimentName, metric1, "days-in")

// val metric2Results =
//   api.pipeline.getExperimentRawMetricDataWithView(country, experimentName, metric2, "days-in")

// COMMAND ----------

// DBTITLE 1,Canonical layer product conversion
nu.data_domain.core_brazil.br.foundational_data.datasets.bp_core_brazil_canonical_layer.facts.product_conversion.allSubdomainOps
.map(_.name)
.map(datasets)
.map(_.withColumn("conversion__at", to_date(col("conversion__at"))).groupBy("conversion__product", "conversion__at").agg(count(col("conversion__id")), sum(col("conversion__value"))))
.reduce(_ unionByName _)
.d

// COMMAND ----------

// DBTITLE 1,[PPLP] Comms performance - union push, email, annoucement
val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
val startDate = "2025-07-01"
val daysAgo: Int = 6
val endDate: String = LocalDate.now().minusDays(daysAgo).format(dateFormatter)
val segment_filter = "core"

val push = datasets("dataset/purple_loop_assignments_push_communications")
  .filter($"customer_segment" === segment_filter)
  .withColumn("date", to_date('assigned_at))
  .filter('date.between(startDate, endDate))
  .withColumn("delivered_numeric", when('communication__sent_at.isNotNull, 1).otherwise(0))  // communication__delivered_at substituido provisoriamente devido a problema de delivery rate
  .withColumn("engage_numeric", when('communication__clicked === true, 1).otherwise(0))
  .select('journey_moment__name, 'stage, 'campaign__name, 'template_name, lit("push").as("channel"), 'date, 'engage_numeric, 'assignment__identifier, 'customer__id, 'delivered_numeric)

val email = datasets("dataset/purple_loop_assignments_email_communications")
  .filter($"customer_segment" === segment_filter)
  .withColumn("date", to_date('assigned_at))
  .filter('date.between(startDate, endDate))
  .withColumn("delivered_numeric", when('communication__delivered_at.isNotNull, 1).otherwise(0))
  .withColumn("engage_numeric", when('communication__opened === true, 1).otherwise(0))
  .select('journey_moment__name, 'stage, 'campaign__name, 'template_name, lit("email").as("channel"), 'date, 'engage_numeric, 'assignment__identifier, 'customer__id, 'delivered_numeric)

val annoucement = datasets("dataset/purple_loop_assignments_announcement_communications")
  .filter($"customer_segment" === segment_filter)
  .withColumn("date", to_date('assigned_at))
  .filter('date.between(startDate, endDate))
  .withColumn("delivered_numeric", when('communication__displayed_at.isNotNull, 1).otherwise(0))
  .withColumn("engage_numeric", when('communication__clicked_primary || 'communication__clicked_secondary, lit(1)).otherwise(lit(0)))
  .select('journey_moment__name, 'stage, 'campaign__name, 'template_name, lit("announcement").as("channel"), 'date, 'engage_numeric, 'assignment__identifier, 'customer__id, 'delivered_numeric)

val campaigns = datasets("dataset/purple-loop-monitoring-journey-moment-to-campaign-dimension")
  .filter(array_contains('campaign__segments, "core"))
  .select('journey_moment__product, 'journey_moment__name, 'campaign__name, 'campaign__stage.as("stage"), 'campaign__branch__id.as("branch__id"), 'campaign__duration.as("duration"), 'campaign__cooldown_duration.as("cooldown"), 'purple_loop__enabled)
  .withColumn("jm_stage", concat('journey_moment__name, lit("-"), 'stage))

val comms = push.unionByName(email).unionByName(annoucement)
  .withColumn("month", date_format('date, "MM-yyyy"))
  .join(campaigns, Seq("campaign__name", "stage"), "left")

val comms_performance = comms
  .groupBy('journey_moment__product, 'stage, 'channel)
  .agg(
    countDistinct("customer__id").as("impacted_customers"),
    count("*").as("total_requested"),
    (sum('delivered_numeric) / count("*")).as("delivery_rate"),
    (sum('engage_numeric) / sum('delivered_numeric)).as("CTR/open")
  )

comms_performance.d

// COMMAND ----------

// DBTITLE 1,[PPLP] Assignments per product/JM
val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
val startDate = "2024-01-01"


val jm = datasets("dataset/purple-loop-monitoring-journey-moment-to-campaign-dimension")
.select('journey_moment__id, 'journey_moment__product, 'journey_moment__name)
.dropDuplicates

val assignments = datasets("dataset/purple-loop-assignments-deduplicated")
.withColumn("assigment_date", to_date('assigned_at))
.filter('assigment_date >= startDate)
.withColumnRenamed("journey_moment_id", "journey_moment__id")
.join(jm, Seq("journey_moment__id"), "left")
.filter('journey_moment__name === "pj-account-acquisition") // alterar o filtro do produto

assignments
.groupBy('type, 'assigment_date).count.d

// COMMAND ----------

// DBTITLE 1,[PPLP] list of all product names
nu.data_domain.core_brazil.br.core_cross_sell_customer_lifecycle_management.datasets.purple_loop.bp_campaign_eligibility_factory.config.allProductEligibilityConfigs.map(_.productName).foreach(println)

// COMMAND ----------

// DBTITLE 1,[PPLP] List of all actives JMs
datasets("dataset/purple-loop-monitoring-journey-moment-to-campaign-dimension")
.filter('journey_moment__enabled === true)
.filter('segment_experiment__segment === "core")
.select('journey_moment__product, 'journey_moment__name).dropDuplicates.d

// COMMAND ----------

// DBTITLE 1,[PPLP] Elegibilidade archive de todos os JMs juntos
import nu.data.infra.util.dataset_series.DatasetSeriesOpNameLookup

    val jm_product_map = spark.table("br__contract.purple_journeys__journey_moments")
    .select($"journey_moment__identifier".as("journey_moment__id"), $"journey_moment__product")
    .cache
    val targetDate = java.time.LocalDate.now.minusDays(30).toString

    val jmEligiblityHistorical = nu.data_domain.core_brazil.br.core_cross_sell_customer_lifecycle_management.datasets.purple_loop.bp_campaign_eligibility_factory.allCampaignEligibilityOps
      .map(DatasetSeriesOpNameLookup.datasetSeriesContractSquishName)
      .map(
        datasets(_)
        .filter($"eligibility__target_date" >= targetDate))
      .reduce(_ unionByName _)


    jmEligiblityHistorical
    // .filter('is_eligible__operator) // fazer os filtros que vc quiser,  vide https://nubank.atlassian.net/wiki/spaces/FCB/pages/263994048815/Core+Operator+Filters
    // .filter('is_eligible__product)
    .join(jm_product_map, Seq("journey_moment__id"), "left")
    .groupBy("journey_moment__product")
    .agg(countDistinct($"customer__id").as("total_eligibles"))
    .write.format("delta").mode("overwrite")
    .saveAsTable(s"usr.purple_loop.xxxx") /// vai demora> 1h pra rodar

// COMMAND ----------

// DBTITLE 1,[GERAL] salvar tabela intermediaria
df_to_save
.write.format("delta").mode("overwrite")
.saveAsTable("usr.purple_loop.xxxxx")
val df = spark.table("usr.purple_loop.xxxxx")


// OU

df.createOrReplaceTempView("oioi")

// COMMAND ----------

// DBTITLE 1,Regsol
// MAGIC %run "/Shared/RegSol/Ops Efficiency/Google_Utils-uc" 

// COMMAND ----------

// MAGIC %python
// MAGIC #google_credential - don't need to change
// MAGIC
// MAGIC google_suite_credentials = '''{
// MAGIC   "type": "service_account",
// MAGIC   "project_id": "nu-documents-check",
// MAGIC   "private_key_id": "3186adfba5836725d17bcbc9b3e5c519ac8fb94f",
// MAGIC   "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDdB1wroeikraJ5\nBCdS76X28Xkx+7/KFgbYeblGa/sOuScbdKawNloPag0PWxMAe1mAWTQerZaU6Z1z\nwucsD9PyZge+fWw78Fl24v/9oYa/j6a42o5ORYKpkzwA6YvVQ4Rc6CptdBy7K1md\nTh+iVnsuwc0PkpxLnHdpfsa9iV5tpPTLTE/L/J8jDZSmuGX2CAGfNPIAKR4zj5Nf\n59ppUP08MwwtzAl2Uop9IUMeAWn2miAVWgLIHEX8aM6SLwc1zAZ7BZC+gHyoiDvF\nxF6Px2O19/QERpBBmA3Zoe3BxHiXXC3Cd4ZUj9ybDMoF7WYooxVjdOjn8b1/yr/6\nQXsXTfIPAgMBAAECggEAWvF70bIpTJTrDHsSiRP1CEH8GX5ZwBdHEswm2Rx17O1u\n82OQcGG0tbHvlLlm/KREQ8SwZs0K79OdxvNTdfQ/Q3YKyqzFm3X+AaZOKThMfsZn\nFZlLw1XjeM2Ne1yc/g9tR2L5jhaf+b1Q/qIV7xBBWBBCxR0QdSX4li4lLPh4I4lf\nrK6iK/Ph3QdrQOcO5Ilfi/ZXwZIuAf/SUNgv82S+AzjQOJK2XT3dVQW89VEO4abv\nncP6QQz13Xbj+zOZXU87EMycZd5mBHHs9RLtzC0oGy7V0YWjyRLFjJ1RSb8I8sCM\nGXGomCk9ZT4OjpKu9ChGjMHT/p5NJNfrRj4Lc1Wm1QKBgQDyaXtzwV6q2xGzvLj/\nuku/X0Og+9oFStVU7AfHUAA2DhTUkQTb/p/bbfE9FJaLyyns9RsPt4bFUelUy4B2\n2ye4rKwddBxZ0Vd/ClU+IsHz9sGAgYiGeXaVcFGLO2qhv0LR2rdQ8UI6RpQcN3QN\n3Wn7I9LfNkiV1vBXrSzEwjg5zQKBgQDpawk105T/rdo4laJrvR3WjRHeNOcTNhLX\nEMzH4Vbg6uyEyrQ6nKZSi47YkQ1y5jx/7DEIGjQn6TNp03H+d3MDp+mwjsyftihr\n+FKMIAm4Xq1uDaEpFIZ5+pnLjok+ZdHawk45Ht/31ZgvdaZEtvudfOpWvWpxP3k0\nubt2AToPSwKBgFDqjN7SyTrW7U3FzSrvkKFMt1JhPyFNHXC/aMlLFwb0JFfgIHMe\nx1WDmWb+HFKFenUyS9ovNLlg4jX/x93kwZmPadRbEauZiU9Kr7GSAJPi5ixzEJNy\nr/aqC04igzGCmldaXp6SKb0yGfZhPEf0hI/kIVuIzynVGVzq3WrAOexxAoGBANgk\n7KdyYOQOCighsACSZCH8CdK5LE0m4nVSsj5ZFUk7YX9p8VDhJEcFwmYak9iVCOrw\nPUsicK+Qi7JwQXwBAnkdMRH0edlfJbktfssRE88tpO1nI5hV0Fz9yRKsz7v/Lz+i\ndCsCwyTNHpq8GBZDV2YzeRCPYBS0UTmYz9VrN2dXAoGBAJZisvvhy+RzzGbSMPxS\n65gwdqrt98VgZ5Gyjxu7zgPxZZT0dEiyBGrWRf1TfxZpSNrQUEB+DauV1FQ1Dl1G\nT4RwuZysxqnB0cFa2IRhJYScvS4fdXkk/wLvO2gxX2byjrvfeVTvm5ozP5m+yk6I\nDe/1n7kuwjsaDGwH3I3mxp1/\n-----END PRIVATE KEY-----\n",
// MAGIC   "client_email": "regsol@nu-documents-check.iam.gserviceaccount.com",
// MAGIC   "client_id": "107964271178351317542",
// MAGIC   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
// MAGIC   "token_uri": "https://oauth2.googleapis.com/token",
// MAGIC   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
// MAGIC   "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/regsol%40nu-documents-check.iam.gserviceaccount.com"
// MAGIC   }'''

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC ##convert to pandas
// MAGIC db_general_evolution = spark.table("pplp_monitoring_core__db_general_evolution") ## substituir pela sua base
// MAGIC #db_general_evolution = ("usr.purple_loop.pplp_monitoring_core__db_general_evolution")
// MAGIC pandas_df = db_general_evolution.toPandas()

// COMMAND ----------

// MAGIC %python
// MAGIC ##upload data to google sheets - the second argument is part of the sheet link after "d/" and before "/edit". The last argument is the sheet name. Before running this, you need to create a google sheet and share it with the service account email address. You can use this one regsol@nu-documents-check.iam.gserviceaccount.com
// MAGIC
// MAGIC upload_into_spreadsheet(google_suite_credentials, "1jL6EjiUV59yfOcTtbPU5O389MxWa7k8MgH-2UC4SCI0", pandas_df, 'DB_monthlyEvolutionGeneral')

// COMMAND ----------

// MAGIC %md
// MAGIC