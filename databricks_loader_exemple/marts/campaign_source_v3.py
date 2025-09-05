# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC from modelspace.materialized import MaterializedModel

# COMMAND ----------

dim_user_campaign_source_v3 = MaterializedModel(
    schema='looker',
    table='dim_user_campaign_source_v3',
    model_query_path='queries/dim_user_campaign_source_v3.sql',
    post_creation_commands=[
        "CREATE INDEX date_medium_source_campaign ON looker.dim_user_campaign_source_v3 (created_at, medium_agg, source, campaign)",
        "GRANT SELECT ON looker.dim_user_campaign_source_v3 TO limited_user"
    ]
)

dim_user_campaign_source_v3.build()

