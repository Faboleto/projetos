# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pathlib import Path
import sys
sys.path.append(str(Path.cwd().parent))

from modelspace import MaterializedModel, ViewModel, Connector

# COMMAND ----------

user_acquisition_daily = MaterializedModel(
    schema='looker',
    table='user_acquisition_daily',
    connector=Connector.POSTGRES,
    model_query_path='queries/user_acquisition_daily.sql',
    post_creation_commands=[
        "CREATE INDEX acquisition_daily_index ON looker.user_acquisition_daily (_date, medium_agg, source, campaign_agg)",
        "GRANT SELECT ON looker.user_acquisition_daily TO limited_user"
    ])
    
user_acquisition_daily.build()


