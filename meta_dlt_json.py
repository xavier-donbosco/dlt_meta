# Databricks notebook source
# MAGIC %pip install dlt-meta

# COMMAND ----------

onboarding_json_path = "/mnt/meta/onboarding.json"
bronze_database      = "bronze_it_001"
onboarding_spec_path = "/mnt/meta/json/onboarding/"

# COMMAND ----------

onboarding_json_file = """[
   {
      "data_flow_id": "100",
      "data_flow_group": "A1",
      "source_system": "MYSQL",
      "source_format": "delta",
      "source_details": {
         "source_database": "APP",
         "source_table": "CUSTOMERS",
         "source_path_it": "/mnt/meta/source/dim_customers.csv"
      },
      "bronze_database_dev": "bronze_it_001",
      "bronze_table": "customers_cdc",
      "bronze_table_path_dev" : "/mnt/meta/bronze/",
      "bronze_reader_options": {
         "header": "true"
      }
   }
]"""

# COMMAND ----------

dbutils.fs.put(onboarding_json_path, onboarding_json_file, True)

# COMMAND ----------

onboarding_params_map = {
                      "onboarding_file_path":onboarding_json_path,
                      "database":bronze_database,
                      "env":"dev", 
                      "bronze_dataflowspec_table":"bronze_dataflowspec_tablename",
                      "bronze_dataflowspec_path":onboarding_spec_path,
                      "overwrite":"True",
                      "version":"v1",
                      "import_author":"Xavi",
}
print(onboarding_params_map)

from src.onboard_dataflowspec import OnboardDataflowspec
OnboardDataflowspec(spark, onboarding_params_map).onboard_bronze_dataflow_spec()

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;
# MAGIC show tables in bronze_it_001;
# MAGIC select * from bronze_it_001.bronze_dataflowspec_tablename;

# COMMAND ----------

# "bronze_dataflowspec_path": bronze_dataflowspec_path,
