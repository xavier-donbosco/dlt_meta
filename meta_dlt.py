# Databricks notebook source
# MAGIC %pip install dlt-meta

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
      "bronze_database_it": "bronze_it_001",
      "bronze_table": "customers_cdc",
      "bronze_reader_options": {
         "header": "true"
      }
   }
]"""
