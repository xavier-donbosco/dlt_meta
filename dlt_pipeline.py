# Databricks notebook source
# DBTITLE 1,Import Statements
import dlt
from datetime import date
from pyspark.sql.functions import col,lit

# COMMAND ----------

# DBTITLE 1,Needed Parameters
today = date.today()
ingestion_date   = today.strftime("%Y-%m-%d")
dim_users_path   = "/mnt/meta/data/source/dimension_1_users.csv"
dim_address_path = "/mnt/meta/data/source/dimension_2_Address.csv"
dim_menu_path    = "/mnt/meta/data/source/dimension_3_Menu.csv"
dim_restaurant_path = "/mnt/meta/data/source/dimension_4_Restaurants.csv"
dim_drivers_path = "/mnt/meta/data/source/dimension_5_Drivers.csv"
fact_1_path      = "/mnt/meta/data/source/fact_table_1.csv"
fact_2_path      = "/mnt/meta/data/source/fact_table_2.csv"
fact_3_path      = "/mnt/meta/data/source/fact_table_3.csv"
config           = {"header", True}
bronze_load_date_clm = "ingestion_date"
silver_load_date_clm = "load_date"

# COMMAND ----------

# DBTITLE 1,Common Functions
def read_csv(path, config):
    return spark.read.option(*config).csv(path)

def add_load_date(df, load_date, clm_name):
    return df.withColumn(clm_name, lit(load_date))

# COMMAND ----------

# DBTITLE 1,Source to Bronze
@dlt.table(
  comment="The raw dim user dataset, ingested from adls."
)
def t_stream_bronze_users():
  return add_load_date(read_csv(dim_users_path, config), today, bronze_load_date_clm)

@dlt.table(
  comment="The raw dim address dataset, ingested from adls."
)
def t_stream_bronze_address():
  return add_load_date(read_csv(dim_address_path, config), today, bronze_load_date_clm)

@dlt.table(
  comment="The raw dim menu dataset, ingested from adls."
)
def t_stream_bronze_menu():
  return add_load_date(read_csv(dim_menu_path, config), today, bronze_load_date_clm)

@dlt.table(
  comment="The raw dim restaurant dataset, ingested from adls."
)
def t_stream_bronze_restaurant():
  return add_load_date(read_csv(dim_restaurant_path, config), today, bronze_load_date_clm)

@dlt.table(
  comment="The raw dim drivers dataset, ingested from adls."
)
def t_stream_bronze_drivers():
  return add_load_date(read_csv(dim_drivers_path, config), today, bronze_load_date_clm)

@dlt.table(
  comment="The raw fact 1 dataset, ingested from adls."
)
def t_stream_bronze_fact_1():
  return add_load_date(read_csv(fact_1_path, config), today, bronze_load_date_clm)

@dlt.table(
  comment="The raw fact 2 dataset, ingested from adls."
)
def t_stream_bronze_fact_2():
  return add_load_date(read_csv(fact_2_path, config), today, bronze_load_date_clm)

@dlt.table(
  comment="The raw fact 3 dataset, ingested from adls."
)
def t_stream_bronze_fact_3():
  return add_load_date(read_csv(fact_3_path, config), today, bronze_load_date_clm)

# COMMAND ----------

@dlt.table(
  comment="dim users - data quality of passed records"
)

@dlt.expect_all_or_drop({"valid_record": "user_id IS NOT NULL AND name IS NOT NULL AND email IS NOT NULL AND password IS NOT NULL AND phone IS NOT NULL"})

def t_stream_silver_users():
  return add_load_date(dlt.read("t_stream_bronze_users").drop(bronze_load_date_clm), today, silver_load_date_clm)

@dlt.table(
  comment="dim address - data quality of passed records"
)

@dlt.expect_all_or_drop({"valid_record": "address_id IS NOT NULL AND user_id IS NOT NULL AND state IS NOT NULL AND city IS NOT NULL AND street IS NOT NULL AND pincode IS NOT NULL"})

def t_stream_silver_address():
  return add_load_date(dlt.read("t_stream_bronze_address").drop(bronze_load_date_clm), today, silver_load_date_clm)

@dlt.table(
  comment="dim users - data quality of passed records"
)

@dlt.expect_all_or_drop({"valid_record": "menu_id IS NOT NULL AND restaurant_id IS NOT NULL AND item_name IS NOT NULL AND price IS NOT NULL"})

def t_stream_silver_menu():
  return add_load_date(dlt.read("t_stream_bronze_menu").drop(bronze_load_date_clm), today, silver_load_date_clm)

@dlt.table(
  comment="dim restaurant - data quality of passed records"
)

@dlt.expect_all_or_drop({"valid_record": "restaurant_id IS NOT NULL AND name IS NOT NULL AND address IS NOT NULL AND phone IS NOT NULL"})

def t_stream_silver_restaurant():
  return add_load_date(dlt.read("t_stream_bronze_restaurant").drop(bronze_load_date_clm), today, silver_load_date_clm)

@dlt.table(
  comment="dim drivers - data quality of passed records"
)

@dlt.expect_all_or_drop({"valid_record": "driver_id IS NOT NULL AND name IS NOT NULL AND location IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL"})

def t_stream_silver_drivers():
  return add_load_date(dlt.read("t_stream_bronze_drivers").drop(bronze_load_date_clm), today, silver_load_date_clm)

@dlt.table(
  comment="fact 1 - data quality of passed records"
)

@dlt.expect_all_or_drop({"valid_record": "rating_id IS NOT NULL AND user_id IS NOT NULL AND restaurant_id IS NOT NULL AND rating IS NOT NULL"})

def t_stream_silver_fact_1():
  return add_load_date(dlt.read("t_stream_bronze_fact_1").drop(bronze_load_date_clm), today, silver_load_date_clm)

@dlt.table(
  comment="fact 2 - data quality of passed records"
)

@dlt.expect_all_or_drop({"valid_record": "payment_id IS NOT NULL AND payment_method IS NOT NULL AND amount IS NOT NULL AND status IS NOT NULL"})

def t_stream_silver_fact_2():
  return add_load_date(dlt.read("t_stream_bronze_fact_2").drop(bronze_load_date_clm), today, silver_load_date_clm)

@dlt.table(
  comment="fact 3 - data quality of passed records"
)

@dlt.expect_all_or_drop({"valid_record": "order_id IS NOT NULL AND user_id IS NOT NULL AND restaurant_id IS NOT NULL AND order_total IS NOT NULL AND delivery_status IS NOT NULL AND driver_id IS NOT NULL"})

def t_stream_silver_fact_3():
  return add_load_date(dlt.read("t_stream_bronze_fact_3").drop(bronze_load_date_clm), today, silver_load_date_clm)
