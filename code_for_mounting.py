# Databricks notebook source
# DBTITLE 1,needed parameters
container_name       = "dlt-meta"
storage_account_name = "dgbgen2"
mounting_point_name  = "/mnt/meta/"
storage_account_key  = "1e1yBukbRYxuRoKXava/eS5fnyKX/orNHms0F1Zi+c2h1SJKgk13MqG7Y1ugsFa+VUOT4Rbyd+YK+ASt6w1liQ=="

# COMMAND ----------

# DBTITLE 1,mounting
dbutils.fs.mount(source = f'wasbs://{container_name}@{storage_account_name}.blob.core.windows.net',
                 mount_point = mounting_point_name,
                 extra_configs = {f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net':storage_account_key}
)

# COMMAND ----------

# MAGIC %fs ls /mnt/meta/data/source

# COMMAND ----------

spark.read.option("header", True).format("csv").load("/mnt/meta/data/source/Address.csv").display()

# COMMAND ----------

spark.read.option("header", True).format("csv").load("dbfs:/mnt/meta/data/source/Drivers.csv").display()

# COMMAND ----------

spark.read.option("header", True).format("csv").load("/mnt/meta/data/source/users.csv").select('user_id').distinct().display()

# COMMAND ----------


