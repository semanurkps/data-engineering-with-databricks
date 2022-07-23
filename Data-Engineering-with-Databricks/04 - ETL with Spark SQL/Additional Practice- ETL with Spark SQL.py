# Databricks notebook source
# MAGIC %md
# MAGIC We will try to read the databricks raw kafka jsons in the directory: dbfs:/user/semanur.kapusizoglu@bs.nttdata.com/dbacademy/dewd/source/eltwss   

# COMMAND ----------

file_API_path = "/dbfs/user/semanur.kapusizoglu@bs.nttdata.com/dbacademy/dewd/source/eltwss/raw/events-kafka"
spark_path = "dbfs:/user/semanur.kapusizoglu@bs.nttdata.com/dbacademy/dewd/source/eltwss/raw/events-kafka"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's list all the files in the path. There are different ways to do thet

# COMMAND ----------

import os 

os.listdir(file_API_path)

# COMMAND ----------

display(dbutils.fs.ls(spark_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/user/semanur.kapusizoglu@bs.nttdata.com/dbacademy/dewd/source/eltwss/raw/events-kafka/000.json`

# COMMAND ----------

query = f"SELECT * FROM json.`{spark_path}`"
display(spark.sql(query))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC help

# COMMAND ----------


