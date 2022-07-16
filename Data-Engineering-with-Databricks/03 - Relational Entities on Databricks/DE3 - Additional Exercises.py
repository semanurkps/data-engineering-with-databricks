# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Create Table form csv

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW imdb_trial USING CSV OPTIONS (
# MAGIC   path = 'dbfs:/FileStore/tables/imdb_prep.csv',
# MAGIC   header = "true",
# MAGIC   mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC );
# MAGIC SELECT * FROM  imdb_trial

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Connect to JDBC Driver

# COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:/${da.username}_ecommerce.db",
  dbtable = "users"
)

# COMMAND ----------


