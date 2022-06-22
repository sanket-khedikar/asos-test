# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS asos_test;
# MAGIC USE asos_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a partitioned table for ratings
# MAGIC CREATE TABLE IF NOT EXISTS asos_test.ratings 
# MAGIC   (userId INT, movieId INT, rating DOUBLE, `timestamp` BIGINT)
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (rating);

# COMMAND ----------

# Reading data from ratings.csv
ratings_df = spark.read.format("csv") \
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/ratings.csv")


ratings_df.createOrReplaceTempView("ratings_view")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO asos_test.ratings r
# MAGIC USING ratings_view v
# MAGIC ON r.userId = v.userId and r.movieId = v.movieId
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# checking if count is same in source file and target table
assert spark.table("asos_test.ratings").count() == spark.read.format("csv").option("header","true").load("/FileStore/tables/ratings.csv").count(), "It should have same count"
