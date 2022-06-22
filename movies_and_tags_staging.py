# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS asos_test;
# MAGIC USE asos_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a table for movies and tags
# MAGIC CREATE TABLE IF NOT EXISTS asos_test.movies 
# MAGIC   (movieId BIGINT, title STRING, genres STRING)
# MAGIC USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS asos_test.tags 
# MAGIC   (userId BIGINT, movieId BIGINT, tag STRING, `timestamp` BIGINT)
# MAGIC USING DELTA;

# COMMAND ----------

# Reading data from ratings.csv
movies_df = spark.read.format("csv") \
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/movies.csv")

movies_df.createOrReplaceTempView("movies_view")


tags_df = spark.read.format("csv") \
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/tags.csv")

tags_df.createOrReplaceTempView("tags_view")



# COMMAND ----------

# MAGIC %sql
# MAGIC insert into asos_test.movies select * from movies_view;
# MAGIC insert into asos_test.tags select * from tags_view;

# COMMAND ----------

# checking if count is same in source file and target table
assert spark.table("asos_test.movies").count() == spark.read.format("csv").option("header","true").load("/FileStore/tables/movies.csv").count(), "It should have same count"
assert spark.table("asos_test.tags").count() == spark.read.format("csv").option("header","true").load("/FileStore/tables/tags.csv").count(), "It should have same count"
