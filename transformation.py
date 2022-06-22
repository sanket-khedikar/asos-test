# Databricks notebook source
import pyspark.sql.functions as F
transf_df = spark.sql("select * from asos_test.movies")
transf_df = transf_df.withColumn("genres",F.explode(F.split(F.col("genres"),'\|')))

#display(transf_df)
transf_df.repartition(1).write.format("csv").mode("overwrite").option("header","true").save("/FileStore/tables/transformed_data/")

# COMMAND ----------


avg_rating_df = spark.sql("""with filtered_data as
(
SELECT distinct movieId, avg(rating) AS avg_rating, count(rating) as rating_cnt
FROM asos_test.ratings
GROUP BY movieId
)
select m.title from filtered_data f
inner join asos_test.movies m
on f.movieId = m.movieId
where f.rating_cnt >=5 order by f.avg_rating desc limit 10""")
avg_rating_df.repartition(1).write.format("csv").mode("overwrite").option("header","true").save("/FileStore/tables/top_ten_films_data/")

# COMMAND ----------

# Checking if query and output file has same records or not
assert spark.sql("""with filtered_data as
(
SELECT distinct movieId, avg(rating) AS avg_rating, count(rating) as rating_cnt
FROM asos_test.ratings
GROUP BY movieId
)
select m.title from filtered_data f
inner join asos_test.movies m
on f.movieId = m.movieId
where f.rating_cnt >=5 order by f.avg_rating desc limit 10""").collect() == spark.read.format("csv").option("header","true").load("/FileStore/tables/top_ten_films_data/").collect(), "Records are not matching"

