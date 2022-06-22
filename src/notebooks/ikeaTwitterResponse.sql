-- Databricks notebook source
/* creating external hive table on parquet file from dbfs path */
--drop table ikea_twitter_response;
create external table if not exists ikea_twitter_response
(id string, created_at string, text string, lang string, location string)
  PARTITIONED BY (tweet_date string, product string)
  STORED AS PARQUET
  LOCATION '/FileStore/tables/ikea_twitter_response/'; 

-- COMMAND ----------

/* refreshing hive table post data chnages by workflow */
msck repair table ikea_twitter_response;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC //dbutils.fs.rm("/FileStore/tables/ikea_tweet", true)
-- MAGIC //dbutils.fs.mv("/FileStore/tables/ikea_tweet", "/FileStore/tables/ikea_twitter_response", recurse=true)
-- MAGIC //dbutils.fs.rm("/FileStore/tables/ikea_tweet/ikea_twitter_batch_output", true)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val ParquetDF = spark.read.parquet("/FileStore/tables/ikea_tweet/")
-- MAGIC     ParquetDF.show(false);

-- COMMAND ----------

select distinct id, created_at, text, lang, location, product from ikea_twitter_response 
where tweet_date = '2022-06-21' order by id;