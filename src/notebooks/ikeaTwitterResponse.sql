-- Databricks notebook source
/* creating external hive table on parquet file from dbfs path */

drop table if exists ikea_twitter_response;

create external table if not exists ikea_twitter_response
(id string, created_at string, text string)
  PARTITIONED BY (tweet_date string, product string)
  STORED AS PARQUET
  LOCATION '/FileStore/tables/ikea_tweet/'; 

-- COMMAND ----------

/* refreshing hive table post data chnages by workflow */
msck repair table ikea_twitter_response;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val ParquetDF = spark.read.parquet("/FileStore/tables/ikea_tweet/")
-- MAGIC     ParquetDF.show(false);

-- COMMAND ----------

select distinct id, created_at, text, product, tweet_date from ikea_twitter_response order by id;