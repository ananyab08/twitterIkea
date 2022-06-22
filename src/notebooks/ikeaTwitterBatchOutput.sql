-- Databricks notebook source
--drop table ikea_twitter_batch_output;
create external table if not exists default.ikea_twitter_batch_output
(id string, created_at string, text string, lang string, location string, reTweet string,reTweetCount string)
  STORED AS PARQUET
  LOCATION '/FileStore/tables/ikea_twitter_batch_output/'; 

-- COMMAND ----------

select * from ikea_twitter_batch_output where lang='en';
