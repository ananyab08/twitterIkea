// Databricks notebook source
/** Consumer Notebook **/

import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Build connection string with the above information
val namespaceName = "twitterStream"
val eventHubName = "streamdatademo"
val sasKeyName = "twitter_stream_data_eh_sap"
val sasKey = "kJ5Wc0y571rt8vW1NybM0ohUOQQDJsUGtAMYSWmUFGs="

val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val customEventhubParameters =
  EventHubsConf(connStr.toString())
  .setMaxEventsPerTrigger(5)

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

val messages =
  incomingStream
  .withColumn("id", $"offset".cast(StringType))
  .withColumn("created_at", $"enqueuedTime".cast(StringType))
  .withColumn("text", $"body".cast(StringType))
  .select("id", "created_at", "text")

val productMessage = messages.withColumn("product", 
      expr("case when lower(text) like '%furniture%' then 'Furniture'" +
      "when lower(text) like '%food%' then 'Food'"+
      "when lower(text) like '%kitchen%' or lower(text) like '%dining%' then 'Kitchen & Dining'"+
      "when lower(text) like '%living%' then 'Living Room'"+
      "when lower(text) like '%bed%' or lower(text) like '%bathroom%' then 'Bed & Bath Room'"+
      "else 'others' end"))
  .withColumn("tweet_date",expr("subString(created_at,1,10)"))
.select("id", "created_at", "text", "tweet_date","product")

//productMessage.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
dbutils.fs.rm("/user/root/Checkpoints2/", true)
productMessage.repartition(2).writeStream.outputMode("append").partitionBy("tweet_date","product").format("parquet").option("path","/FileStore/tables/ikea_tweet/").option("checkpointLocation", "/user/root/Checkpoints2").start()



// COMMAND ----------


    //dbutils.fs.rm("/FileStore/tables/ikea_tweet", true)
    