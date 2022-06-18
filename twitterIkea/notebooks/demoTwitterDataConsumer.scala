// Databricks notebook source
/** Consumer Notebook **/

import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/* Preparing connection for Event Hub  */

val namespaceName = namespaceName
val eventHubName = eventHubName
val sasKeyName = sasKeyName
val sasKey = sasKey

val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)

val customEventhubParameters =
  EventHubsConf(connStr.toString())
  .setMaxEventsPerTrigger(5)

/* Reading stream from Event Hub */
val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

/* getting event messages and preparing output*/
val messages =
  incomingStream
  .withColumn("id", $"offset".cast(StringType))
  .withColumn("created_at", $"enqueuedTime".cast(StringType))
  .withColumn("text", $"body".cast(StringType))
  .select("id", "created_at", "text")

/* additional logic to get any new or existing product mention */
val productMessage = messages.withColumn("product", 
      expr("case when lower(text) like '%furniture%' then 'Furniture'" +
      "when lower(text) like '%food%' then 'Food'"+
      "when lower(text) like '%kitchen%' or lower(text) like '%dining%' then 'Kitchen & Dining'"+
      "when lower(text) like '%living%' then 'Living Room'"+
      "when lower(text) like '%bed%' or lower(text) like '%bathroom%' then 'Bed & Bath Room'"+
      "else 'others' end"))
  .withColumn("tweet_date",expr("subString(created_at,1,10)"))
.select("id", "created_at", "text", "tweet_date","product")

dbutils.fs.rm("/user/root/Checkpoints2/", true)

/* Writing output to parquet file */
productMessage.repartition(2).writeStream.outputMode("append").partitionBy("tweet_date","product").format("parquet").option("path","/FileStore/tables/ikea_tweet/").option("checkpointLocation", "/user/root/Checkpoints2").start()

