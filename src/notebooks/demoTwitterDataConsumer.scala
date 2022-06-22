// Databricks notebook source
/** Consumer Notebook **/

import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/* Preparing connection for Event Hub  */

val namespaceName = dbutils.widgets.get("varNamespaceName")
val eventHubName = dbutils.widgets.get("varEventHubName")
val sasKeyName = dbutils.widgets.get("varSasKeyName")
val sasKey = dbutils.widgets.get("varSasKey")

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

incomingStream.printSchema()

//incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
/* getting event messages and preparing output*/
val messages =
  incomingStream
  .withColumn("id", $"offset".cast(StringType))
  .withColumn("created_at", $"enqueuedTime".cast(StringType))
  .withColumn("fullText", $"body".cast(StringType))
  .select("id", "created_at", "fullText")

//messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

val updMessage = messages.withColumn("text", split($"fullText","::ab").getItem(0))
.withColumn("lang", split($"fullText","::ab").getItem(1))
.withColumn("location", split($"fullText","::ab").getItem(2))

/* additional logic to get any new or existing product mention */

val productMessage = updMessage.withColumn("product", 
      expr("case when lower(text) like '%furniture%' then 'Furniture'" +
      "when lower(text) like '%food%' then 'Food'"+
      "when lower(text) like '%kitchen%' or lower(text) like '%dining%' then 'Kitchen & Dining'"+
      "when lower(text) like '%living%' then 'Living Room'"+
      "when lower(text) like '%bed%' or lower(text) like '%bathroom%' then 'Bed & Bath Room'"+
      "else 'others' end"))
  .withColumn("tweet_date",expr("subString(created_at,1,10)"))
  .select("id", "created_at", "text", "lang","location","tweet_date","product")

dbutils.fs.rm("/user/root/Checkpoints2/", true)

/* Writing output to parquet file at dbfs path */
productMessage.repartition(2).writeStream.outputMode("append").partitionBy("tweet_date","product").format("parquet").option("path","/FileStore/tables/ikea_twitter_response/").option("checkpointLocation", "/user/root/Checkpoints2").start()

//productMessage.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
