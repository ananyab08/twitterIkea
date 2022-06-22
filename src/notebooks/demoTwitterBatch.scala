// Databricks notebook source
import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{lit, _}
import scala.collection.JavaConverters._

//set spark.sql.hive.convertMetastoreParquet = false

/* Preparing connection for twitter dev account */
val twitterConsumerKey = dbutils.widgets.get("varTwitterConsumerKey")
val twitterConsumerSecret = dbutils.widgets.get("varTwitterConsumerSecret")
val twitterOauthAccessToken = dbutils.widgets.get("varTwitterOauthAccessToken")
val twitterOauthTokenSecret = dbutils.widgets.get("varTwitterOauthTokenSecret")


val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    .setOAuthConsumerKey(twitterConsumerKey)
    .setOAuthConsumerSecret(twitterConsumerSecret)
    .setOAuthAccessToken(twitterOauthAccessToken)
    .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

val twitterFactory = new TwitterFactory(cb.build())
val twitter = twitterFactory.getInstance()

/* Getting tweets with keyword "IKEA" and getting related tweets */
//  query.setCount(100)
val queryExp =  "IKEA"
val query = new Query(queryExp).lang("en")
val result = twitter.search(query)
val statuses = result.getTweets()

/* Creating sparkSession */
val spark: SparkSession = SparkSession.builder()
  .appName("tweetBatch")
  .getOrCreate()

/* Creating the dataframe structure */
var tweetDf = spark.sql(s"""select * from ikea_twitter_batch_output limit 1""")

tweetDf.printSchema()

/* getting attribute for each tweet */
    for(status <- statuses.asScala) {
      val id = status.getId().toString()  
      val text = status.getText()
      val createdAt = status.getCreatedAt().toString()
      val lang = status.getLang()
      val location = status.getUser().getLocation()
      var reTweet = "N"
      var reTweetCount = "0"
      if(status.isRetweet())
      {
         reTweet = "Y"
         reTweetCount = status.getRetweetCount().toString()
      } 

 /* Assigning derived data to dtaframe for each tweet */          
 val iterDf = Seq(Row(id,createdAt,text,lang,location,reTweet,reTweetCount))
      
 val tweetSchema = StructType(
  List(
    StructField("id", StringType, nullable = false),    
    StructField("createdAt", StringType, nullable = false),
    StructField("text", StringType, nullable = false),
    StructField("lang", StringType, nullable = false),
    StructField("location",StringType, nullable = false),
    StructField("reTweet",StringType, nullable = false),
    StructField("reTweetCount",StringType, nullable = false)
    ))

 val finalDf = spark.createDataFrame(
  spark.sparkContext.parallelize(iterDf),
  StructType(tweetSchema)
)

/* combining all data */
 tweetDf = tweetDf.union(finalDf)
}
 tweetDf.show(2)
/* write data on dbfs for batch mode processing for data analysis*/

 tweetDf.write.mode("append").parquet("/FileStore/tables/ikea_twitter_batch_output")
     


// COMMAND ----------

