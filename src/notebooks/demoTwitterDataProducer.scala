// Databricks notebook source
/** Producer Notebook **/

import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder
import scala.collection.mutable.HashMap


/* Preparing connection for Event Hub  */


val namespaceName = dbutils.widgets.get("varNamespaceName")
val eventHubName = dbutils.widgets.get("varEventHubName")
val sasKeyName = dbutils.widgets.get("varSasKeyName")
val sasKey = dbutils.widgets.get("varSasKey")

val connStr = new ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey) 

/* Creating pool to keep the even hub connection alive during streaming */
val pool = Executors.newScheduledThreadPool(1)
val eventHubClient = EventHubClient.createFromConnectionString(connStr.toString(), pool)

/* UDF method to handle delay if required*/
def sleep(time: Long): Unit = Thread.sleep(time)
def sendEvent(message: String, delay: Long) = {
    sleep(delay)
    val messageData = EventData.create(message.getBytes("UTF-8"))
    eventHubClient.get().send(messageData)
   System.out.println("Sent event: " + message + "\n")
}

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

/* Getting tweets with keyword "IKEA" and sending them to the Event Hub in realtime */
val query = new Query("IKEA")
    query.setCount(100)
    //query.lang("en")
var finished = false
while (!finished) {
    val result = twitter.search(query)
    val statuses = result.getTweets()
    var lowestStatusId = Long.MaxValue
    for(status <- statuses.asScala) {
       if(!status.isRetweet()){
         
         val text = status.getText()
         val lang = status.getLang()
         val location = status.getUser().getLocation()
         var tweetStr = text+"::ab"+lang+"::ab"+location
         
         sendEvent(tweetStr, 0)
    }
         
    lowestStatusId = Math.min(status.getId(), lowestStatusId)
   
   }
   query.setMaxId(lowestStatusId - 1)
  } 

/* Closing connection to the Event Hub */
eventHubClient.get().close()
