package ucm.socialbd.com.utils

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

/**
  * Created by Jeff on 09/03/2017.
  * The listener(simpleStatusListener) envolve a kafka-producer that contains a message(tweet)
  */
object TwitterUtils {
  /**
    * Create a location twitter stream
    */
  def createStreamLocation(twitterConfig: ConfigurationBuilder,
                           twitterListener: StatusListener, filterPlace: Array[Array[Double]]): Unit = {
    val twitterStream = new TwitterStreamFactory(twitterConfig.build()).getInstance
    twitterStream.addListener(twitterListener)
    val place = filterPlace
    twitterStream.filter(new FilterQuery().locations(place))
  }
  /**
    * Create a simple twitter stream
    */
  def createStream(config: ConfigurationBuilder,
                   listener: StatusListener): Unit = {
    val twitterStream = new TwitterStreamFactory(config.build()).getInstance
    twitterStream.addListener(listener)
    twitterStream.sample()
  }
  //properties of project
  def config(consumerKey: String,
             consumerSecret: String,
             accessToken: String,
             accessTokenSecret: String): ConfigurationBuilder = {

    new ConfigurationBuilder()
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
  }

  //publish a message in Kafka
  def publishTweet(kafkaProducer: KafkaProducer[String, String],
                   kafkaTopic: String,
                   status: Status): Unit = {
    val msg = copyToJsonString(status)
    println(msg)
    val tweetMessage = new ProducerRecord[String, String](kafkaTopic,msg)

    kafkaProducer.send(tweetMessage)
  }

  //twitter listener
  def simpleStatusListener(kafkaProducer: KafkaProducer[String, String],
                           kafkaTopic: String): StatusListener = {

    new StatusListener() {
      override def onStallWarning(warning: StallWarning): Unit = {}
      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
      //when a tweet comes in get the text and publish to kafka
      override def onStatus(status: Status): Unit = {


        publishTweet(
          kafkaProducer,
          kafkaTopic,
          status
        )
      }
      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
      override def onException(ex: Exception): Unit = {}
    }
  }
  def copyToJsonString(status: Status ): String ={
    new Gson().toJson(status) + ""
  }
}