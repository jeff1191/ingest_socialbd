package ucm.socialbd.com.kafkaclient

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

/**
  * Created by Jeff on 09/03/2017.
  */
object TwitterUtils {

  def createStreamLocation(twitterConfig: ConfigurationBuilder,
                           twitterListener: StatusListener, filterPlace: Array[Array[Double]]): Unit = {

    val twitterStream = new TwitterStreamFactory(twitterConfig.build()).getInstance
    twitterStream.addListener(twitterListener)

    val place = filterPlace

    twitterStream.filter(new FilterQuery().locations(place))

   // Thread.sleep(50000)
    //twitterStream.cleanUp()
    //twitterStream.shutdown
  }


  //configure the twitter api (using twitter4j)
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

  /**
    * Create a twitter stream with the given twitter api config and listener (which publishes to kafka)
    *
    * @param config   Twitter api config created using api secret keys
    * @param listener Listener which publishes to kafka.
    */
  def createStream(config: ConfigurationBuilder,
                   listener: StatusListener): Unit = {

    val twitterStream = new TwitterStreamFactory(config.build()).getInstance

    twitterStream.addListener(listener)

    twitterStream.sample()
  }

  /**
    * For a given tweet, publish it with the given producer and topic
    *
    * @param kafkaProducer kafka producer
    * @param kafkaTopic    kafka topic
    * @param status         tweet to publish
    */
  def publishTweet(kafkaProducer: KafkaProducer[String, String],
                   kafkaTopic: String,
                   status: Status): Unit = {

    //create publishable message
    /*val tweetMessage = new ProducerRecord[String, String](kafkaTopic,
      "\n\n\n" + "Idioma: "+ status.getLang + "\nUsuario: " +status.getUser.getName +
        "\nCiudad: " + status.getPlace.getName + " \nTexto:" + status.getText )*/

    val tweetMessage = new ProducerRecord[String, String](kafkaTopic,copyToJsonString(status))
    kafkaProducer.send(tweetMessage)
  }


  /**
    * Twitter api listener i.e. a place to define what to do when a new tweet comes in.
    * @return twitter listener
    */
  def simpleStatusListener(kafkaProducer: KafkaProducer[String, String],
                           kafkaTopic: String): StatusListener = {

    new StatusListener() {

      override def onStallWarning(warning: StallWarning): Unit = {}

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}

      //when a tweet comes in get the text and publish to kafka
      override def onStatus(status: Status): Unit = {

        //val tweetText =  "Usuario: " + status.getUser.getName + "\nTexto: " + status.getText + "\n\n"
        val tweet_raw = status.getUser + "\n\n"
        //val tweet_raw =  TwitterObjectFactory.getRawJSON(status)
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
    new Gson().toJson(status) + "\n\n\n"
  }
}