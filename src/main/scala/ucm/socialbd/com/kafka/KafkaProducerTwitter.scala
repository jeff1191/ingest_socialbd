package ucm.socialbd.com.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.utils.{SocialBDConfig, TwitterUtils}

/**
  * Created by Jeff on 09/03/2017.
  */
class KafkaProducerTwitter(socialBDProperties: SocialBDProperties) extends KafkaProducerActions{

  override def process(): Unit = {
    val  props = SocialBDConfig.getProperties(socialBDProperties)

    val producer = new KafkaProducer[String, String](props)

    //configure twitter client
    val twitterConfig = TwitterUtils.config(
      socialBDProperties.twitterConf.consumerKey,
      socialBDProperties.twitterConf.consumerSecret,
      socialBDProperties.twitterConf.accessToken,
      socialBDProperties.twitterConf.accessSecret
    )
    //create a listener with a kafka-producer
    val twitterListener = TwitterUtils.simpleStatusListener(
      producer,
      socialBDProperties.twitterConf.twitterTopic
    )
    //Add filters needed
    val filterMadrid= Array(
      Array(-3.703,  40.4167754),
      Array(-3.103, 40.616))

    val filterSpain= Array(
      Array(-7.0,  28.0),
      Array(-3.103 , 42.616))

    //finally, create a stream using twitterConfig, twitterListener and the filters, its important consider that the
    //listener wraps a kafka-producer
    val stream = TwitterUtils.createStreamLocation(
      twitterConfig,
      twitterListener,
      filterSpain
    )
  }
}
