package ucm.socialbd.com.kafkaclient

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Created by Jeff on 09/03/2017.
  */
object KafkaProducerTwitter extends App{

  val consumerKey = "Q1rTYz2S8jar59EcOI47K78K0"
  val consumerSecret = "Xn6sdt66aqyqEPdDt3TyUV2FUAxwhapVGoh9ZLIdTvpwLCi6wi"
  val accessToken = "826476918987948032-FdfoSLaZz6zjao3KSwnsOQMoL9yzDn9"
  val accessSecret = "JmBjXrizj3GuDlz2ubE9B9XELbUWNXQz8dV9nZkMiNs2s"
  val kafkaTweetTopic = "tweets"
  //configure kafka producer client
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("key-class-type", "java.lang.String")
  props.put("value-class-type", "java.lang.String")

  val producer = new KafkaProducer[String, String](props)

  //configure twitter client
  val twitterConfig = TwitterUtils.config(
    consumerKey,
    consumerSecret,
    accessToken,
    accessSecret
  )

  //setup twitter stream listener which publishes to kafka using the producer we just made
  val twitterListener = TwitterUtils.simpleStatusListener(
    producer,
    kafkaTweetTopic
  )
/*
  //create the stream
  val stream = TwitterUtils.createStream(
    twitterConfig,
    twitterListener
  )*/

  val filterMadrid= Array(
    Array(-3.703,  40.4167754),
    Array(-3.103, 40.616))

  val filterSpain= Array(
    Array(-7.0,  28.0),
    Array(-3.103 , 42.616))

  val stream = TwitterUtils.createStreamLocation(
    twitterConfig,
    twitterListener,
    filterSpain
  )
}
