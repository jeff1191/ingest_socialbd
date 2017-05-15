package ucm.socialbd.com.utils

import java.util.Properties

import ucm.socialbd.com.config.SocialBDProperties

/**
  * Created by Jeff on 16/04/2017.
  */
object SocialBDConfig {

  def getProperties(socialBDProperties: SocialBDProperties): Properties  = {
    val properties = new Properties()
    // comma separated list of Kafka brokers
    properties.setProperty("bootstrap.servers", socialBDProperties.urlKafka)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("key-class-type", "java.lang.String")
    properties.put("value-class-type", "java.lang.String")
    properties
  }
}
