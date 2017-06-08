package ucm.socialbd.com.cluster

import java.util.Properties

import com.github.sakserv.minicluster.impl.KafkaLocalBroker
import ucm.socialbd.com.config.SocialBDProperties

/**
  * Created by Jeff on 24/04/2017.
  */
object KafkaEmbedded extends EmbeddedCluster{
  def initCluster(socialBDProperies:SocialBDProperties): Unit = {
    val kafkaLocalBroker = new KafkaLocalBroker.Builder()
      .setKafkaHostname(socialBDProperies.urlKafka.split(":")(0))
      .setKafkaPort(socialBDProperies.urlKafka.split(":")(1).toInt)
      .setKafkaBrokerId(0)
      .setKafkaProperties(new Properties())
      .setKafkaTempDir("embedded_kafka")
      .setZookeeperConnectionString("localhost:12345")
      .build()
    kafkaLocalBroker.start()
  }
}
