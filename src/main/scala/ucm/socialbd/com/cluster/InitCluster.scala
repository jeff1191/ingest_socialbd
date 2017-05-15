package ucm.socialbd.com.cluster

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import ucm.socialbd.com.config.SocialBDProperties

/**
  * Created by Jeff on 26/04/2017.
  */
object InitCluster {

  def run(socialDBProperties:SocialBDProperties): Unit ={
    //MODIFY CONFIGURATION

    ZookeeperEmbedded.initCluster(socialDBProperties)
    KafkaEmbedded.initCluster(socialDBProperties)

    createKafkaTopic(socialDBProperties.qualityAirConf.qualityAirTopic, "localhost:12345", 1, 1)
    createKafkaTopic(socialDBProperties.trafficConf.urbanTrafficTopic, "localhost:12345", 1, 1)
    createKafkaTopic(socialDBProperties.twitterConf.twitterTopic, "localhost:12345", 1, 1)
  }


  def createKafkaTopic(topic: String, zookeeperHosts: String, partitionSize: Int, replicationCount: Int, connectionTimeoutMs: Int = 10000, sessionTimeoutMs: Int = 10000): Unit = {
    val zkUtils = ZkUtils.apply(zookeeperHosts, sessionTimeoutMs, connectionTimeoutMs, false)
    AdminUtils.createTopic( zkUtils, topic, partitionSize, replicationCount, new Properties())
    zkUtils.close()
  }
}
