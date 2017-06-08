package ucm.socialbd.com.cluster

import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster
import ucm.socialbd.com.config.SocialBDProperties

/**
  * Created by Jeff on 24/04/2017.
  */
object ZookeeperEmbedded {
  def initCluster(socialBDProperties:SocialBDProperties): Unit = {
    val zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
    .setPort(socialBDProperties.urlZookeeper.split(":")(1).toInt)
    .setTempDir("embedded_zookeeper")
    .setZookeeperConnectionString(socialBDProperties.urlZookeeper)
    .setMaxClientCnxns(60)
    .setElectionPort(20001)
    .setQuorumPort(20002)
    .setDeleteDataDirectoryOnClose(false)
    .setServerId(1)
    .setTickTime(2000)
    .build()
    zookeeperLocalCluster.start()
  }
}
