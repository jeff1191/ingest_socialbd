package ucm.socialbd.com.cluster

import ucm.socialbd.com.config.SocialBDProperties

/**
  * Created by Jeff on 24/04/2017.
  */
trait EmbeddedCluster {

  def initCluster(socialBDProperties: SocialBDProperties): Unit
}
