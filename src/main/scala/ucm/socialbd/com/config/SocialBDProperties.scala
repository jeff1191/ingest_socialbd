package ucm.socialbd.com.config
import com.typesafe.config.ConfigFactory
/**
  * Created by Jeff on 16/04/2017.
  */

case class TwitterConf(twitterTopic: String, consumerKey:String,consumerSecret:String,accessToken:String,accessSecret:String)
case class TrafficConf(trafficTopic:String, urlTraffic: String, delay: Long)
case class QualityAirConf(qualityAirTopic:String, qualityAirUrl:String, delay: Long)

class SocialBDProperties {
  //checks about if exist components in application.conf
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "twitter")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "traffic")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "qualityAir")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "kafka")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "zk")

  val twitterConf = TwitterConf(ConfigFactory.load().getString("twitter.twitterTopic"),
                    ConfigFactory.load().getString("twitter.consumerKey"),
                    ConfigFactory.load().getString("twitter.consumerSecret"),
                    ConfigFactory.load().getString("twitter.accessToken"),
                    ConfigFactory.load().getString("twitter.accessSecret"))

 val trafficConf = TrafficConf(ConfigFactory.load().getString("traffic.trafficTopic"),
                    ConfigFactory.load().getString("traffic.urlTraffic"),
                    ConfigFactory.load().getInt("traffic.delayMinutes") * 60 * 1000)

  val qualityAirConf = QualityAirConf(ConfigFactory.load().getString("qualityAir.qualityAirTopic"),
                      ConfigFactory.load().getString("qualityAir.qualityAirUrl"),
                      ConfigFactory.load().getInt("traffic.delayMinutes") * 60 * 1000)

val urlKafka =  ConfigFactory.load().getString("kafkaUrl")
}
