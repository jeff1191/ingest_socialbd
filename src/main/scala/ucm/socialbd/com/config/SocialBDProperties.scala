package ucm.socialbd.com.config
import com.typesafe.config.ConfigFactory
/**
  * Created by Jeff on 16/04/2017.
  */

case class TwitterConf(twitterTopic: String, consumerKey:String,consumerSecret:String,accessToken:String,accessSecret:String)

case class TrafficConf(urbanTrafficTopic:String, interUrbanTrafficTopic:String,  urlTraffic: String, delay: Long)

case class QualityAirConf(qualityAirTopic:String, qualityAirUrl:String, delay: Long)

case class BiciMadConf(bicimadTopic: String, urlBiciMad:String, delayMinutes:Long)

case class EMTBusesConf(emtbusesTopic:String, urlEMTBuses:String, delayMinutes:Long )

class SocialBDProperties {
  //checks about if exist components in application.conf
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "twitter")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "traffic")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "air")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "bici")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "emtbuses")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "kafkaUrl")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "zkUrl")

  val twitterConf = TwitterConf(ConfigFactory.load().getString("twitter.twitterTopic"),
                    ConfigFactory.load().getString("twitter.consumerKey"),
                    ConfigFactory.load().getString("twitter.consumerSecret"),
                    ConfigFactory.load().getString("twitter.accessToken"),
                    ConfigFactory.load().getString("twitter.accessSecret"))

  val trafficConf = TrafficConf(ConfigFactory.load().getString("traffic.urbanTrafficTopic"),
                    ConfigFactory.load().getString("traffic.interUrbanTrafficTopic"),
                    ConfigFactory.load().getString("traffic.urlTraffic"),
                    ConfigFactory.load().getInt("traffic.delayMinutes") * 60 * 1000)

  val qualityAirConf = QualityAirConf(ConfigFactory.load().getString("air.qualityAirTopic"),
                    ConfigFactory.load().getString("air.qualityAirUrl"),
                    ConfigFactory.load().getInt("air.delayMinutes") * 60 * 1000)

  val biciMadConf = BiciMadConf(ConfigFactory.load().getString("bicimad.bicimadTopic"),
                    ConfigFactory.load().getString("bicimad.urlBiciMad"),
                    ConfigFactory.load().getInt("bicimad.delayMinutes") * 60 * 1000)

  val eMTBusesConf = EMTBusesConf(ConfigFactory.load().getString("buses.emtbusesTopic"),
                    ConfigFactory.load().getString("buses.urlEMTBuses"),
                    ConfigFactory.load().getInt("buses.delayMinutes") * 60 * 1000)

  val urlKafka =  ConfigFactory.load().getString("kafkaUrl")
  val urlZookeeper =  ConfigFactory.load().getString("zkUrl")
}
