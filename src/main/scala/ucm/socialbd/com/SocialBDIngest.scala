package ucm.socialbd.com

import ucm.socialbd.com.config.{SocialBDProperties}
import ucm.socialbd.com.kafkaclient.{KafkaProducerAirQuality, KafkaProducerTraffic, KafkaProducerTwitter}

/**
  * Created by Jeff on 16/04/2017.
  */

object SocialBDIngest {
  val ingestNames = Set("AIR", "TRAFFIC", "TWITTER")

  def printUsage(exit: Boolean = false): Unit = {
    println ("Arguments:<ingest name>")
    println ("Ingest name must be one of: [" + ingestNames.mkString(", ") +"]")
    if (exit)
      sys.exit(1)
  }
  def main(args: Array[String]): Unit = {
    println("-------------------------")
    println("  IngestSocialBigData-CM")
    println("-------------------------")
    if (args.length !=  1 ) printUsage(exit = true)

    val ingest = args(0).trim.toUpperCase match {
      case "AIR" => new KafkaProducerAirQuality(new SocialBDProperties())
      case "TRAFFIC" => new KafkaProducerTraffic(new SocialBDProperties())
      case "TWITTER" => new KafkaProducerTwitter(new SocialBDProperties())
      case _ => {
        println (s"Unrecognized ingest type ${args(0)}")
        printUsage(exit = false)
        sys.exit(1)
      }
    }
    ingest.process()
  }
}