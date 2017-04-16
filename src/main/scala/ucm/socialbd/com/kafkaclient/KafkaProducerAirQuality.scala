package ucm.socialbd.com.kafkaclient

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import ucm.socialbd.com.config.SocialBDProperties

import scalaj.http._
/**
  * Created by Jeff on 22/03/2017.
  */
class KafkaProducerAirQuality(socialBDProperties: SocialBDProperties) extends KafkaProducerActions{

  override def process(): Unit = {

    val  props = new Properties()
    props.put("bootstrap.servers", socialBDProperties.urlKafka)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      val response: HttpResponse[String] = Http(socialBDProperties.qualityAirConf.qualityAirUrl).asString
      val qualityAir = response.body.split("\n")

      val itJSON: Iterator[String] = qualityAir.iterator
      while(itJSON.hasNext) {
        val record = new ProducerRecord(socialBDProperties.qualityAirConf.qualityAirTopic, "key", csvToJsonString(itJSON.next()))
        producer.send(record)
        Thread.sleep(100) //delay between events
      }
      Thread.sleep(socialBDProperties.qualityAirConf.delay) // delay between requests to the web page
    }
    producer.close()
  }
  //Convert a line of csv in a Json Object
  def csvToJsonString(content: String): String = {
    val mapper = content.split(",")
    s"""{"ESTACION" : "${mapper(0)}|${mapper(1)}|${mapper(2)}",
        |"MAGNITUD" : "${mapper(3)}",
        |"TECNICA": "${mapper(4)}",
        |"HORARIO": "${mapper(5)}",
        |"FECHA": "${mapper(6)}-${mapper(7)}-${mapper(8)}",
        |"H1": "${mapper(9)}", "isValidH1": "${mapper(10)}",
        |"H2": "${mapper(11)}", "isValidH2": "${mapper(12)}",
        |"H3": "${mapper(13)}", "isValidH3": "${mapper(14)}",
        |"H4": "${mapper(15)}", "isValidH4": "${mapper(16)}",
        |"H5": "${mapper(17)}", "isValidH5": "${mapper(18)}",
        |"H6": "${mapper(19)}", "isValidH6": "${mapper(20)}",
        |"H7": "${mapper(21)}", "isValidH7": "${mapper(22)}",
        |"H8": "${mapper(23)}", "isValidH8": "${mapper(24)}",
        |"H9": "${mapper(25)}", "isValidH9": "${mapper(26)}",
        |"H10": "${mapper(27)}", "isValidH10": "${mapper(28)}",
        |"H11": "${mapper(29)}", "isValidH11": "${mapper(30)}",
        |"H12": "${mapper(31)}", "isValidH12": "${mapper(32)}",
        |"H13": "${mapper(33)}", "isValidH13": "${mapper(34)}",
        |"H14": "${mapper(35)}", "isValidH14": "${mapper(36)}",
        |"H15": "${mapper(37)}", "isValidH15": "${mapper(38)}",
        |"H16": "${mapper(39)}", "isValidH16": "${mapper(40)}",
        |"H17": "${mapper(41)}", "isValidH17": "${mapper(42)}",
        |"H18": "${mapper(43)}", "isValidH18": "${mapper(44)}",
        |"H19": "${mapper(45)}", "isValidH19": "${mapper(46)}",
        |"H20": "${mapper(47)}", "isValidH20": "${mapper(48)}",
        |"H21": "${mapper(49)}", "isValidH21": "${mapper(50)}",
        |"H22": "${mapper(51)}", "isValidH22": "${mapper(52)}",
        |"H23": "${mapper(53)}", "isValidH23": "${mapper(54)}",
        |"H24": "${mapper(55)}", "isValidH24": "${mapper(56)}" }""".stripMargin.replace("\n","").replace("\r", "")
  }

}
