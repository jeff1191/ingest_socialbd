package ucm.socialbd.com.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.utils.SocialBDConfig

import scalaj.http._
/**
  * Created by Jeff on 22/03/2017.
  */
class KafkaProducerAirQuality(socialBDProperties: SocialBDProperties) extends KafkaProducerActions{

  override def process(): Unit = {
    val  props = SocialBDConfig.getProperties(socialBDProperties)

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      val response: HttpResponse[String] = Http(socialBDProperties.qualityAirConf.qualityAirUrl).asString
      val qualityAir = response.body.split("\n")

      val itJSON: Iterator[String] = qualityAir.iterator
      while(itJSON.hasNext) {
        val message = csvToJsonString(itJSON.next())
        println(message)
        val record = new ProducerRecord(socialBDProperties.qualityAirConf.qualityAirTopic, "key", message)
        producer.send(record)
        Thread.sleep(10) //delay between events
      }
      Thread.sleep(socialBDProperties.qualityAirConf.delay) // delay between requests to the web page
    }
    producer.close()
  }
  //Convert a line of csv in a Json Object
  def csvToJsonString(content: String): String = {
    val mapper = content.split(",")
    s"""{"estacion" : "${mapper(0)}${mapper(1)}${mapper(2)}",
        |"magnitud" : "${mapper(3)}",
        |"tecnica": "${mapper(4)}",
        |"horario": "${mapper(5)}",
        |"fecha": "${mapper(6)}-${mapper(7)}-${mapper(8)}",
        |"listaHoras": [
        |{"hora": "00:00", "valor": "${mapper(9)}", "isValid": "${mapper(10)}"},
        |{"hora": "01:00", "valor": "${mapper(11)}", "isValid": "${mapper(12)}"},
        |{"hora": "02:00", "valor": "${mapper(13)}", "isValid": "${mapper(14)}"},
        |{"hora": "03:00", "valor": "${mapper(15)}", "isValid": "${mapper(16)}"},
        |{"hora": "04:00", "valor": "${mapper(17)}", "isValid": "${mapper(18)}"},
        |{"hora": "05:00", "valor": "${mapper(19)}", "isValid": "${mapper(20)}"},
        |{"hora": "06:00", "valor": "${mapper(21)}", "isValid": "${mapper(22)}"},
        |{"hora": "07:00", "valor": "${mapper(23)}", "isValid": "${mapper(24)}"},
        |{"hora": "08:00", "valor": "${mapper(25)}", "isValid": "${mapper(26)}"},
        |{"hora": "09:00", "valor": "${mapper(27)}", "isValid": "${mapper(28)}"},
        |{"hora": "10:00", "valor": "${mapper(29)}", "isValid": "${mapper(30)}"},
        |{"hora": "11:00", "valor": "${mapper(31)}", "isValid": "${mapper(32)}"},
        |{"hora": "12:00", "valor": "${mapper(33)}", "isValid": "${mapper(34)}"},
        |{"hora": "13:00", "valor": "${mapper(35)}", "isValid": "${mapper(36)}"},
        |{"hora": "14:00", "valor": "${mapper(37)}", "isValid": "${mapper(38)}"},
        |{"hora": "15:00", "valor": "${mapper(39)}", "isValid": "${mapper(40)}"},
        |{"hora": "16:00", "valor": "${mapper(41)}", "isValid": "${mapper(42)}"},
        |{"hora": "17:00", "valor": "${mapper(43)}", "isValid": "${mapper(44)}"},
        |{"hora": "18:00", "valor": "${mapper(45)}", "isValid": "${mapper(46)}"},
        |{"hora": "19:00", "valor": "${mapper(47)}", "isValid": "${mapper(48)}"},
        |{"hora": "20:00", "valor": "${mapper(49)}", "isValid": "${mapper(50)}"},
        |{"hora": "21:00", "valor": "${mapper(51)}", "isValid": "${mapper(52)}"},
        |{"hora": "22:00", "valor": "${mapper(53)}", "isValid": "${mapper(54)}"},
        |{"hora": "23:00", "valor": "${mapper(55)}", "isValid": "${mapper(56)}"} ] }""".stripMargin.replace("\n","").replace("\r", "")
  }

}
