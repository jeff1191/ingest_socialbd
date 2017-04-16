package ucm.socialbd.com.kafkaclient

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.{JSONArray, XML}
import ucm.socialbd.com.config.SocialBDProperties

import scalaj.http._
/**
  * Created by Jeff on 19/03/2017.
  */
class KafkaProducerTraffic(socialBDProperties: SocialBDProperties) extends KafkaProducerActions{

  override def process(): Unit = {
    val  props = new Properties()
    props.put("bootstrap.servers", socialBDProperties.urlKafka)

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      val response: HttpResponse[String] = Http(socialBDProperties.trafficConf.urlTraffic).asString
      println(response.headers)
      val ret: JSONArray = fromXMLJSONArray(response.body)
      val itJSON = ret.iterator()
      while(itJSON.hasNext) {
        val record = new ProducerRecord(socialBDProperties.trafficConf.trafficTopic, "key", "" + itJSON.next())
        producer.send(record)
        Thread.sleep(100) //delay between json messages
      }
      Thread.sleep(socialBDProperties.trafficConf.delay) // delay between request to the web page
    }
    producer.close()

  }
  def fromXMLJSONArray(xml: String): JSONArray ={
    val jsonObj = XML.toJSONObject(xml)
    val arr = jsonObj.getJSONObject(jsonObj.keys().next())
    arr.getJSONArray(arr.keys().next())
  }
}
