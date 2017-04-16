package ucm.socialbd.com.kafkaclient

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.{JSONArray, XML}

import scalaj.http._
/**
  * Created by Jeff on 19/03/2017.
  */
object XMLProxyTraffic{
  val urlTrafficMadrid="http://informo.munimadrid.es/informo/tmadrid/pm.xml"
  val urlCalidadAire="http://www.mambiente.munimadrid.es/opendata/horario.txt"
  val urlBiciMad="https://rbdata.emtmadrid.es:8443/BiciMad/get_stations/WEB.SERV.jalmache@ucm.es/E45799A1-93D4-442D-8BFA-BAEDC09BE363"
  val urlBuses="https://servicios.emtmadrid.es:8443/bus/servicebus.asmx/GetRouteLines?idClient=WEB.SERV.jalmache@ucm.es&PassKey=E45799A1-93D4-442D-8BFA-BAEDC09BE363&SelectDate=&Lines="


  val delayTime = 5 *   // minutes to sleep
    60 *   // seconds to a minute
    1000 //milliseconds

 def main(args: Array[String]): Unit = {
   val  props = new Properties()
   props.put("bootstrap.servers", "localhost:9092")

   props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
   props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

   val producer = new KafkaProducer[String, String](props)
   val TOPIC="trafficMadrid"
   while(true) {
     val response: HttpResponse[String] = Http(urlTrafficMadrid).asString
     println(response.headers)
    val ret: JSONArray = fromXMLJSONArray(response.body)
   val itJSON = ret.iterator()
    while(itJSON.hasNext) {
      val record = new ProducerRecord(TOPIC, "key", "" + itJSON.next())
      producer.send(record)
      Thread.sleep(200) //delay between json messages
    }
     Thread.sleep(delayTime)
   }
   producer.close()
 }

  def fromXMLJSONArray(xml: String): JSONArray ={
    val jsonObj = XML.toJSONObject(xml)
    val arr = jsonObj.getJSONObject(jsonObj.keys().next())
    arr.getJSONArray(arr.keys().next())
  }
}
