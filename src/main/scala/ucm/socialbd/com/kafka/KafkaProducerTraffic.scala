package ucm.socialbd.com.kafka

import java.net.SocketTimeoutException
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.{JSONArray, JSONException, XML}
import org.slf4j.LoggerFactory
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.utils.SocialBDConfig

import scalaj.http._
/**
  * Created by Jeff on 19/03/2017.
  */
class KafkaProducerTraffic(socialBDProperties: SocialBDProperties) extends KafkaProducerActions{
  private val logger = LoggerFactory.getLogger(getClass)

  override def process(): Unit = {
    val props = SocialBDConfig.getProperties(socialBDProperties)
    val producer = new KafkaProducer[String, String](props)
    try{
    while(true) {//requesting every 5 minutes regard if the field Last-Modified have changed
      val response: HttpResponse[String] = Http(socialBDProperties.trafficConf.urlTraffic).asString
      println(response.headers)
      val eventTime = response.headers.getOrElse("Date","Problem encountered").asInstanceOf[Vector[String]](0)
      val formattedDate = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US).parse(eventTime)
      val fechaHora = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(formattedDate)

      val ret: JSONArray = fromXMLJSONArray(response.body)
      val itJSON = ret.iterator()
      while(itJSON.hasNext) {
        val message = ("" + itJSON.next()).replaceFirst("}",",\"timestamp\":"+ s""""$fechaHora"}""")
        println(message)
        if(message contains "descripcion"){
          val record = new ProducerRecord(socialBDProperties.trafficConf.urbanTrafficTopic, "key", message)
          producer.send(record)
        }
        else{
          val record = new ProducerRecord(socialBDProperties.trafficConf.interUrbanTrafficTopic, "key", message)
          producer.send(record)
        }
        Thread.sleep(20) //delay between json messages
      }
     // System.exit(1)
      Thread.sleep(socialBDProperties.trafficConf.delay) // delay between request to the web page
    }
    }
    catch{
      case e: NoSuchElementException =>{
        logger.warn("NoSuchElementException, reconnecting...")
        process() //reconnect
      }
      case e: SocketTimeoutException => {
        logger.warn("SocketTimeoutException, reconnecting...")
        process()//reconnect
      }
      case e : JSONException => {
        logger.warn("JSONException, reconnecting...")
        process()//reconnect
      }
      case e: Exception => {
        logger.warn("Unknown exception, reconnecting...")
        process()//reconnect
      }
    }
    producer.close()

  }
  def fromXMLJSONArray(xml: String): JSONArray ={
    val jsonObj = XML.toJSONObject(xml)
    val arr = jsonObj.getJSONObject(jsonObj.keys().next())
    arr.getJSONArray(arr.keys().next())
  }
}
