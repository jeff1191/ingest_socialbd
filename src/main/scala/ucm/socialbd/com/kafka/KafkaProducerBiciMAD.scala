package ucm.socialbd.com.kafka

import java.net.SocketTimeoutException
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.{JSONArray, JSONException, JSONObject}
import org.slf4j.LoggerFactory
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.utils.SocialBDConfig

import scalaj.http._

/**
  * Created by Jeff on 14/05/2017.
  */
class KafkaProducerBiciMAD(socialBDProperties: SocialBDProperties) extends KafkaProducerActions{
  private val logger = LoggerFactory.getLogger(getClass)

  override def process(): Unit = {
    val props = SocialBDConfig.getProperties(socialBDProperties)
    val producer = new KafkaProducer[String, String](props)
    try {
      while (true){
        val response: HttpResponse[String] = Http(socialBDProperties.biciMadConf.urlBiciMad).asString
        val eventTime = response.headers.getOrElse("Date", "Problem encountered").asInstanceOf[Vector[String]](0)
        val formattedDate = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US).parse(eventTime)
        val fechaHora = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(formattedDate)
        val jsonObj = new JSONObject(response.body)
        val jsonObj2 = new JSONObject(jsonObj.get("data").toString)

        val ret: JSONArray = jsonObj2.getJSONArray("stations")
        val itJSON = ret.iterator()

        while (itJSON.hasNext) {
          val message = ("" + itJSON.next()).replaceFirst("}",",\"timestamp\":"+ s""""$fechaHora"}""")
          println(message)
          val record = new ProducerRecord(socialBDProperties.biciMadConf.bicimadTopic, "key", message)
          producer.send(record)
          Thread.sleep(10)
        }
        Thread.sleep(10000)
    }
  }catch{
      case e: NoSuchElementException =>{
        logger.warn("NoSuchElementException, reconnecting..." + e.getMessage)
        process() //reconnect
      }
      case e: SocketTimeoutException => {
        logger.warn("SocketTimeoutException, reconnecting..." + e.getMessage)
        process()//reconnect
      }
      case e : JSONException => {
        logger.warn("JSONException, reconnecting..." + e.getMessage)
        process()//reconnect
      }
      case e: Exception => {
        logger.warn("Unknown exception, reconnecting..." + e.getMessage)
        process()//reconnect
      }
    }

    producer.close()
  }
}
