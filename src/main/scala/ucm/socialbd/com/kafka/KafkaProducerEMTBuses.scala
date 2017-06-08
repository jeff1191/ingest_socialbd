package ucm.socialbd.com.kafka

import java.net.SocketTimeoutException
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json.{JSONArray, JSONException, XML}
import org.slf4j.LoggerFactory
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.utils.{DateUtil, SocialBDConfig}
import net.liftweb.json._

import scala.io.Source._
import scalaj.http._

/**
  * Created by Jeff on 14/05/2017.
  */
class KafkaProducerEMTBuses(socialBDProperties: SocialBDProperties) extends KafkaProducerActions{
  private val logger = LoggerFactory.getLogger(getClass)
  private val stopList = fromFile(getClass.getResource("/listadoParadasEMT").getPath).getLines.toList.map(x => x.split(",")(1))
  private var nParada = 0
  override def process(): Unit = {
    val props = SocialBDConfig.getProperties(socialBDProperties)
    val producer = new KafkaProducer[String, String](props)
    try{
      while(true) {
        if(nParada != stopList.size - 1) //skip header
          nParada = nParada + 1
        else{
          nParada = 1
        }
        Thread.sleep(6000 )
        val urlRequest = socialBDProperties.eMTBusesConf.urlEMTBuses.replace("#inputIdStop#",stopList(nParada))
        val response: HttpResponse[String] = Http(urlRequest).asString
        val eventTime = response.headers.getOrElse("Date","Problem encountered").asInstanceOf[Vector[String]](0)
        val fechaHora = DateUtil.getDateFormatted(eventTime)

        val json = net.liftweb.json.Xml.toJson(xml.XML.loadString(response.body).child)

        println(compactRender(json))

        var stopLines = compactRender(json \ "StopLines" \ "Data")
        if (!stopLines.contains("["))
          stopLines = "[" + compactRender(json \ "StopLines"\ "Data") + "]"


        var listArrive = compactRender(json \ "ListaArriveEstimation" \ "Arrive")
        if(!listArrive.contains("["))
          listArrive ="[" + compactRender(json \ "ListaArriveEstimation" \ "Arrive") + "]"


        val jsonString =
          s"""{"idStop":${compactRender(json \ "Label")},
             |"DescriptionStop":${compactRender(json \ "Description")},
             |"Direction":${compactRender(json \ "Direction")},
             |"StopLine":${stopLines},
             |"ArriveEstimation":${listArrive},
             |"timestamp": "${fechaHora}"}""".stripMargin.replace("\n","").replace("\r","")
          println(jsonString)
          val record = new ProducerRecord(socialBDProperties.eMTBusesConf.emtbusesTopic, "key", jsonString)
          producer.send(record)
      }
    }
    catch{
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
      case e : RuntimeException => {
        logger.warn("JSONException, reconnecting..." + e.getMessage)
        nParada = nParada + 1 //next event
        process()//reconnect
      }
    }
//    producer.close()

  }
  def fromXMLJSONArray(xml: String): JSONArray ={
    val jsonObj = XML.toJSONObject(xml)
    val arr = jsonObj.getJSONObject(jsonObj.keys().next())
    arr.getJSONArray(arr.keys().next())
  }
  def fromXMLtoJSON(xml:String): Unit ={

  }
}
