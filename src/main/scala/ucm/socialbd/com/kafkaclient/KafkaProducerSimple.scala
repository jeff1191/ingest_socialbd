package ucm.socialbd.com.kafkaclient

import java.util.Properties

import org.apache.kafka.clients.producer._


/**
  * Created by Jeff on 09/03/2017.
  */
object KafkaProducerSimple {


  def main(args: Array[String]): Unit = {
    val  props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")

      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC="trafficMadrid"

    for(i<- 1 to 50){
      val record = new ProducerRecord(TOPIC, "key", s"hello $i")
      producer.send(record)
    }

    val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
    producer.send(record)

    producer.close()
  }
}
