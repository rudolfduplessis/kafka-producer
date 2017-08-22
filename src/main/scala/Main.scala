import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * Created by rudolf on 2017/08/21.
  */
object Main extends App {
  val events = 100
  val topic = "meters-topic"
  val brokers = "192.168.1.6:32775"
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  //props.put("acks", "all")
  //props.put("retries", 0)
  //props.put("batch.size", 16384)
  //props.put("linger.ms", 1)
  //props.put("buffer.memory", 33554432)
  //props.put("transactional.id", "my-transactional-id")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events)) {
      val runtime = System.currentTimeMillis()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String](topic, null, System.currentTimeMillis(), ip, msg)
      println(s"Sending $data")
      //async
      //producer.send(data, )
      //sync
      producer.send(data/*, (m,e) => { println(e.getMessage) }*/)
    }

    println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    producer.close()

/*  producer.initTransactions()

  try {
    producer.beginTransaction()
    producer.send(new ProducerRecord(topic, "some-key-1", "some-value-1"))
    producer.commitTransaction();
  } catch {
    case ex @ (_:ProducerFencedException | _:OutOfOrderSequenceException | _:AuthorizationException) =>
    // We can't recover from these exceptions, so our only option is to close the producer and exit.
      println(ex.getMessage)
      producer.close()
    case ex: KafkaException =>
      println(ex.getMessage)
      producer.abortTransaction()
  }
  producer.close()*/
}
