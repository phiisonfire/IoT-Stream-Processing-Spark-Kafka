package de.phinguyen.kafkawithscala

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object IoTDataProducer {

  // Get configuration
  private val config = ConfigFactory.load()
  val bootstrapServers: String = config.getString("kafka.bootstrapServers")
  val topicName: String = config.getString("kafka.topicName")


  // Producer configuration
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  def generateIoTData(): String = {
    val deviceId = "1001"

    val isFreqOutOfRange = Random.nextDouble() < 0.01 // 1% chance of frequency out of range
    val sensorFreq = if (isFreqOutOfRange) {
      // Generate a value outside the [98;102] range
      if (Random.nextBoolean()) {
        Random.nextDouble() * 98 // Generate values [0;98]
      } else {
        Random.nextDouble() * 98 + 102 // Generate values [102;200]
      }
    } else {
      // Generate value within the range [98;102]
      Random.nextDouble() * 4 + 98
    }

    val eventTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS").format(new Date())

    s"""{
       |  "device_id": "$deviceId"
       |  "sensor_freq": $sensorFreq
       |  "event_time": "$eventTime"
       |}""".stripMargin
  }

  def sendMessage(producer: KafkaProducer[String, String], topicName: String, message: String): Unit = {
    // 2% chance of delaying message
    if (Random.nextDouble() < 0.02) {
      val delay = Random.nextInt(30) + 1 // delay between 1 to 30 seconds
      println(s"Message delayed for $delay seconds: $message")
      Future {
        Thread.sleep(delay * 1000)
        producer.send(new ProducerRecord[String, String](topicName, message))
        println(s"Delayed message sent: $message")
      }
    } else {
      // Send immediately
      producer.send(new ProducerRecord[String, String](topicName, message))
      println(s"Message sent: $message")
    }
  }

  def main(args: Array[String]): Unit = {
    val producer = new KafkaProducer[String, String](props)
    // Generate and send data to kafka
    try {
      while (true) {
        val message = generateIoTData()
        sendMessage(producer, topicName, message)
        Thread.sleep(1000) // Send a message every second
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
  }
}
