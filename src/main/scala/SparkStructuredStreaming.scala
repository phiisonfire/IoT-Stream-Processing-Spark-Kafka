package de.phinguyen.kafkawithscala

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, concat_ws, from_json, to_timestamp, window}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkStructuredStreaming {

  def sparkReadStreamFromKafka(spark: SparkSession, kafkaBootstrapServers: String, kafkaTopicName: String): DataFrame = {
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopicName)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as value")
  }

  val sensorSchema: StructType = StructType(Seq(
    StructField("device_id", StringType, nullable = true),
    StructField("sensor_freq", DoubleType, nullable = true),
    StructField("event_time", StringType, nullable = true)
  ))

  def main(args: Array[String]) = {
    // Get configuration
    val config = ConfigFactory.load()
    val bootstrapServers: String = config.getString("kafka.bootstrapServers")
    val topicName: String = config.getString("kafka.topicName")

    val spark = SparkSession.builder()
      .appName("Spark-Streaming-Arbitrary-Stateful-Processing")
      .master("local[*]")
      .getOrCreate()

    val kafkaDF = sparkReadStreamFromKafka(spark, bootstrapServers, topicName)
      .withColumn("json_data", from_json(col("value"), sensorSchema))
      .select(col("json_data.*")) // kafkaDF.columns = ["device_id", "sensor_freq", "event_time"]

    val query1 = kafkaDF
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .start()

    val kafkaAggDF = kafkaDF
      .withColumn("event_time", to_timestamp(col("event_time")))
      .withColumn("time_window", window(col("event_time"), "60 seconds", "30 seconds"))
      .withColumn("time_window", concat_ws("-", col("time_window.start"), col("time_window.end")))

    // Await termination of the queries
    query1.awaitTermination()
  }

}
