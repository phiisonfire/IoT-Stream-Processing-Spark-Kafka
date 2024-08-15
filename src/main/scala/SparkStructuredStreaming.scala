package de.phinguyen.kafkawithscala

import cats.conversions.all.autoWidenFunctor
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, concat_ws, from_json, to_date, to_timestamp, window}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}

import java.time.temporal.ChronoUnit
import scala.collection.IterableOnce.iterableOnceExtensionMethods

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

  case class sensorEvent(device_id: String, sensor_freq: Double, event_time: java.sql.Timestamp, time_window: String)
  // object to hold the intermediate state of the aggregation
  case class sensorEventState(sensorEvents: List[sensorEvent], maxEventTimeStamp: java.time.Instant)
  // object to be returned once the aggregates are finalized
  case class sensorEventsFiltered(device_id: String,
                                  sensor_freq: Double,
                                  event_time: java.sql.Timestamp,
                                  isAnomalous: Int)

  def transformSensorState(
                          time_window: String,
                          values: Iterator[sensorEvent],
                          state: GroupState[sensorEventState]
                          ): Iterator[sensorEventsFiltered] = {
    if (state.hasTimedOut) {
      println("***** State timed out called ***** for key: " + time_window)

      val sensorEventsList: List[sensorEvent] = state.get.sensorEvents

      val anomalousEvents = sensorEventsList.filter(_.sensor_freq <= 0).map{
        case sensorEvent(dev, freq, event_time, time_window) => sensorEventsFiltered(dev, freq, event_time, isAnomalous = 1)
      }
      val nonAnomalousEvents = sensorEventsList.filter(_.sensor_freq > 0).map {
        case sensorEvent(dev, freq, event_time, time_window) => sensorEventsFiltered(dev, freq, event_time, isAnomalous = 1)
      }
      val sortedEvents = (anomalousEvents:::nonAnomalousEvents.sortWith((a,b) => a.event_time.before(b.event_time))).toArray
      val finalEvents = (sortedEvents.head :: sortedEvents(sortedEvents.size/2) :: sortedEvents.last :: Nil).toSet ++ anomalousEvents.toSet

      state.remove()
      return finalEvents.iterator
    }
    if (!state.exists) { // For the first time the state has been created against this group key
      val sensorEventsList = values.toList
      val maxEventTSAllowed = sensorEventsList.head.event_time.toInstant().plus(60, ChronoUnit.SECONDS)
      state.update(sensorEventState(sensorEventsList, maxEventTSAllowed))
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs(), "3 seconds")
      println("***** Getting into the state for the first time ***** for key: " + time_window)
      println(sensorEventsList.head.event_time)
      Iterator.empty
    }
    else { // Event appear again for the same key
      val sensorEventsList: List[sensorEvent] = state.get.sensorEvents
      val maxTS = state.get.maxEventTimeStamp
      state.update(sensorEventState(sensorEventsList ++ values.toList, maxTS))
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs(), "3 seconds")
      println("**** Getting into same state for another time **** for key: " + time_window)
      Iterator.empty
    }
  }

  def csv_writer(df: DataFrame, path: String, partition_by: String): Unit = {
    df.write.partitionBy(partition_by).mode("append").option("mergeSchema", "true")
      .csv(path)
  }

  val csvDir = "D:\\phinguyen\\IoT-Stream-Processing-Spark-Kafka\\data\\csv_output"
  val aggCheckpointDir = "D:\\phinguyen\\IoT-Stream-Processing-Spark-Kafka\\data\\checkpoints"

  def main(args: Array[String]) = {
    // Get configuration
    val config = ConfigFactory.load()
    val bootstrapServers: String = config.getString("kafka.bootstrapServers")
    val topicName: String = config.getString("kafka.topicName")

    val spark = SparkSession.builder()
      .appName("Spark-Streaming-Arbitrary-Stateful-Processing")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._  // Import implicits for encoders

    val kafkaDF = sparkReadStreamFromKafka(spark, bootstrapServers, topicName)
      .withColumn("json_data", from_json(col("value"), sensorSchema))
      .select(col("json_data.*")) // kafkaDF.columns = ["device_id", "sensor_freq", "event_time"]

    val kafkaAggDF = kafkaDF
      .withColumn("event_time", to_timestamp(col("event_time")))
      .withColumn("time_window", window(col("event_time"), "60 seconds", "30 seconds"))
      .withColumn("time_window", concat_ws("-", col("time_window.start"), col("time_window.end")))

    val streamedDS: Dataset[sensorEvent] = kafkaAggDF.as[sensorEvent]
    val aggFilteredDS = streamedDS
      .withWatermark("event_time", "30 seconds")
      .groupByKey(_.time_window)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(transformSensorState)

    val streamingQueryForAggregation = aggFilteredDS.toDF().writeStream.foreachBatch(
      (outputDF: DataFrame, batch_id: Long) => {
        csv_writer(outputDF.drop("time_window").withColumn("part_date", to_date(col("event_time"))), csvDir, "part_date")
      }
    ).option("checkpointLocation", aggCheckpointDir)
      .queryName("Sampling_Agg_Stream")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    streamingQueryForAggregation.awaitTermination()
  }
}
