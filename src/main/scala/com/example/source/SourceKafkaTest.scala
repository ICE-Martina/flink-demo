package com.example.source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

case class Event(user: String, url: String, ts: Long)

object SourceKafkaTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    val kafkaSource = KafkaSource.builder()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092")
      .setTopics("test")
      .setGroupId("test-groupId")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
    val sourceDs = environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source")

    val eventDs = sourceDs.map(str => {
      val strs = str.split(",")
      Event(strs(0), strs(1), strs(2).toLong)
    })

    eventDs.print()

    environment.execute("SourceKafkaTest");
  }

}
