package com.example.sinkapi

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._

object KafkaSinkScalaTest {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val hosts = "node1:9092,node2:9092,node3:9092"

    val kafkaSource = KafkaSource.builder()
      .setBootstrapServers(hosts)
      .setGroupId("test")
      .setTopics("test")
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setStartingOffsets(OffsetsInitializer.latest())
      .build()

    val streamSource = environment.fromSource(kafkaSource,
      watermarkStrategy = WatermarkStrategy.noWatermarks(), "kafka-source")

    val wordCount = streamSource.flatMap(_.split("\\W+")).map((_, 1)).keyBy(_._1).sum(1)
    val kafkaSink = KafkaSink.builder()
      .setBootstrapServers(hosts)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("test_out")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

    wordCount.map(t => s"""{word: "${t._1}", count: ${t._2}""").sinkTo(kafkaSink)
    environment.execute()
  }
}
