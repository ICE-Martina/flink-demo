package com.example.sinkapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liuwei
 * @date 2022/5/21 16:29
 */
public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setGroupId("test")
                .setTopics("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> streamSource = environment.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "kafka-source");
        DataStream<Tuple2<String, Integer>> wordCount = streamSource.flatMap((String lines, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = lines.split("\\W+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(t -> t.f0).sum(1);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test_out")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        wordCount.map(t -> "{word: \"" + t.f0 + "\", count: " + t.f1 + "}").sinkTo(kafkaSink);

        environment.execute();
    }
}
