package com.example.multistreamingapi;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/26 9:19
 */
public class JoinStreamingTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Tuple2<String, Long>> stream1 = environment.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1500L),
                Tuple2.of("a", 1700L),
                Tuple2.of("b", 2000L)).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (element, recordTimestamp) -> element.f1));

        DataStream<Tuple2<String, Integer>> stream2 = environment.fromElements(
                Tuple2.of("a", 3000),
                Tuple2.of("b", 3500),
                Tuple2.of("a", 4700),
                Tuple2.of("b", 5500)).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Integer>>) (element, recordTimestamp) -> element.f1));

        stream1.join(stream2)
                .where(data -> data.f0)
                .equalTo(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                /*.apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> first, Tuple2<String, Integer> second) throws Exception {
                        return first + "  -->  " + second;
                    }
                })*/
                .apply((JoinFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>) (first, second) -> first + "  -->  " + second)
                .print();

        environment.execute();
    }
}
