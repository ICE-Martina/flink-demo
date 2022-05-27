package com.example.multistreamingapi;

import com.example.common.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/26 9:38
 */
public class IntervalJoinStreamingTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Tuple2<String, Long>> orderStream = environment.fromElements(
                Tuple2.of("Mary", 50000L),
                Tuple2.of("Alice", 30000L),
                Tuple2.of("Bob", 52000L),
                Tuple2.of("Alice", 20000L),
                Tuple2.of("Cary", 55000L)).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (element, recordTimestamp) -> element.f1));

        DataStream<Event> clickStream = environment.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prd?id=100", 3000L),
                new Event("Alice", "./prd?id=200", 3500L),
                new Event("Bob", "./prd?id=2", 2500L),
                new Event("Alice", "./prd?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./buy?id=100", 23000L),
                new Event("Bob", "./prd?id=300", 33000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(data -> data.user))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + " --> " + left);
                    }
                })
                .print();

        environment.execute();
    }
}
