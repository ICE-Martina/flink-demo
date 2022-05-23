package com.example.timeWindowapi;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/23 14:55
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        DataStream<Event> streamSource = environment.addSource(new ClickParallelSource());
        DataStream<Tuple2<String, Integer>> watermarks = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp
                )).map(event -> Tuple2.of(event.url, 1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });

        watermarks.keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .window(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(2)))
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
//                .countWindow(10, 5)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }).print();

        environment.execute();
    }
}
