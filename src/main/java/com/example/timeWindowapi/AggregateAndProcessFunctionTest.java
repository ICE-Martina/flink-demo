package com.example.timeWindowapi;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Set;

/**
 * @author liuwei
 * @date 2022/5/24 11:35
 */
public class AggregateAndProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Event> andWatermarks = environment.addSource(new ClickParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
                        (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp
                ));

        andWatermarks.print("data: ");
        andWatermarks.keyBy(data -> true).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CustomAggregateFunction(), new CustomProcessFunction())
                .print();

        environment.execute();
    }

    // 自定义实现AggregateFunction, 增量计算
    public static class CustomAggregateFunction implements AggregateFunction<Event, Set<String>, Integer> {

        @Override
        public Set<String> createAccumulator() {
            return Sets.newHashSet();
        }

        @Override
        public Set<String> add(Event value, Set<String> accumulator) {
            accumulator.add(value.user);
            return accumulator;
        }

        @Override
        public Integer getResult(Set<String> accumulator) {
            return accumulator.size();
        }

        @Override
        public Set<String> merge(Set<String> a, Set<String> b) {
            return null;
        }
    }

    // 自定义实现ProcessFunction, 包装输出uv信息
    public static class CustomProcessFunction extends ProcessWindowFunction<Integer, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
            int uv = elements.iterator().next();
            long start = context.window().getStart();
            long end = context.window().getEnd();

            out.collect("window: [ " + new Timestamp(start) + " ~ " + new Timestamp(end) + " ): uv -> " + uv);
        }
    }
}
