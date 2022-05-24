package com.example.timeWindowapi;

import com.example.common.Event;
import com.example.common.UrlCount;
import com.example.sourceapi.ClickParallelSource;
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

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/24 14:47
 */
public class AggregateAndProcessFunctionTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<Event> streamOperator = environment.addSource(new ClickParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        streamOperator.print("data: ");
        streamOperator.keyBy(data -> data.url).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountUrlStatistics(), new UrlStatisticsResult()).print();

        environment.execute();
    }

    public static class CountUrlStatistics implements AggregateFunction<Event, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Event value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    public static class UrlStatisticsResult extends ProcessWindowFunction<Integer, UrlCount, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Integer> elements, Collector<UrlCount> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Integer next = elements.iterator().next();
            out.collect(new UrlCount(s, next, start, end));
        }
    }
}
