package com.example.timeWindowapi;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @date 2022/5/24 10:58
 */
public class ProcessWindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Event> andWatermarks = environment.addSource(new ClickParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        andWatermarks.print("data: ");
        andWatermarks.keyBy(data -> true).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new CustomProcessWindowFunction()).print();

        environment.execute();
    }

    public static class CustomProcessWindowFunction extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Set<String> users = Sets.newHashSet();

            for (Event element : elements) {
                users.add(element.user);
            }

            int uv = users.size();
            long start = context.window().getStart();
            long end = context.window().getEnd();

            out.collect("window: [ " + new Timestamp(start) + " ~ " + new Timestamp(end) + " ): uv -> " + uv);
        }
    }
}
