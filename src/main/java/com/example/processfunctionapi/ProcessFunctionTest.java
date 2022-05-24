package com.example.processfunctionapi;

import com.example.common.Event;
import com.example.sourceapi.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/24 15:56
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<Event> andWatermarks = environment.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        andWatermarks.print("data");
        andWatermarks.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                if (value.url.contains("buy")) {
                    out.collect("{ user: \"" + value.user + "\", url: \"" + value.url + "\", timestamp: " + value.timestamp + " }");
                }
                System.out.println("timestamp: " + ctx.timestamp());
                System.out.println("watermark: " + ctx.timerService().currentWatermark());
            }
        }).print();

        environment.execute();
    }
}
