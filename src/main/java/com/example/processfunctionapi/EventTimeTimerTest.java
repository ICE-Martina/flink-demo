package com.example.processfunctionapi;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/24 18:02
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Event> streamOperator = environment.addSource(new ClickParallelSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        streamOperator.keyBy(data -> data.user).process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                Long timestamp = ctx.timestamp();
                out.collect(ctx.getCurrentKey() + " 数据到达, 时间戳: " + new Timestamp(timestamp) +
                        ", watermark: " + ctx.timerService().currentWatermark());
                ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(ctx.getCurrentKey() + " 定时器触发, 触发时间: " + new Timestamp(timestamp) +
                        ", watermark: " + ctx.timerService().currentWatermark());
            }
        }).print();

        environment.execute();
    }
}
