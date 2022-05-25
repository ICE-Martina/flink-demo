package com.example.multistreamingapi;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/25 14:41
 */
public class SeparateStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Event> stream = environment.addSource(new ClickParallelSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        OutputTag<Tuple2<String, Long>> buyTag = new OutputTag<Tuple2<String, Long>>("buy") {
        };
        OutputTag<Tuple2<String, Long>> cartTag = new OutputTag<Tuple2<String, Long>>("cart") {
        };

        SingleOutputStreamOperator<Event> process = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) {
                if (value.url.contains("buy")) {
                    ctx.output(buyTag, Tuple2.of(value.url, ctx.timestamp()));
                } else if (value.url.contains("cart")) {
                    ctx.output(cartTag, Tuple2.of(value.url, ctx.timestamp()));
                } else {
                    out.collect(value);
                }
            }
        });
        process.print("main");
        process.getSideOutput(buyTag).print("buy");
        process.getSideOutput(cartTag).print("cart");

        environment.execute();
    }
}
