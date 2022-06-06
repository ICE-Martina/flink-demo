package com.example.cepapi;

import com.example.common.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author liuwei
 * @date 2022/6/6 10:05
 */
public class OrderTimeoutDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<OrderEvent> orderStream = environment.fromElements(
                new OrderEvent("user_1", "order_1", "create", 1000L),
                new OrderEvent("user_2", "order_2", "create", 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.ts));

        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) {
                return "create".equalsIgnoreCase(value.eventType);
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) {
                return "pay".equalsIgnoreCase(value.eventType);
            }
        }).within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP.pattern(orderStream.keyBy(data -> data.orderId), orderEventPattern);

        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };

        SingleOutputStreamOperator<String> result = patternStream.process(new OrderPayMatch());

        result.print("payed");
        result.getSideOutput(outputTag).print("timeout");

        environment.execute();
    }

    public static class OrderPayMatch extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {

        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) {
            OrderEvent payEvent = match.get("pay").get(0);
            out.collect("用户 " + payEvent.userId + " 已支付订单 " + payEvent.orderId + " .");
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) {
            OrderEvent createEvent = match.get("create").get(0);
            OutputTag<String> outputTag = new OutputTag<String>("timeout") {
            };
            ctx.output(outputTag, "用户 " + createEvent.userId + " 未支付订单 " + createEvent.orderId + " .");
        }
    }
}
