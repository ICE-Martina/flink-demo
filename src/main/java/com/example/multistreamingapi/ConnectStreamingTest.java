package com.example.multistreamingapi;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/25 16:05
 */
public class ConnectStreamingTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // app支付日志
        DataStream<Tuple3<String, String, Long>> appPayLog = environment.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3500L)).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2));

        // 第三方支付平台返回日志
        DataStream<Tuple4<String, String, String, Long>> thirdPayLog = environment.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple4<String, String, String, Long>>) (element, recordTimestamp) -> element.f3));

        // 检测同一支付订单在两条流中是否匹配, 不匹配就做预警提示
        // connect连接stream的第一种写法:
//        appPayLog.keyBy(data -> data.f0).connect(thirdPayLog.keyBy(data -> data.f0));
        appPayLog.connect(thirdPayLog).keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())
                .print();

        environment.execute();
    }

    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>,
            Tuple4<String, String, String, Long>, String> {
        // 定义状态, 保存已到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdEventState;

        @Override
        public void open(Configuration parameters) {
            appEventState = getRuntimeContext().getState(new ValueStateDescriptor<>
                    ("app-state", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdEventState = getRuntimeContext().getState(new ValueStateDescriptor<>
                    ("third-state", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value,
                                    Context ctx, Collector<String> out) throws IOException {
            // app event, 匹配third event数据是否到达
            if (thirdEventState.value() != null) {
                out.collect("对账成功: " + value + "  " + thirdEventState.value());
                // 对账完成, 清空状态
                thirdEventState.clear();
            } else {
                // third event未到达, 更新app状态
                appEventState.update(value);
                // 注册定时器, 等待5s, 等待third的数据到达
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value,
                                    Context ctx, Collector<String> out) throws IOException {

            // third event, 匹配app event数据是否到达
            if (appEventState.value() != null) {
                out.collect("对账成功: " + appEventState.value() + "  " + value);
                // 对账完成, 清空状态
                appEventState.clear();
            } else {
                // app event未到达, 更新third状态
                thirdEventState.update(value);
                // 注册定时器, 等待app的数据到达
                ctx.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发, 判断状态, 如果其中一个状态不为空, 说明另一条流的数据未到达
            if (appEventState.value() != null) {
                out.collect("对账失败: " + appEventState.value() + "  第三方支付平台数据未到.");
            }
            if (thirdEventState.value() != null) {
                out.collect("对账失败: " + thirdEventState.value() + "  app数据未到.");
            }
            appEventState.clear();
            thirdEventState.clear();
        }
    }
}
