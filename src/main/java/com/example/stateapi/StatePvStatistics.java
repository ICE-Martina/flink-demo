package com.example.stateapi;

import com.example.common.Event;
import com.example.sourceapi.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/26 17:05
 */
public class StatePvStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<Event> source = environment.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        source.print("data");

        source.keyBy(data -> data.user)
                .process(new PeriodPvStatistics())
                .print("pv");


        environment.execute();
    }

    public static class PeriodPvStatistics extends KeyedProcessFunction<String, Event, String> {

        // 保存PV统计值
        ValueState<Long> countState;
        // 保存定时器是都存在的状态
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count-state", Long.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-state", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);

            // 如果没有定时器, 注册定时器
            if (timerState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发, 输出统计结果
            out.collect(ctx.getCurrentKey() + "  pv: " + countState.value() + "  ts: " + new Timestamp(timestamp));
            // 清空定时器
            timerState.clear();
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerState.update(timestamp + 10 * 1000L);
        }
    }
}
