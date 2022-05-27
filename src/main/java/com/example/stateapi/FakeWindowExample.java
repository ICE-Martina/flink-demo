package com.example.stateapi;

import com.example.common.Event;
import com.example.sourceapi.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/27 11:06
 */
public class FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<Event> andWatermarks = environment.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        andWatermarks.print("data");

        andWatermarks.keyBy(data -> data.url)
                .process(new FakeWindowResult(10000L))
                .print();

        environment.execute();
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        // 窗口大小
        private final Long windowSize;

        // 定义MapState, 保存每个窗口统计的值
        private MapState<Long, Long> windowUrlCountState;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) {
            windowUrlCountState = getRuntimeContext().getMapState(new MapStateDescriptor<>("window-state", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 窗口分配器: 每来一条数据, 根据时间戳判断属于那个窗口
            Long windowStart = value.timestamp / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;

            // 注册定时器
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            // 更新状态, 进行增量聚合
            if (windowUrlCountState.contains(windowStart)) {
                Long count = windowUrlCountState.get(windowStart);
                windowUrlCountState.put(windowStart, count + 1);
            } else {
                windowUrlCountState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long count = windowUrlCountState.get(windowStart);
            out.collect("窗口: " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
                    + "  url: " + ctx.getCurrentKey()
                    + "  count: " + count);
            // 关闭对应的窗口: 清楚对应窗口的值.
            windowUrlCountState.remove(windowStart);
        }
    }
}
