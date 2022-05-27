package com.example.stateapi;

import com.example.common.Event;
import com.example.sourceapi.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/27 11:40
 */
public class AverageTimestampExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<Event> stream = environment.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        stream.print();

        stream.keyBy(data -> data.user)
                .flatMap(new AvgTimestampResult(5L))
                .print();

        environment.execute();
    }

    public static class AvgTimestampResult extends RichFlatMapFunction<Event, String> {
        private final Long count;
        // 保存平均时间戳的状态
        private AggregatingState<Event, Long> avgState;

        // 保存用户访问次数的状态
        private ValueState<Long> countState;

        public AvgTimestampResult(Long count) {
            this.count = count;
        }

        @Override
        public void open(Configuration parameters) {
            avgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>("avg-state",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    }, Types.TUPLE(Types.LONG, Types.LONG)));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count-state", Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 每来一条数据 current count +1
            Long currentCount = countState.value() == null ? 1L : countState.value() + 1;
            countState.update(currentCount);
            avgState.add(value);

            // 达到count次数就输出结果
            if (currentCount.equals(count)) {
                out.collect(value.user + "过去" + count + "次访问平均时间戳: " + avgState.get());
                // 清空状态
                countState.clear();
                avgState.clear();
            }
        }
    }
}
