package com.example.stateapi;

import com.example.common.Event;
import com.example.sourceapi.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/26 14:46
 */
public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<Event> streamOperator = environment.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        streamOperator.keyBy(data -> data.user).flatMap(new CustomFlatMap()).print();

        environment.execute();
    }

    public static class CustomFlatMap extends RichFlatMapFunction<Event, String> {
        private ValueState<Event> customValueState;
        private ListState<Event> customListState;
        private MapState<String, Long> customMapState;
        private ReducingState<Event> customReducingState;
        private AggregatingState<Event, String> customAggregatingState;
        private Long localVar = 0L;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Event> eventValueStateDescriptor = new ValueStateDescriptor<>("custom-value-state", Event.class);
            // 设置custom-value-state的TTL; Only TTLs in reference to processing time are currently supported.
            StateTtlConfig valueStateTtl = StateTtlConfig.newBuilder(Time.minutes(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            eventValueStateDescriptor.enableTimeToLive(valueStateTtl);
            customValueState = getRuntimeContext().getState(eventValueStateDescriptor);
            customListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("custom-list-state", Event.class));
            customMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("custom-map-state", String.class, Long.class));
            customReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("custom-reducing-state",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) {
                            Long ts = value1.timestamp > value2.timestamp ? value1.timestamp : value2.timestamp;
                            return new Event(value1.user, value1.url, ts);
                        }
                    }, Event.class));
            customAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("custom-aggregate-state",
                    new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event value, Long accumulator) {
                            return accumulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return "count: " + accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    }, Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            System.out.println("value state update before: " + customValueState.value());
            customValueState.update(value);
            System.out.println("value state update after: " + customValueState.value());

            customListState.add(value);
            System.out.println("list state update after: " + customListState.get());

            customMapState.put(value.user, customMapState.get(value.user) == null ? 1 : customMapState.get(value.user) + 1);
            System.out.println("map state update after: " + value.user + "  count: " + customMapState.get(value.user));

            customReducingState.add(value);
            System.out.println("reduce state update after: " + value.user + "  count: " + customReducingState.get());

            customAggregatingState.add(value);
            System.out.println("aggregate state update after: " + value.user + "  count: " + customAggregatingState.get());

            localVar++;
            System.out.println("local var: " + localVar);
        }
    }
}
