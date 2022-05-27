package com.example.stateapi;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author liuwei
 * @date 2022/5/27 9:33
 */
public class StreamFullJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Tuple3<String, String, Long>> stream1 = environment.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L),
                Tuple3.of("a", "stream-1", 6000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2));

        DataStream<Tuple3<String, String, Long>> stream2 = environment.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L),
                Tuple3.of("a", "stream-2", 5000L),
                Tuple3.of("c", "stream-2", 7000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2));

        stream1.connect(stream2)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    // 定义状态保存已到达的数据
                    private ListState<Tuple3<String, String, Long>> stream1State;
                    private ListState<Tuple3<String, String, Long>> stream2State;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stream1State = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-state",
                                Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                        stream2State = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream2-state",
                                Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        // 获取另一条流已到达的数据，进行匹配
                        for (Tuple3<String, String, Long> right : stream2State.get()) {
                            out.collect(value + " -> " + right);
                        }
                        stream1State.add(value);
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        // 获取另一条流已到达的数据，进行匹配
                        for (Tuple3<String, String, Long> left : stream1State.get()) {
                            out.collect(left + " -> " + value);
                        }
                        stream2State.add(value);
                    }
                })
                .print();

        environment.execute();
    }
}
