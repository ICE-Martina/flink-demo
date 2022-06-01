package com.example.stateapi;

import com.example.common.Event;
import com.example.sourceapi.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liuwei
 * @date 2022/5/30 9:46
 */
public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = environment.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        stream.addSink(new BufferingSink(10));

        environment.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {

        private final int batchSize;
        private final List<Event> bufferedElement;
        private ListState<Event> checkpointState;

        public BufferingSink(int batchSize) {
            this.batchSize = batchSize;
            this.bufferedElement = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) {
            bufferedElement.add(value);
            if (bufferedElement.size() == batchSize) {
                for (Event event : bufferedElement) {
                    System.out.println(event);
                }
                System.out.println("================= 输出完成 =================");
                bufferedElement.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清空状态
            checkpointState.clear();

            // 持久化状态
            checkpointState.addAll(bufferedElement);

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            checkpointState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("buffer-state", Event.class));

            // 如果是从故障中恢复的状态，需要复制状态数据到本地缓存
            if (context.isRestored()) {
                for (Event event : checkpointState.get()) {
                    bufferedElement.add(event);
                }
            }
        }
    }
}
