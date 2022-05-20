package com.example.transformations;

import com.example.common.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwei
 * @date 2022/5/20 17:03
 */
public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> streamSource = environment.fromElements(
                new Event("patton", "/collectList.htm?spm=a1z02.1", 1000L),
                new Event("stellar", "/collectList.htm?spm=a1z02.1", 2000L),
                new Event("patton", "/collectList.htm?spm=a1z02.1", 3000L),
                new Event("stellar", "/collectList.htm?spm=a1z02.1", 4000L),
                new Event("patton", "/collectList.htm?spm=a1z02.1", 3000L),
                new Event("stellar", "/collectList.htm?spm=a1z02.1", 3500L),
                new Event("patton", "/collectList.htm?spm=a1z02.1", 5000L),
                new Event("danika", "/collectList.htm?spm=a1z02.1", 4500L));
        // 1.随机分区, shuffle的默认分区策略是随机分发到下一个operator
//        streamSource.shuffle().print("shuffle: ").setParallelism(4);
        // 2.轮询分区
//        streamSource.rebalance().print("rebalance: ").setParallelism(4);
        // 3.重缩放分区, 同一组内的轮询分发数据到下游分区
        /*environment.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i < 9; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(4);*/
        // 4.广播
//        streamSource.broadcast().print().setParallelism(3);

        // 5.全局分区, 上游的所有分区都分配到下游的一个分区
//        streamSource.global().print().setParallelism(4);

        // 6.自定义分区
        /*streamSource.partitionCustom(new Partitioner<Event>() {
            @Override
            public int partition(Event event, int i) {
                char prefix = event.user.charAt(0);
                return ((int) prefix) % i;
            }
        }, new KeySelector<Event, Event>() {
            @Override
            public Event getKey(Event event) throws Exception {
                return event;
            }
        }).print().setParallelism(4);*/
        streamSource.partitionCustom((Partitioner<Event>) (event, i) -> {
            char prefix = event.user.charAt(0);
            return prefix % i;
        }, (KeySelector<Event, Event>) event -> event).print().setParallelism(4);

        environment.execute();
    }
}
