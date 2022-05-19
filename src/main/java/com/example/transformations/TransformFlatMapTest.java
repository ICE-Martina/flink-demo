package com.example.transformations;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liuwei
 * @date 2022/5/19 16:21
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<String> flatMapOperator = environment.addSource(new ClickParallelSource())
                // 1.lambda表达式
                /*.flatMap((Event event, Collector<String> out) -> {
                    out.collect(event.url);
                    out.collect(event.user);
                }).returns(Types.STRING);*/
                // 2.匿名类
                /*.flatMap(new FlatMapFunction<Event, String>() {
                    @Override
                    public void flatMap(Event event, Collector<String> collector) throws Exception {
                        collector.collect(event.user);
                        collector.collect(event.url);
                    }
                });*/
                // 3.自定义类
                .flatMap(new CustomFlatMapFunction());

        flatMapOperator.print();
        environment.execute();
    }

    public static class CustomFlatMapFunction implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
        }
    }
}
