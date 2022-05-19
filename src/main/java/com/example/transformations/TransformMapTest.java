package com.example.transformations;

import com.example.common.Event;
import com.example.common.PageView;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwei
 * @date 2022/5/19 14:50
 */
public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<PageView> mapOperator = environment.addSource(new ClickParallelSource())
                // 1.lambda表达式
                .map(event -> new PageView(event.url, 1));
        // 2.自定义类
//                .map(new CustomMapFunction());
        // 3.匿名类
                /*.map(new MapFunction<Event, PageView>() {
                    @Override
                    public PageView map(Event event) throws Exception {
                        return new PageView(event.url, 1);
                    }
                });*/
        DataStream<PageView> pv = mapOperator.keyBy(p -> p.url).sum("count");

        pv.print();
        environment.execute();
    }

    public static class CustomMapFunction implements MapFunction<Event, PageView> {

        @Override
        public PageView map(Event event) throws Exception {
            return new PageView(event.url, 1);
        }
    }

}
