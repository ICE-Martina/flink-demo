package com.example.transformations;

import com.example.common.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwei
 * @date 2022/5/19 17:15
 */
public class TransformSimpleAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<Event> streamSource = environment.readTextFile("input/user_clicks.txt").map(data -> {
            String[] logs = data.split(",");
            return new Event(logs[0], logs[1], Long.valueOf(logs[2]));
        });

        // 1.匿名类
        streamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("max: ");
        // 2.lambda表达式
        streamSource.keyBy(event -> event.user).maxBy("timestamp").print("maxBy: ");


        environment.execute();
    }
}
