package com.example.sourceapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liuwei
 * @date 2022/5/19 13:01
 */
public class ReadTextSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 文本读数据
        DataStream<String> source1 = environment.readTextFile("input/user_clicks.txt");

        List<Integer> data = new ArrayList<>();
        data.add(1);
        data.add(2);
        data.add(3);
        // 集合读数据
        DataStream<Integer> source2 = environment.fromCollection(data);

        DataStream<String> source3 = environment.fromElements("a", "b", "c");
        source1.print();
        source2.print();
        source3.print();
        environment.execute();
    }
}
