package com.example.sourceapi;

import com.example.common.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwei
 * @date 2022/5/19 13:50
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
//        DataStream<Event> streamSource = environment.addSource(new ClickSource());
        DataStream<Event> streamSource = environment.addSource(new ClickParallelSource());

        streamSource.print();
        environment.execute();
    }
}
