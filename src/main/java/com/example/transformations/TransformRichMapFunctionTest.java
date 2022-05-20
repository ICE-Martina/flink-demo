package com.example.transformations;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwei
 * @date 2022/5/20 16:39
 */
public class TransformRichMapFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setParallelism(1);
        environment.setParallelism(2);

        DataStream<Event> source = environment.addSource(new ClickParallelSource());
        DataStream<String> richMap = source.map(new CustomRichMapFunctionDemo());
        richMap.print();
        environment.execute();
    }

    public static class CustomRichMapFunctionDemo extends RichMapFunction<Event, String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("RichMapFunction ---> open: " + getRuntimeContext().getTaskName());
        }

        @Override
        public String map(Event event) {
            return "{ user: \"" + event.user + "\", url: \"" + event.url +
                    "\", timestamp: \"" + event.timestamp + "\" }";
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("RichMapFunction ---> close: " + getRuntimeContext().getTaskName());
        }
    }
}
