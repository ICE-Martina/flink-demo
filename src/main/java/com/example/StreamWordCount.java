package com.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liuwei jetbrains://idea/navigate/reference?project=flink-wordcount&fqn=com.example.WordCount
 * @date 2022/5/18 12:04
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStream<String> source = environment.socketTextStream("master1", 12345);
        DataStream<Tuple2<String, Integer>> wordOne = source.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split("\\W+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> wordCount = wordOne.keyBy(word -> word.f0).sum(1);
        wordCount.print();

        environment.execute();
    }
}
