package com.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liuwei
 * @date 2022/5/19 8:48
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // DataSet的api要被弃用
//        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用流批一体的api, 也可以在命令行指定 bin/flink run -Dexecution.runtime-mode=BATCH examples/streaming/WordCount.jar
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        environment.setParallelism(1);
        DataStream<String> dataSource = environment.readTextFile("input/paper.txt");
        DataStream<Tuple2<String, Integer>> flatMapOperator = dataSource.flatMap((String lines, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = lines.split("\\W+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        });
        DataStream<Tuple2<String, Integer>> wordCount = flatMapOperator.keyBy(t -> t.f0).sum(1);
        wordCount.print();

        environment.execute();
    }
}
