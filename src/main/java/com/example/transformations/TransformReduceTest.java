package com.example.transformations;

import com.example.common.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwei
 * @date 2022/5/19 17:42
 */
public class TransformReduceTest {
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

        DataStream<Tuple2<String, Integer>> userOne = streamSource.map(event -> Tuple2.of(event.user, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        DataStream<Tuple2<String, Integer>> clickByUser = userOne.keyBy(data -> data.f0)
                // 1.匿名类
                /*.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) {
                        return Tuple2.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
                    }
                });*/
                .reduce((Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));
        // 所有数据都以"key"为键进行分区, 即所有数据都会分到一个分区
        SingleOutputStreamOperator<Tuple2<String, Integer>> activeUser = clickByUser.keyBy(data -> "key")
                .reduce((Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) -> t1.f1 > t2.f1 ? t1 : t2);

        activeUser.print();

        environment.execute();
    }
}
