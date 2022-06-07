package com.example.tableapi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author liuwei
 * @date 2022/6/7 13:32
 */
public class UdfTestTableAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        String ddl = "create table user_click ( " +
                "user_name string, " +
                "url string, " +
                "ts bigint, " +
                "et AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000)), " +
                "WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") with ( " +
                "'connector'='filesystem', " +
                "'path'='input/user_clicks.txt', " +
                "'format'='csv')";

        tableEnvironment.executeSql(ddl);
        tableEnvironment.createTemporarySystemFunction("Top2", Top2.class);

        Table aggTable = tableEnvironment.sqlQuery("SELECT user_name, COUNT(url) AS cnt, window_start, window_end FROM TABLE(" +
                "   TUMBLE( TABLE user_click, DESCRIPTOR(et), INTERVAL '10' SECOND )" +
                ") GROUP BY user_name, window_start, window_end");

        Table result = aggTable.groupBy($("window_end"))
                .flatAggregate(call("Top2", $("cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));
        tableEnvironment.toChangelogStream(result).print();

        environment.execute();
    }

    public static class Top2Accumulator {
        public Long max;
        public Long secondMax;
    }

    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator top2Accumulator = new Top2Accumulator();
            top2Accumulator.max = Long.MIN_VALUE;
            top2Accumulator.secondMax = Long.MIN_VALUE;
            return top2Accumulator;
        }

        // 更新累加器, 方法名必须是accumulate
        public void accumulate(Top2Accumulator top2Accumulator, Long value) {
            if (value > top2Accumulator.max) {
                top2Accumulator.secondMax = top2Accumulator.max;
                top2Accumulator.max = value;
            } else if (value > top2Accumulator.secondMax) {
                top2Accumulator.secondMax = value;
            }
        }

        public void emitValue(Top2Accumulator top2Accumulator, Collector<Tuple2<Long, Integer>> out) {
            if (top2Accumulator.max != Long.MIN_VALUE) {
                out.collect(Tuple2.of(top2Accumulator.max, 1));
            }

            if (top2Accumulator.secondMax != Long.MIN_VALUE) {
                out.collect(Tuple2.of(top2Accumulator.secondMax, 2));
            }
        }
    }
}
