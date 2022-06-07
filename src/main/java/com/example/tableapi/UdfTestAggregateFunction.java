package com.example.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author liuwei
 * @date 2022/6/7 11:56
 */
public class UdfTestAggregateFunction {
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

        tableEnvironment.createTemporarySystemFunction("WeightAvg", WeightAverage.class);
        Table result = tableEnvironment.sqlQuery("select user_name, WeightAvg(ts,1) as w_avg from user_click group by user_name");

        tableEnvironment.toChangelogStream(result).print();

        environment.execute();
    }

    public static class WeightAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    public static class WeightAverage extends AggregateFunction<Long, WeightAvgAccumulator> {

        @Override
        public Long getValue(WeightAvgAccumulator weightAvg) {
            if (weightAvg.count == 0) {
                return null;
            } else {
                return weightAvg.sum / weightAvg.count;
            }
        }

        @Override
        public WeightAvgAccumulator createAccumulator() {
            return new WeightAvgAccumulator();
        }

        public void accumulate(WeightAvgAccumulator accumulator, Long iValue, Integer iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }
}
