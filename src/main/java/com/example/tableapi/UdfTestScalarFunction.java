package com.example.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author liuwei
 * @date 2022/6/7 11:26
 */
public class UdfTestScalarFunction {
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

        // 注册自定义标量函数
        tableEnvironment.createTemporarySystemFunction("myHash", HashFunction.class);

        // 调用自定义标量函数
        Table result = tableEnvironment.sqlQuery("select user_name, myHash(user_name) from user_click");

        tableEnvironment.toDataStream(result).print();

        environment.execute();
    }

    public static class HashFunction extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }
    }
}
