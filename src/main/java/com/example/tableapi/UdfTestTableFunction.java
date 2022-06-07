package com.example.tableapi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author liuwei
 * @date 2022/6/7 11:41
 */
public class UdfTestTableFunction {
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

        // 注册自定义表函数
        tableEnvironment.createTemporarySystemFunction("mySplit", MyTableFunction.class);

        // 调用表函数
        Table result = tableEnvironment.sqlQuery("SELECT user_name, url, id, length FROM " +
                "user_click, LATERAL TABLE(mySplit(url)) AS T(id, length)");
        tableEnvironment.toDataStream(result).print();

        environment.execute();
    }

    public static class MyTableFunction extends TableFunction<Tuple2<String, Integer>> {

        public void eval(String str) {
            String[] ids = str.split("\\?");
            for (String id : ids) {
                collect(Tuple2.of(id, id.length()));
            }
        }
    }
}
