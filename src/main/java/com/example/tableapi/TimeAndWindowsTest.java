package com.example.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liuwei
 * @date 2022/6/6 14:39
 */
public class TimeAndWindowsTest {
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
        // 1. 分组聚合
        Table aggUser = tableEnvironment.sqlQuery("select user_name, count(1) from user_click group by user_name");
        tableEnvironment.toChangelogStream(aggUser).print("agg");

        // 2. 分组窗口聚合(老版本)
        Table aggWindowUser = tableEnvironment.sqlQuery("select " +
                "user_name, count(1) as cnt, TUMBLE_END(et, interval '10' second) as endT " +
                "from user_click group by " +
                "user_name, tumble(et, interval '10' second)");
        tableEnvironment.toDataStream(aggWindowUser).print("window");

        // 3. 窗口聚合(新版本)
        // 3.1 滚动窗口
        Table tumbleWindow = tableEnvironment.sqlQuery("select user_name, count(1) as cnt, window_end as endT " +
                "from TABLE(" +
                "  TUMBLE(TABLE user_click, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") group by " +
                " user_name, window_start, window_end");
        tableEnvironment.toDataStream(tumbleWindow).print("tumble window");

        // 3.2 滑动窗口
        Table hopWindow = tableEnvironment.sqlQuery("select user_name, count(1) as cnt, window_end as endT " +
                "from TABLE(" +
                "  HOP(TABLE user_click, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") group by " +
                " user_name, window_start, window_end");
        tableEnvironment.toDataStream(hopWindow).print("hop window");

        // 3.3 累积窗口
        Table cumulateWindow = tableEnvironment.sqlQuery("select user_name, count(1) as cnt, window_end as endT " +
                "from TABLE(" +
                "  CUMULATE(TABLE user_click, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") group by " +
                " user_name, window_start, window_end");
        tableEnvironment.toDataStream(cumulateWindow).print("cumulate window");

        // 4. 开窗聚合(Over)
        Table overWindow = tableEnvironment.sqlQuery("select user_name, AVG(ts) OVER( " +
                "  PARTITION BY user_name " +
                "  ORDER BY et " +
                "  ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) as user_avg_ts " +
                "from user_click");
        tableEnvironment.toDataStream(overWindow).print("over window");

        environment.execute();
    }
}
