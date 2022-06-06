package com.example.tableapi;

import com.example.common.Event;
import com.example.sourceapi.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/6/6 16:42
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Event> clickStream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, timestamp) -> event.timestamp));

        Table table = tableEnv.fromDataStream(clickStream,
                Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("url", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .columnByExpression("et", "TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000))")
                        .watermark("et", "et - INTERVAL '1' SECOND").build());
        tableEnv.toDataStream(table).print("data");

        tableEnv.createTemporaryView("click", table);

        // 普通top-n
        Table topN = tableEnv.sqlQuery("select user, cnt, row_num " +
                " from ( " +
                "  select *, row_number() over( " +
                "       order by cnt desc" +
                "     ) AS row_num " +
                "  from (" +
                "         select user, count(url) as cnt from click group by user" +
                "     )" +
                " ) where row_num <= 2");

        tableEnv.toChangelogStream(topN).print("top-n");

        // 窗口top-n
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
        tableEnv.executeSql(ddl);

        Table windowTopN = tableEnv.sqlQuery("SELECT user_name, cnt, row_num " +
                " FROM ( " +
                "  SELECT *, ROW_NUMBER() OVER( " +
                "       PARTITION BY window_start, window_end" +
                "       ORDER BY cnt desc" +
                "     ) AS row_num " +
                "  FROM (" +
                "         SELECT user_name, COUNT(url) AS cnt, window_start, window_end FROM TABLE(" +
                "            TUMBLE(TABLE user_click, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                "          ) GROUP BY user_name, window_start, window_end" +
                "     )" +
                " ) WHERE row_num <= 2");

        tableEnv.toDataStream(windowTopN).print("window top-n");

        env.execute();
    }
}
