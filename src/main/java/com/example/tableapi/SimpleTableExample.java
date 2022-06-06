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
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/5/31 15:01
 */
public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<Event> sourceStream = environment.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 方法一:
        Table eventTable = tableEnvironment.fromDataStream(sourceStream);
        Table table = tableEnvironment.sqlQuery("select user, url, `timestamp` from " + eventTable);
        DataStream<Row> rowDataStream = tableEnvironment.toDataStream(table);
        rowDataStream.print("method1");

        // 方法二:
        Table sourceTable = tableEnvironment.fromDataStream(sourceStream).as("user_name", "url");
        tableEnvironment.createTemporaryView("event_table", sourceTable);
        Table countTable = tableEnvironment.sqlQuery("select user_name, count(url) from event_table group by user_name");
        // 这里转换成流会抛异常
//        tableEnvironment.toDataStream(countTable).print();
        // 正确的方法
//        tableEnvironment.toChangelogStream(countTable).print("method2");

        // 方法三:
        Table sourceTable1 = tableEnvironment.fromDataStream(sourceStream, Schema.newBuilder()
                .column("user", "STRING")
                .column("url", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`timestamp`, 3)")
                .watermark("ts", "ts - INTERVAL '10' SECOND")
                .build());
        tableEnvironment.createTemporaryView("source_table1", sourceTable1);
        Table countTable1 = tableEnvironment.sqlQuery("select count(user) from source_table1");
        tableEnvironment.toChangelogStream(countTable1).print("method3");

        environment.execute();
    }
}
