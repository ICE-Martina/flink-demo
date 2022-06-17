package com.example.cdc.stream;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwe 2022/6/17 11:12
 */
public class FlinkCDCTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("master1")
                .port(3306)
                .databaseList("gmall")
                .tableList("gmall.user_info")
                .build();
        DataStream<String> streamSource = environment.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");
        streamSource.print();

        environment.execute();
    }
}
