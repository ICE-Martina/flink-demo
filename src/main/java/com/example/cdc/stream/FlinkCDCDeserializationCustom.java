package com.example.cdc.stream;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwe 2022/6/20 10:59
 */
public class FlinkCDCDeserializationCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("master1").port(3306)
                .username("root").password("111111")
                .databaseList("db_config").tableList("db_config.dim_config")
                .deserializer(new CustomStringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> dbConfigStream = environment.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "db_config");
        dbConfigStream.print("db_config");

        environment.execute();
    }
}
