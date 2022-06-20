package com.example.cdc.stream;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwe 2022/6/17 11:12
 */
public class FlinkCDCStreamTest {
    /**
     * flink1.14.4使用flink-cdc-2.2.1版本的依赖问题
     启动模式使用StartupOptions.initial()无法初始化数据
     相较1.x版本，2.x开发测试时需要额外添加依赖flink-table-common

     报错 java.lang.NoSuchMethodError: com.mysql.cj.CharsetMapping.getJavaEncodingForMysqlCharset
     flink cdc依赖的mysql驱动版本与项目版本冲突
     需要使用mysql:mysql-connector-java:8.0.21

     报错 java.lang.NoClassDefFoundError: org/apache/flink/shaded/guava18/com/google/common/util/concurrent/ThreadFactoryBuilder
     flink cdc 2.2.1源码依赖flink1.13.5
     如果使用flink1.14版本，则会缺少依赖org.apache.flink:flink-shaded-guava:18.0-13.0
     添加依赖org.apache.flink:flink-shaded-guava:18.0-13.0或改为使用依赖flink-sql-connector-mysql-cdc
     */

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("master1")
                .port(3306)
                .username("root")
                .password("111111")
                .databaseList("db_config")
                .tableList("db_config.dim_config")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        DataStream<String> streamSource = environment.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "db-config");
        streamSource.print();

        environment.execute();
    }
}
