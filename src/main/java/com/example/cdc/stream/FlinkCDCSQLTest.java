package com.example.cdc.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author liuwe 2022/6/20 14:12
 */
public class FlinkCDCSQLTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        tableEnvironment.executeSql("create table dim_config( " +
                "id int not null, " +
                "name string, " +
                "gender char, " +
                "PRIMARY KEY(id) NOT ENFORCED" +
                " ) with ( " +
                "'connector'='mysql-cdc', " +
                "'hostname'='master1', " +
                "'port'='3306', " +
                "'username'='root', " +
                "'password'='111111', " +
                "'database-name'='db_config', " +
                "'table-name'='dim_config'" +
                " )");

        Table table = tableEnvironment.sqlQuery("select * from dim_config");
        DataStream<Row> rowDataStream = tableEnvironment.toChangelogStream(table);
        rowDataStream.print("flinkSQL-CDC");
        environment.execute();
    }
}
