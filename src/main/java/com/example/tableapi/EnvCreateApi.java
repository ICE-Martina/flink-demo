package com.example.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liuwei
 * @date 2022/5/31 16:10
 */
public class EnvCreateApi {
    public static void main(String[] args) throws Exception {
        // 方法一:
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);

        // 方法二:
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
//                .inBatchMode()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnvironment1 = TableEnvironment.create(environmentSettings);


        streamExecutionEnvironment.execute();
    }
}
