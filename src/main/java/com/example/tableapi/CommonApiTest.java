package com.example.tableapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author liuwei
 * @date 2022/5/31 16:26
 */
public class CommonApiTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String ddl = "create table user_click ( " +
                "user_name string, " +
                "url string, " +
                "ts bigint ) with ( " +
                "'connector'='filesystem', " +
                "'path'='input/user_clicks.txt', " +
                "'format'='csv')";
        tableEnvironment.executeSql(ddl);
        Table userClick = tableEnvironment.from("user_click");
        Table resultTable = userClick.where($("user_name").isEqual("anthony")).select($("user_name"), $("url"));

        String outputDdl = "create table clickTable ( " +
                "user_name string, " +
                "url string ) with ( " +
                "'connector'='filesystem', " +
                "'path'='output', " +
                "'format'='csv')";

        tableEnvironment.executeSql(outputDdl);
        resultTable.executeInsert("clickTable");

        String printTable = "create table printTable ( " +
                "user_name string, " +
                "url string ) with ( " +
                "'connector'='print')";
        tableEnvironment.executeSql(printTable);
        resultTable.executeInsert("printTable");

        Table countTable = tableEnvironment.sqlQuery("select user_name, count(url) as total from user_click group by user_name");

        String printcTable = "create table printCountTable ( " +
                "user_name string, " +
                "total bigint ) with ( " +
                "'connector'='print')";
        tableEnvironment.executeSql(printcTable);
        countTable.executeInsert("printCountTable");
    }
}
