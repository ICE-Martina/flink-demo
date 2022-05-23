package com.example.sinkapi;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;

/**
 * @author liuwei
 * @date 2022/5/22 16:59
 */
public class MysqlSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<Event> source = environment.addSource(new ClickParallelSource());

        source.addSink(
                JdbcSink.sink(
                        "insert into user_click_data (username, click_url, ts) values (?,?,?)",
                        (statement, event) -> {
                            statement.setString(1, event.user);
                            statement.setString(2, event.url);
                            statement.setTimestamp(3, new Timestamp(event.timestamp));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://192.168.56.122:3306/analysis_db")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("bigdata")
                                .withPassword("111111")
                                .build()
                ));

        /*source.addSink(JdbcSink.exactlyOnceSink(
                "insert into user_click_data (username, click_url, ts) values (?,?,?)",
                (ps, event) -> {
                    ps.setString(1, event.user);
                    ps.setString(2, event.url);
                    ps.setTimestamp(3, new Timestamp(event.timestamp));
                },
                JdbcExecutionOptions.builder().withBatchSize(10).withMaxRetries(0).build(),
                JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(),
                () -> {
                    MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
                    xaDataSource.setUrl("jdbc:mysql://192.168.56.122:3306/analysis_db");
                    xaDataSource.setUser("bigdata");
                    xaDataSource.setPassword("111111");
                    return xaDataSource;
                }
        ));*/

        environment.execute();
    }
}
