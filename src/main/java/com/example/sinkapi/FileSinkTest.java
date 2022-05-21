package com.example.sinkapi;

import com.example.common.Event;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author liuwei
 * @date 2022/5/21 16:08
 */
public class FileSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> source = environment.addSource(new ClickParallelSource());

        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("./output/file_sink"),
                new SimpleStringEncoder<>("utf-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .withRolloverInterval(TimeUnit.HOURS.toMillis(1))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
                        .build())
                .build();

        source.map(event -> "{ user: \"" + event.user + "\", url: \"" + event.url +
                "\", timestamp: \"" + event.timestamp + "\" }").addSink(fileSink).setParallelism(2);

        environment.execute();
    }
}
