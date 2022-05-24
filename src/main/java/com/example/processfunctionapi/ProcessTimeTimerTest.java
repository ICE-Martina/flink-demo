package com.example.processfunctionapi;

import com.example.common.Event;
import com.example.sourceapi.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author liuwei
 * @date 2022/5/24 16:35
 */
public class ProcessTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        environment.addSource(new ClickSource()).keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        System.out.println(ctx.getCurrentKey() + " 数据到达, 到达时间: " + new Timestamp(currentProcessingTime));
                        // 注册一个10秒后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器出发, 出发时间: " + new Timestamp(timestamp));
                    }
                }).print();

        environment.execute();
    }
}
