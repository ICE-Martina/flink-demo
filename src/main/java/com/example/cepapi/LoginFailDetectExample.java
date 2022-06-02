package com.example.cepapi;

import com.example.common.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author liuwei
 * @date 2022/6/1 17:19
 */
public class LoginFailDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 1.获取数据
        DataStream<LoginEvent> loginData = environment.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (element, recordTimestamp) -> element.ts));

        // 2.定义模式, 连续三次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) {
                        return "fail".equals(value.eventType);
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) {
                        return "fail".equals(value.eventType);
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) {
                        return "fail".equals(value.eventType);
                    }
                });

        // 3.将模式应用到数据流上, 检测复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginData.keyBy(data -> data.userId), pattern);

        // 4.将检测到的复杂事件提取出来
        DataStream<String> result = patternStream.select((PatternSelectFunction<LoginEvent, String>) map -> {
            LoginEvent firstFail = map.get("first").get(0);
            LoginEvent secondFail = map.get("second").get(0);
            LoginEvent thirdFail = map.get("third").get(0);
            return firstFail.userId + " 连续三次登录失败, 登录时间: " +
                    firstFail.ts + ", " +
                    secondFail.ts + ", " +
                    thirdFail.ts + ".";
        });

        result.print();

        environment.execute();
    }
}
