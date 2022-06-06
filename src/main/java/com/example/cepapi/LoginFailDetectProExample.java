package com.example.cepapi;

import com.example.common.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author liuwei
 * @date 2022/6/6 9:39
 */
public class LoginFailDetectProExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
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

        Pattern<LoginEvent, LoginEvent> loginFail = Pattern.<LoginEvent>begin("loginFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) {
                return "fail".equalsIgnoreCase(value.eventType);
            }
        }).times(3).consecutive();

        PatternStream<LoginEvent> eventPatternStream = CEP.pattern(loginData.keyBy(data -> data.userId), loginFail);
        eventPatternStream.process(new PatternProcessFunction<LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> map, Context context, Collector<String> collector) throws Exception {
                LoginEvent firstFail = map.get("loginFail").get(0);
                LoginEvent secondFail = map.get("loginFail").get(1);
                LoginEvent thirdFail = map.get("loginFail").get(2);
                collector.collect(firstFail.userId + " 连续三次登录失败, 登录时间: " +
                        firstFail.ts + ", " +
                        secondFail.ts + ", " +
                        thirdFail.ts + ".");
            }
        }).print();

        environment.execute();
    }
}
