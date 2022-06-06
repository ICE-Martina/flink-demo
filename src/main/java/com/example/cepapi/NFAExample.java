package com.example.cepapi;

import com.example.common.LoginEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liuwei
 * @date 2022/6/6 10:40
 */
public class NFAExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<LoginEvent> loginData = environment.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        )
                .keyBy(data -> data.userId);

        DataStream<String> result = loginData.flatMap(new StateMachineMapper());
        result.print();

        environment.execute();
    }

    public static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {

        ValueState<State> currentSate;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentSate = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent value, Collector<String> out) throws Exception {
            State state = currentSate.value();

            if (state == null) {
                state = State.Initial;
            }

            State nextState = state.transition(value.eventType);

            // 检测当前状态进行跳转
            if (nextState == State.Matched) {
                // 检测到了匹配, 输出报警信息; 不更新状态就是跳转回S2
                out.collect(value.userId + " 连续三次登录失败.");
            } else if (nextState == State.Matched) {
                // 直接将状态更新为初始状态, 重新检测
                currentSate.update(State.Initial);
            } else {
                // 状态覆盖跳转
                currentSate.update(nextState);
            }

        }
    }

    public enum State {
        Terminal, // 匹配失败, 终止状态
        Matched, // 匹配成功

        // S2状态, 传入基于S2状态可以进行的一系列状态转移
        S2(new Transition("fail", Matched), new Transition("success", Terminal)),

        // S1状态
        S1(new Transition("fail", S2), new Transition("success", Terminal)),

        // 初始状态
        Initial(new Transition("fail", S1), new Transition("success", Terminal));

        // 当前状态的转移规则
        private Transition[] transitions;

        State(Transition... transitions) {
            this.transitions = transitions;
        }

        public State transition(String eventType) {
            for (Transition transition : transitions) {
                if (transition.getEventType().equals(eventType)) {
                    return transition.getTargetState();
                }
            }
            return Initial;
        }
    }

    public static class Transition {
        private String eventType;
        private State targetState;

        public Transition(String eventType, State targetState) {
            this.eventType = eventType;
            this.targetState = targetState;
        }

        public String getEventType() {
            return eventType;
        }

        public State getTargetState() {
            return targetState;
        }
    }
}
