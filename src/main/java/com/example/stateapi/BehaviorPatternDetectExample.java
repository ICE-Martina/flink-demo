package com.example.stateapi;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author liuwei
 * @date 2022/5/30 10:24
 */
public class BehaviorPatternDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
//        environment.setStateBackend(new HashMapStateBackend());
//        environment.setStateBackend(new EmbeddedRocksDBStateBackend());
        /*DataStream<Action> userAction = environment.addSource(new ClickSource()).map(data -> {
            String url = data.url;
            if (url.contains("login")) {
                return new Action(data.user, "login");
            } else if (url.contains("buy")) {
                return new Action(data.user, "pay");
            } else if (url.contains("order")) {
                return new Action(data.user, "order");
            } else if (url.contains("pay")) {
                return new Action(data.user, "pay");
            } else {
                return new Action(data.user, "other");
            }
        });
        userAction.print("data");*/

        DataStream<Action> userAction = environment.fromElements(
                new Action("11", "login"),
                new Action("12", "login"),
                new Action("11", "cart"),
                new Action("12", "order")
        );

        DataStream<Pattern> patternStream = environment.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "cart"),
                new Pattern("login", "order"));

        // 定义广播状态描述器
        MapStateDescriptor<Void, Pattern> pattern = new MapStateDescriptor<>("pattern-state", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcast = patternStream.broadcast(pattern);

        userAction.keyBy(data -> data.userName)
                .connect(broadcast)
                .process(new PatternDetector())
                .print();

        environment.execute();
    }

    public static class Action {
        public String userName;
        public String action;

        public Action() {
        }

        public Action(String userName, String action) {
            this.userName = userName;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userName='" + userName + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }

    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        // 定义一个状态保存上一次用户的行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<>("prev-state", String.class));
        }

        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 获取匹配的规则
            ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(
                    new MapStateDescriptor<>("pattern-state", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = patternState.get(null);

            // 获取用户上一次的行为
            String preAction = prevActionState.value();

            // 判断是否符合规则
            if (preAction != null && pattern != null) {
                if (pattern.action1.equals(preAction) && pattern.action2.equals(value.action)) {
                    out.collect(Tuple2.of(ctx.getCurrentKey(), pattern));
                }
            }
            // 更新状态
            prevActionState.update(value.action);
        }

        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 更新为最新的规则
            BroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(
                    new MapStateDescriptor<>("pattern-state", Types.VOID, Types.POJO(Pattern.class)));

            patternState.put(null, value);
        }
    }
}