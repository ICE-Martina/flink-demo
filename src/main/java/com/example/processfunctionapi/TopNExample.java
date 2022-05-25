package com.example.processfunctionapi;

import com.example.common.Event;
import com.example.common.UrlCount;
import com.example.sourceapi.ClickSource;
import com.example.timeWindowapi.AggregateAndProcessFunctionTest2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liuwei
 * @date 2022/5/24 18:43
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Event> stream = environment.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        DataStream<UrlCount> urlCount = stream.keyBy(data -> data.url).window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateAndProcessFunctionTest2.CountUrlStatistics(),
                        new AggregateAndProcessFunctionTest2.UrlStatisticsResult());

//        urlCount.print("url-count");

        urlCount.keyBy(data -> data.end).process(new TopNProcessResult(3)).print();

        environment.execute();
    }

    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlCount, String> {
        private final Integer n;

        private ListState<UrlCount> urlCountListState;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) {
            urlCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("url-count", Types.POJO(UrlCount.class)));
        }

        @Override
        public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
            // 将数据保存到状态中
            urlCountListState.add(value);

            // 注册定时器
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<UrlCount> urlCountList = new ArrayList<>();
            for (UrlCount urlCount : urlCountListState.get()) {
                urlCountList.add(urlCount);
            }
            urlCountList.sort((o1, o2) -> o2.count - o1.count);

            StringBuilder topN = new StringBuilder();
            topN.append("----------------------------------------------------------------------------------\n");
            topN.append("窗口结束时间: ").append(new Timestamp(ctx.getCurrentKey())).append("\n");
            for (int i = 0; i < n; i++) {
                int number = i + 1;
                UrlCount urlCount = urlCountList.get(i);
                topN.append("top ").append(number).append(" url: ").append(urlCount.url)
                        .append(", 访问量: ").append(urlCount.count).append("\n");
            }
            topN.append("----------------------------------------------------------------------------------\n");

            out.collect(topN.toString());
        }
    }
}
