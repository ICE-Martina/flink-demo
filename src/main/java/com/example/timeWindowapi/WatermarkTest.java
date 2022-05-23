package com.example.timeWindowapi;

import com.example.common.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwei
 * @date 2022/5/23 13:08
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);
//        environment.getConfig().setAutoWatermarkInterval(100);

        DataStream<Event> streamSource = environment.fromElements(
                new Event("patton", "/collectList.htm?spm=a1z02.1", 1000L),
                new Event("stellar", "/collectList.htm?spm=a1z02.1", 2000L),
                new Event("patton", "/collectList.htm?spm=a1z02.1", 3000L),
                new Event("stellar", "/collectList.htm?spm=a1z02.1", 4000L),
                new Event("patton", "/collectList.htm?spm=a1z02.1", 3000L),
                new Event("stellar", "/collectList.htm?spm=a1z02.1", 3500L),
                new Event("patton", "/collectList.htm?spm=a1z02.1", 5000L),
                new Event("danika", "/collectList.htm?spm=a1z02.1", 4500L));
        // 有序流的watermark生成
        /*streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })).print();*/

        // 乱序流的Watermark生成
        /*streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })).print();*/

        // 自定义Watermark生成
        streamSource.assignTimestampsAndWatermarks(new CustomWatermarkStrategy()).print();

        environment.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                }
            };
        }
    }

    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {

        // 延迟时间
        private final Long delayTime = 3000L;
        // 观察到的最大时间戳
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次, 更新最大时间戳
            maxTs = Math.max(event.timestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线，默认200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}
