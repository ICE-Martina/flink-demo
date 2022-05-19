package com.example.transformations;

import com.example.common.PageView;
import com.example.sourceapi.ClickParallelSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwei
 * @date 2022/5/19 15:51
 */
public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<PageView> filterOperator = environment.addSource(new ClickParallelSource())
                .map(event -> new PageView(event.url, 1))
                .keyBy(data -> data.url).sum("count")
                // 1.lambda表达式
                .filter(pv -> pv.count > 2);
        // 2.匿名类
                /*.filter(new FilterFunction<PageView>() {
                    @Override
                    public boolean filter(PageView pageView) throws Exception {
                        return pageView.count > 2;
                    }
                });*/
        // 3.自定义类
//                .filter(new CustomFilterFunction());
        filterOperator.print();
        environment.execute();
    }

    public static class CustomFilterFunction implements FilterFunction<PageView> {

        @Override
        public boolean filter(PageView pageView) throws Exception {
            return pageView.count > 2;
        }
    }
}
