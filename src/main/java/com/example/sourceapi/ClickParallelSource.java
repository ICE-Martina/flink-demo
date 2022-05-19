package com.example.sourceapi;

import com.example.common.Event;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author liuwei
 * @date 2022/5/19 14:35
 */
public class ClickParallelSource implements ParallelSourceFunction<Event> {
    private boolean running = true;
    private final Random random = new Random();

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        List<String> users = Lists.newArrayList("patton", "nicole", "anthony", "venus", "starlight",
                "stellar", "aldrich", "edgar", "danika");
        List<String> urls = Lists.newArrayList("https://item.example.com/17761516943.html",
                "https://item.example.com/pmp_item/649125436508.htm",
                "https://cart.example.com/cart.htm?spm=a220o",
                "https://shoucang.example.com/collectList.htm?spm=a1z02.1",
                "https://buy.example.com/order/confirm_order.htm?spm=a1z0d",
                "https://mall.example.hk/shop/tianwangbiao/index.html",
                "https://item.example.com/12949739254.html",
                "https://item.example.com/100037132852.html#crumb-wrap",
                "https://mall.example.com/index-11573727.html?from=pc");

        while (running) {
            String user = users.get(random.nextInt(users.size()));
            String url = urls.get(random.nextInt(urls.size()));
            Long timestamp = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
            // Long timestamp = LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"))
            ctx.collect(new Event(user, url, timestamp));
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        if (running) {
            running = false;
        }
    }
}
