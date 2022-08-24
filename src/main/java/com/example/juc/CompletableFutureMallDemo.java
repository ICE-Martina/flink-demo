package com.example.juc;

import lombok.Getter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 1.同一款产品, 同时搜索出同款产品在各大电商平台的售价;
 * 2.同一款产品, 同时搜索出本产品在同一个电商平台下, 各个入驻卖家售价是是多少.
 *
 * @author liuwe 2022/8/24 13:38
 */
public class CompletableFutureMallDemo {
    static List<NetMall> mallList = Arrays.asList(
            new NetMall("jd"),
            new NetMall("tao bao"),
            new NetMall("dang dang")
    );

    public static List<String> getPrice(List<NetMall> list, String productName) {
        return list.stream().map(netMall ->
                String.format(productName + " in %s price is %.2f.", netMall.getNetMallName(), netMall.calcPrice(productName)))
                .collect(Collectors.toList());
    }

    public static List<String> getPriceCompletableFuture(List<NetMall> list, String productName) {
        return list.stream().map(netMall -> CompletableFuture.supplyAsync(() ->
                String.format(productName + " in %s price is %.2f.", netMall.getNetMallName(), netMall.calcPrice(productName))))
                .collect(Collectors.toList()).stream().map(CompletableFuture::join).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        long start1 = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        // 第一种方法:
        List<String> list = getPrice(mallList, "java");
        list.forEach(System.out::println);
        long end1 = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println("cost time: " + (end1 - start1) + "ms");

        long start2 = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        // 第二种方法:
        List<String> javas = getPriceCompletableFuture(mallList, "java");
        javas.forEach(System.out::println);

        long end2 = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println("cost time: " + (end2 - start2) + "ms");
    }
}

class NetMall {
    @Getter
    private final String netMallName;

    public NetMall(String netMallName) {
        this.netMallName = netMallName;
    }

    public double calcPrice(String productName) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return ThreadLocalRandom.current().nextDouble() * 2 + productName.charAt(0);
    }
}
