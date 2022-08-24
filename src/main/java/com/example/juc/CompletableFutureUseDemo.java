package com.example.juc;

import java.util.concurrent.*;

/**
 * @author liuwe 2022/8/24 10:07
 */
public class CompletableFutureUseDemo {
    public static void main(String[] args) {
//        future1();

        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(2, 2, 2000,
                TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        try {
            CompletableFuture.supplyAsync(() -> {
                System.out.println(Thread.currentThread().getName() + "--------come in.");
                int rand = ThreadLocalRandom.current().nextInt(10);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(rand);
                if (rand>5){
                    throw new IllegalArgumentException("random > 5");
                }
                return rand;
            }, poolExecutor).whenComplete((v, e) -> {
                if (e == null) {
                    System.out.println("random --> " + v);
                }
            }).exceptionally(e -> {
                e.printStackTrace();
                System.out.println("异常情况: " + e.getCause() + "\t" + e.getMessage());
                return null;
            });
            System.out.println(Thread.currentThread().getName());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            poolExecutor.shutdown();
        }

        // 使用默认线程池需要等待一下, 防止main线程执行太快, 未等到子线程返回结果, main线程已经结束, 关闭进程
        /*try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }

    private static void future1() {
        CompletableFuture<Integer> integerCompletableFuture = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName() + "-------come in.");
            int rand = ThreadLocalRandom.current().nextInt(10);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("结果: " + rand);
            return rand;
        });
        System.out.println(Thread.currentThread().getName());
        try {
            System.out.println(integerCompletableFuture.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
