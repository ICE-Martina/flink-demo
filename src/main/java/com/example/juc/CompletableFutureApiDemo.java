package com.example.juc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

/**
 * @author liuwe 2022/8/24 14:35
 */
public class CompletableFutureApiDemo {
    /*public static void main(String[] args) {
        CompletableFuture<String> supplyAsync = CompletableFuture.supplyAsync(() -> {
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "task-1 finish.";
        });

//        System.out.println(supplyAsync.get());
//        System.out.println(supplyAsync.get(2, TimeUnit.SECONDS));
//        System.out.println(supplyAsync.join());
//        System.out.println(supplyAsync.getNow("getNow"));
//        System.out.println(supplyAsync.complete("complete") + "\t" + supplyAsync.join());
        try {
            TimeUnit.SECONDS.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(supplyAsync.complete("complete") + "\t" + supplyAsync.join());
    }*/
    private ThreadPoolExecutor pool;

    @Before
    public void before() {
        pool = new ThreadPoolExecutor(3, 3, 1000,
                TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
    }

    @Test
    public void test1() {
        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "task-1 test.";
        });
        try {
            TimeUnit.SECONDS.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        System.out.println(supplyAsync.get());
//        System.out.println(supplyAsync.get(2, TimeUnit.SECONDS));
//        System.out.println(supplyAsync.join());
//        System.out.println(supplyAsync.getNow("getNow"));
//        System.out.println(supplyAsync.complete("complete") + "\t" + supplyAsync.join());
        System.out.println(completableFuture.complete("complete") + "\t" + completableFuture.join());
    }

    @Test
    public void test2() {
        CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("第一阶段...");
            return ThreadLocalRandom.current().nextInt(6);
        }, pool).thenApply(f -> {
            System.out.println("第二阶段...");
            return f + 1;
        }).thenApply(f -> {
            System.out.println("第三阶段...");
            int res = f + 1;
            if (res > 5) {
                throw new IllegalArgumentException(res + ", 超出预期值...");
            }
            return res;
        }).whenComplete((v, e) -> {
            if (e == null) {
                System.out.println("最终结果: " + v);
            }
        }).exceptionally(e -> {
            System.out.println("异常情况: " + e.getCause() + "\t" + e.getMessage());
            return null;
        });

        System.out.println(Thread.currentThread().getName() + "获取结果: " + completableFuture.join());
    }

    @Test
    public void test3() {
        CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("第一阶段...");
            return ThreadLocalRandom.current().nextInt(6);
        }, pool).handle((f, e) -> {
            System.out.println("第二阶段...");
            return f + 1;
        }).handle((f, e) -> {
            System.out.println("第三阶段...");
            int res = f + 1;
            if (res > 5) {
                throw new IllegalArgumentException(res + ", 超出预期值...");
            }
            return res;
        }).whenComplete((v, e) -> {
            if (e == null) {
                System.out.println("最终结果: " + v);
            }
        }).exceptionally(e -> {
            System.out.println("异常情况: " + e.getCause() + "\t" + e.getMessage());
            return null;
        });

        System.out.println(Thread.currentThread().getName() + "获取结果: " + completableFuture.join());
    }

    @Test
    public void test4() {
        CompletableFuture.supplyAsync(() -> ThreadLocalRandom.current().nextInt(6), pool)
                .thenApply(f -> f + 1).thenApply(f -> f + 1)
                .thenAccept(System.out::println);
    }

    @Test
    public void test5() {
        System.out.println(CompletableFuture.supplyAsync(() -> "task1", pool).thenAccept(System.out::println).join());
        System.out.println(CompletableFuture.supplyAsync(() -> "task1", pool).thenRun(() -> System.out.println("task2")).join());
        System.out.println(CompletableFuture.supplyAsync(() -> "task1", pool).thenApply(f -> f + ", task2").join());
    }

    @After
    public void after() {
        pool.shutdown();
    }
}
