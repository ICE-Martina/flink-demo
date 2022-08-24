package com.example.juc;

import java.util.concurrent.*;

/**
 * @author liuwe 2022/8/24 9:49
 */
public class CompletableFutureDemo {
    public static void main(String[] args) {

        // 创建1：一般不用
//        CompletableFuture<String> completableFuture = new CompletableFuture<>();

        // 创建2：无返回值, 使用默认的线程池
        CompletableFuture<Void> runAsync = CompletableFuture.runAsync(() -> {
            System.out.println(Thread.currentThread().getName());
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        try {
            System.out.println(runAsync.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // 创建3：无返回值, 使用自定义线程池
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(3, 3, 1,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>());
        /*CompletableFuture.runAsync(() -> {
            System.out.println(Thread.currentThread().getName());
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, poolExecutor);*/

        // 创建4：有返回值, 使用默认线程池
        CompletableFuture<String> supplyAsync = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName());
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "task-1";
        });

        // 创建5：有返回值, 使用自定义线程池
        CompletableFuture<String> supplyAsync1 = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName());
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "task-2";
        }, poolExecutor);

        try {
            System.out.println(supplyAsync.get());
            System.out.println(supplyAsync1.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        poolExecutor.shutdown();

    }
}
