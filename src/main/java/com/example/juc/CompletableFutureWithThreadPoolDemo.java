package com.example.juc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author liuwe 2022/8/25 8:20
 */
public class CompletableFutureWithThreadPoolDemo {
    public static void main(String[] args) {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(3, 3, 1000,
                TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        CompletableFuture.supplyAsync(() -> {
            System.out.println("task-1-" + Thread.currentThread().getName());
            return "task-1";
        }, poolExecutor)
                .thenRun(() -> System.out.println("task-2-" + Thread.currentThread().getName()))
                .thenRunAsync(() -> System.out.println("task-3-" + Thread.currentThread().getName()))
                .thenRun(() -> System.out.println("task-4-" + Thread.currentThread().getName()));
        poolExecutor.shutdown();
    }
}
