package com.example.juc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author liuwe 2022/8/25 8:49
 */
public class CompletableFutureCombineDemo {
    public static void main(String[] args) {
        CompletableFuture<Integer> task1 = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName() + " start-1...");
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 6;
        });

        CompletableFuture<Integer> task2 = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName() + " start-2...");
            try {
                TimeUnit.SECONDS.sleep(4);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 8;
        });

        CompletableFuture<Integer> combine = task1.thenCombine(task2, (v1, v2) -> {
            System.out.println(Thread.currentThread().getName() + " combine...");
            return v1 + v2;
        });

        System.out.println(Thread.currentThread().getName() + " ---> " + combine.join());

    }
}
