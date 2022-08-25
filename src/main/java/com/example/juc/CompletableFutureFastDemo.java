package com.example.juc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author liuwe 2022/8/25 8:40
 */
public class CompletableFutureFastDemo {
    public static void main(String[] args) {
        CompletableFuture<String> play1 = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName() + " play-1.");
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "play-1";
        });

        CompletableFuture<String> play2 = CompletableFuture.supplyAsync(() -> {
            System.out.println(Thread.currentThread().getName() + " play-2.");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "play-2";
        });

        CompletableFuture<String> result = play1.applyToEither(play2, f -> f + " win.");

        System.out.println(Thread.currentThread().getName() + "\t" + result.join());
    }
}
