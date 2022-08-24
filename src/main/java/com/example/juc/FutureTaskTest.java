package com.example.juc;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * @author liuwe 2022/8/23 16:48
 */
public class FutureTaskTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        FutureTask<String> futureTask = new FutureTask<>(new CustomThread());

        Thread thread1 = new Thread(futureTask, "thread-1");
        thread1.start();
        System.out.println(futureTask.get());
    }
}

class CustomThread implements Callable<String> {

    @Override
    public String call() throws Exception {
        System.out.println("call() method...");
        return "Callable -> call()";
    }
}
