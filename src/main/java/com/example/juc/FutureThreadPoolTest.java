package com.example.juc;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.*;

/**
 * @author liuwe 2022/8/23 16:55
 */
public class FutureThreadPoolTest {
    public static void main(String[] args) {
        long start = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        // 方法一：单线程运行
//        m1();

        // 方法二：多线程运行, future+线程池异步多线程任务, 提高任务处理速度,
        ThreadPoolExecutor pool = new ThreadPoolExecutor(3, 3, 3000,
                TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(1024), new ThreadPoolExecutor.CallerRunsPolicy());
        FutureTask<String> futureTask1 = new FutureTask<>(() -> {
            TimeUnit.MILLISECONDS.sleep(600);
            return "task-1";
        });
//        new Thread(futureTask1, "thread-1").start();
        pool.submit(futureTask1);

        FutureTask<String> futureTask2 = new FutureTask<>(() -> {
            TimeUnit.MILLISECONDS.sleep(200);
            return "task-2";
        });
        pool.submit(futureTask2);

        try {
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            // get()方法是阻塞的方法，最好放在程序最后调用, 否则容易导致程序阻塞;
            // 也可以调用get(long timeout, TimeUnit unit)方法，传入超时抛异常的方式中断阻塞
            System.out.println(futureTask1.get());
            System.out.println(futureTask2.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        pool.shutdown();

        long end = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println("cost time: " + (end - start) + "ms");

        System.out.println(Thread.currentThread().getName() + " end!");
    }

    public static void m1() {
        try {
            TimeUnit.MILLISECONDS.sleep(600);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
