package com.example.juc;

import java.util.concurrent.TimeUnit;

/**
 *
 * 实例方法interrupter()仅仅是设置线程的中断状态为true, 不会停止线程.
 *
 * @author liuwe 2022/8/27 17:12
 */
public class InterrupterDemo2 {
    public static void main(String[] args) {
        Thread t1 = new Thread(() -> {
            for (int i = 0; i <= 300; i++) {
                System.out.println(Thread.currentThread().getName() + ": " + i);
            }
            System.out.println(Thread.currentThread().getName() + ":" + Thread.currentThread().isInterrupted());
        }, "t1");
        t1.start();
        try {
            TimeUnit.MILLISECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t1.interrupt();
        System.out.println(Thread.currentThread().getName() + "-->t1: " + t1.isInterrupted());

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName() + "-------->t1: " + t1.isInterrupted());
    }

}
