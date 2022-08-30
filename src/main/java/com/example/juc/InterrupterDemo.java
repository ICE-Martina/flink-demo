package com.example.juc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liuwe 2022/8/27 16:38
 */
public class InterrupterDemo {
    static volatile boolean isStop = false;
    static AtomicBoolean atomFlag = new AtomicBoolean(false);

    public static void main(String[] args) {
//        mVolatile();
//        mAtom();
        mInterrupter();
    }

    private static void mInterrupter() {
        Thread t1 = new Thread(() -> {
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println(Thread.currentThread().getName() + " is interrupter.");
                    break;
                }
            }
            System.out.println(Thread.currentThread().getName() + " is running.");
        }, "t1");
        t1.start();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(() -> {
            t1.interrupt();
            System.out.println(Thread.currentThread().getName() + " is running.");
        }, "t2").start();
    }

    private static void mAtom() {
        new Thread(() -> {
            while (true) {
                if (atomFlag.get()) {
                    System.out.println(Thread.currentThread().getName() + "\tatomFlag alter true, t1 interrupter.");
                    break;
                }
                System.out.println(Thread.currentThread().getName() + "\t t1 running.");
            }
        }, "t1").start();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(() -> {
            atomFlag.set(true);
            System.out.println(Thread.currentThread().getName() + "\t t2");
        }, "t2").start();
    }

    private static void mVolatile() {
        new Thread(() -> {
            while (true) {
                if (isStop) {
                    System.out.println(Thread.currentThread().getName() + "\tisStop alter true, t1 interrupter.");
                    break;
                }
                System.out.println(Thread.currentThread().getName() + "\t t1 running.");
            }
        }, "t1").start();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(() -> {
            isStop = true;
            System.out.println(Thread.currentThread().getName() + "\t t2");
        }, "t2").start();
    }

}
