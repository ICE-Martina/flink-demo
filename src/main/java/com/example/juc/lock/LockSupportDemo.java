package com.example.juc.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liuwe 2022/8/30 8:28
 */
public class LockSupportDemo {
    public static void main(String[] args) {

//        waitNotifyTest();

//        lockConditionTest();

        lockSupportTest();
    }

    /**
     * LockSupport无锁块要求. LockSupport调用的是Unsafe的native代码.
     * 可先唤醒后等待, 即先unpark(t1)后park(), 要成对出现.
     * LockSupport和每个使用它的线程都有一个许可(permit)关联.
     * 每个线程都有一个相关的permit, permit最多只有一个, 重复调用unpark也不会累计凭证.
     */
    private static void lockSupportTest() {
        Thread t1 = new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName() + " 进入...");
            LockSupport.park();
//            LockSupport.park();
            System.out.println(Thread.currentThread().getName() + " 被唤醒...");
        }, "t1");
        t1.start();
        /*try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        new Thread(() -> {
            // 传入需要唤醒的线程, 发出通行证唤醒.
            LockSupport.unpark(t1);
            // 许可证不会累计, 最多只有一个, 即只有一个, 一一对应
//            LockSupport.unpark(t1);
            System.out.println(Thread.currentThread().getName() + " 向 t1 发通行证...");
        }, "t2").start();
    }

    /**
     * await()和signal()方法必须先获取锁才能使用.否则会抛异常java.lang.IllegalMonitorStateException
     * 必须先等待后唤醒, 即先await()后signal(), 否则会一直阻塞.
     */
    private static void lockConditionTest() {
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        new Thread(() -> {
            lock.lock();
            System.out.println(Thread.currentThread().getName() + " 进入 lock...");
            try {
                condition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
            System.out.println(Thread.currentThread().getName() + " t1 的 await() 被唤醒...");
        }, "t1").start();

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(() -> {
            lock.lock();
            try {
                condition.signal();
                System.out.println(Thread.currentThread().getName() + " 向 t1 发出唤醒通知...");
            } finally {
                lock.unlock();
            }
        }, "t2").start();
    }

    /**
     * wait和notify方法必须要在同步代码快或者同步方法中调用, 且成对出现.否则会抛异常java.lang.IllegalMonitorStateException
     * 必须先等待后唤醒, 即先wait后notify, 否则会一直阻塞.
     */
    private static void waitNotifyTest() {
        Object lock = new Object();
        new Thread(() -> {
            /*try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            synchronized (lock) {
                System.out.println(Thread.currentThread().getName() + " 进入 synchronized...");
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + " wait() 被唤醒...");
            }
        }, "t1").start();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(() -> {
            synchronized (lock) {
                lock.notify();
                System.out.println(Thread.currentThread().getName() + " 发出唤醒 t1 的通知...");
            }
        }, "t2").start();
    }

}
