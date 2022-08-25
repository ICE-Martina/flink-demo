package com.example.juc.lock;

import java.util.concurrent.locks.ReentrantLock;

/**
 * ObjectMonitor.hpp、ObjectMonitor.cpp
 * Java中ReentrantLock和synchronized都是可重入锁, 是指在同一个线程在外层方法获取锁的时候,
 * 再进入该线程的内层方法会自动获取锁(前提：锁对象是同一个对象), 不会因为之前已经获取过还没释放而阻塞.
 * 隐式锁（即synchronized关键字使用的锁）默认是可重入锁
 * 显示锁（即Lock）也有ReentrantLock这样的可重入锁。
 *
 * @author liuwe 2022/8/25 14:17
 */
public class ReEnterLockDemo {
    public static void main(String[] args) {
//        final Object object = new Object();
//        syncCodeBlock(object);
//        ReEnterLockDemo reEnterLockDemo = new ReEnterLockDemo();
//        new Thread(() -> reEnterLockDemo.m1(), "b").start();

        // 显示锁
        ReentrantLock reentrantLock = new ReentrantLock();

        new Thread(() -> {
            reentrantLock.lock();
            try {
                System.out.println(Thread.currentThread().getName() + "\t" + "外层");
                reentrantLock.lock();
                try {
                    System.out.println(Thread.currentThread().getName() + "\t" + "内层");
                } finally {
                    reentrantLock.unlock();
                }

            } finally {
                reentrantLock.unlock();
            }

        }, "t1").start();

    }

    public synchronized void m1() {
        System.out.println(Thread.currentThread().getName() + "\t" + "m1");
        m2();
        System.out.println(Thread.currentThread().getName() + "\t" + "end m1");
    }

    public synchronized void m2() {
        System.out.println(Thread.currentThread().getName() + "\t" + "m2");
        m3();
    }

    private void m3() {
        System.out.println(Thread.currentThread().getName() + "\t" + "m3");
    }

    private static void syncCodeBlock(Object object) {
        new Thread(() -> {
            synchronized (object) {
                System.out.println(Thread.currentThread().getName() + "\t" + "外层调用");
                synchronized (object) {
                    System.out.println(Thread.currentThread().getName() + "\t" + "中层调用");
                    synchronized (object) {
                        System.out.println(Thread.currentThread().getName() + "\t" + "内层调用");
                    }
                }
            }
        }, "a").start();
    }
}
