package com.example.juc.lock;

import java.util.concurrent.TimeUnit;

/**
 * @author liuwe 2022/8/26 16:45
 */
public class DeadLockDemo {
    public static void main(String[] args) {
        final Object a = new Object();
        final Object b = new Object();
        new Thread(()->{
            synchronized (a){
                System.out.println(Thread.currentThread().getName() + "\t持有a锁, 希望持有b锁");
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (b){
                    System.out.println(Thread.currentThread().getName() + "\t持有b锁");
                }
            }
        },"A").start();

        new Thread(()->{
            synchronized (b){
                System.out.println(Thread.currentThread().getName() + "\t持有b锁, 希望持有a锁");

                synchronized (a){
                    System.out.println(Thread.currentThread().getName() + "\t持有a锁");
                }
            }
        },"B").start();
    }

}
