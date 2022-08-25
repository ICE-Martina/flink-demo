package com.example.juc.lock;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liuwe 2022/8/25 13:54
 */
public class SaleTicketDemo {
    public static void main(String[] args) {
        Ticket ticket = new Ticket();
        new Thread(() -> {
            for (int i = 0; i < 55; i++) {
                ticket.sale();
            }
        }, "a").start();
        new Thread(() -> {
            for (int i = 0; i < 55; i++) {
                ticket.sale();
            }
        }, "b").start();
        new Thread(() -> {
            for (int i = 0; i < 55; i++) {
                ticket.sale();
            }
        }, "c").start();
    }

}

class Ticket {
    private int number = 50;
    /*
     *
     * 默认是false, 即非公平锁; 减少线程切换的开销, 充分利用cpu资源
     * 设置为true, 即公平锁; 会增加线程切换的开销
     *
     */
    //    ReentrantLock lock = new ReentrantLock();
    ReentrantLock lock = new ReentrantLock(true);

    public void sale() {
        lock.lock();
        try {
            if (number > 0) {
                System.out.println(Thread.currentThread().getName() + "\t 卖出第: " + (number--) + "\t 还剩下: " + number);
            }
        } finally {
            lock.unlock();
        }
    }
}