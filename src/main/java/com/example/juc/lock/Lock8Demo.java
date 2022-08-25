package com.example.juc.lock;

import java.util.concurrent.TimeUnit;

/**
 * 口诀： 线程 操作 资源类
 * 1. 标准访问a、b两个线程, 先输出sendEmail, 还是先输出sendSMS?  先输出sendEmail
 * 2. sendEmail方法中暂停3s, 先输出sendEmail, 还是先输出sendSMS?  先输出sendEmail
 * 3. 添加一个普通的hello方法, 先输出sendEmail, 还是先输出hello?  先输出hello
 * 4. 有两部手机, 先输出sendEmail, 还是先输出sendSMS?   先输出sendSMS
 * 5. 有两个静态同步方法, 1部手机, 先输出sendEmail, 还是先输出sendSMS?  先输出sendEmail
 * 6. 有两个静态同步方法, 2部手机, 先输出sendEmail, 还是先输出sendSMS?  先输出sendEmail
 * 7. 有1个静态同步方法, 有1个普通同步方法, 1部手机, 先输出sendEmail, 还是先输出sendSMS?  先输出sendSMS
 * 8. 有1个静态同步方法, 有1个普通同步方法, 2部手机, 先输出sendEmail, 还是先输出sendSMS?  先输出sendSMS
 *
 * 总结：
 * 1-2问题：一个类里面如果有多个synchronized修饰的方法, 某一时刻内, 只要有一个线程去调用其中一个synchronized方法, 其他线程只能等待,
 * 只能有唯一的线程访问这些synchronized方法, 锁的是当前对象this, 被锁定后, 其他线程都不能进入到当前对象的其他synchronized方法.
 * 3-4问题：添加的普通方法与同步锁无关; 换成两个对象后, 不是同一把锁, 互相影响.
 * 5-6问题：对于普通同步方法, 锁的是当前实例对象, 通常指this;
 * 对于静态同步方法, 锁的是当前类的Class对象;
 * 对于同步方法快, 锁的是synchronized括号内的对象
 * 7-8问题：当一个线程试图访问同步代码时, 它首先必须得到锁, 正常退出或者抛出异常时必须释放锁;
 * 所有的普通同步方法用的都是同一把锁----具体实例对象本身, 即this, 也就是说如果一个实例对象的普通同步方法获取锁后, 该实例对象的其他普通同步方法
 * 必须等待获取锁的方法释放锁后才能获取锁;
 * 所有的静态同步方法用的也是一把锁----类对象本身, 就是唯一模版Class, 具体实例对象this与唯一模版Class, 这两把锁是两个不同的对象,
 * 所以静态同步方法与普通同步方法不存在竞态条件; 一个静态同步方法获取锁后, 其他静态同步方法必须等待获取锁的方法释放锁后才能获取锁.
 *
 *
 * @author liuwe 2022/8/25 9:11
 */

class Phone {
    public static synchronized void sendEmail() {
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("---------sendEmail");
    }

    public synchronized void sendSMS() {
        System.out.println("-----------sendSMS");
    }

    public void hello() {
        System.out.println("-------------hello");
    }
}

public class Lock8Demo {
    public static void main(String[] args) {

        Phone phone = new Phone();
        Phone phone2 = new Phone();

//        new Thread(phone::sendEmail, "a").start();
        new Thread(() -> {
            phone.sendEmail();
        }, "a").start();

        try {
            // 保证a线程先启动
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        new Thread(phone::sendSMS, "b").start();
//        new Thread(phone::hello, "b").start();
//        new Thread(phone2::sendSMS).start();
        new Thread(() -> {
            phone2.sendSMS();
//            phone.sendSMS();
        }, "b").start();
    }
}


