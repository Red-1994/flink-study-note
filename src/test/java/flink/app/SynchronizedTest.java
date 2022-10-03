package flink.app;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * <b>Synchronized 锁的使用</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/23 17:21<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class SynchronizedTest {
    public synchronized void m1(){
        System.out.println("I am synchronized method()");
    }

    public static synchronized void m2(){
        System.out.println("I am synchronized static method()");
    }


    public static void main(String[] args) {
        SynchronizedTest test = new SynchronizedTest();

        synchronized (test){
            System.out.println("I am synchronized code block");
        }

        AtomicStampedReference reference = new AtomicStampedReference(1, 10001);

        System.out.println("getReference = " + reference.getReference());
        System.out.println("getStamp = " + reference.getStamp());
        reference.set(2,10002);
        System.out.println("getReference = " +reference.getReference());
        System.out.println("getStamp = " + reference.getStamp());


    }
}
