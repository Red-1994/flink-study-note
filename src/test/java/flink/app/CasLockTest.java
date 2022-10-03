package flink.app;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/8/23 17:32<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class CasLockTest {
    public static void main(String[] args) {
        AtomicInteger integer = new AtomicInteger();

        integer.addAndGet(1);
        integer.getAndAdd(1);
        integer.get();
        integer.getAndIncrement();
        integer.getAndSet(1);
    }
}
