package qunar.tc.qmq;

import java.util.concurrent.Semaphore;

/**
 * @author yunfeng.yang
 * @since 2017/8/22
 */
public class SemaphoreTest {
    public static void main(String[] args) throws InterruptedException {
        Semaphore semaphore = new Semaphore(1);
        semaphore.acquire();
        System.out.println("1");
        semaphore.release();
        semaphore.acquire();
        System.out.println("2");
    }
}
