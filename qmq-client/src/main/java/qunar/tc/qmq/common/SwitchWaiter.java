package qunar.tc.qmq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class SwitchWaiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchWaiter.class);

    private static final int WAIT_TIMEOUT = 60000;  // 1分钟

    private volatile boolean switcher;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public SwitchWaiter(boolean initValue) {
        switcher = initValue;
    }

    public void on() {
        if (!switcher) {
            change(true);
        }
    }

    public void off() {
        if (switcher) {
            change(false);
        }
    }

    private void change(boolean switcher) {
        lock.lock();
        try {
            this.switcher = switcher;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitOn() {
        if (switcher) {
            return;
        }
        lock.lock();
        try {
            while (!switcher) {
                try {
                    condition.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOGGER.info("ignore interrupt switch waitOn");
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
