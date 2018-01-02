package qunar.tc.qmq.model;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yunfeng.yang
 * @since 2017/8/1
 */
public class ActionLogOffset {
    private AtomicLong pullLogOffset;
    private AtomicLong ackLogOffset;

    private Lock pullLock = new ReentrantLock();
    private Semaphore ackSemaphore = new Semaphore(1);

    public ActionLogOffset(final long pullLogOffset, final long ackLogOffset) {
        this.pullLogOffset = new AtomicLong(pullLogOffset);
        this.ackLogOffset = new AtomicLong(ackLogOffset);
    }

    public long getPullLogOffset() {
        return pullLogOffset.get();
    }

    public void setPullLogOffset(long pullLogOffset) {
        this.pullLogOffset.set(pullLogOffset);
    }

    public long getAckLogOffset() {
        return ackLogOffset.get();
    }

    public void setAckLogOffset(long ackLogOffset) {
        this.ackLogOffset.set(ackLogOffset);
    }

    public void pullLock() {
        pullLock.lock();
    }

    public void pullUnlock() {
        pullLock.unlock();
    }

    public void ackSemaphoreAcquire() {
        try {
            ackSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("acquire action semaphore error", e);
        }
    }

    public void ackSemaphoreRelease() {
        ackSemaphore.release();
    }
}
