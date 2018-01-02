package qunar.tc.qmq.consumer.pull.model;

import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.pull.AckSendQueue;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yiqun.fan create on 17-7-20.
 */
public class AckEntry {
    private final AckSendQueue ackSendQueue;
    private final long pullOffset;
    private final AtomicBoolean completing = new AtomicBoolean(false);
    private final AtomicBoolean done = new AtomicBoolean(false);
    private volatile AckEntry next;

    public AckEntry(AckSendQueue ackSendQueue, long pullOffset) {
        this.ackSendQueue = ackSendQueue;
        this.pullOffset = pullOffset;
    }

    public void setNext(AckEntry next) {
        this.next = next;
    }

    public AckEntry next() {
        return next;
    }

    public long pullOffset() {
        return pullOffset;
    }

    public void ack() {
        if (!completing.compareAndSet(false, true)) {
            return;
        }
        completed();
    }

    public void nack(final int nextRetryCount, final BaseMessage message) {
        if (!completing.compareAndSet(false, true)) {
            return;
        }
        ackSendQueue.sendbackAndCompleteNack(nextRetryCount, message, this);
    }

    public void ackDelay(int nextRetryCount, long nextRetryTime, BaseMessage message) {
        // TODO consumer
        nack(nextRetryCount, message);
    }

    public void completed() {
        done.set(true);
        ackSendQueue.ackCompleted(this);
    }

    public boolean isDone() {
        return done.get();
    }
}
