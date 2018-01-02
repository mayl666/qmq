package qunar.tc.qmq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yunfeng.yang
 * @since 2017/7/6
 */
public class ConsumerLogOffset {
    private String subject;
    private AtomicLong confirmedOffset;
    @JsonIgnore
    private Lock lock = new ReentrantLock();

    @JsonCreator
    public ConsumerLogOffset(@JsonProperty("subject") String subject, @JsonProperty("offset") long offset) {
        this.subject = subject;
        this.confirmedOffset = new AtomicLong(offset);
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public long getOffset() {
        return confirmedOffset.get();
    }

    public void setOffset(long offset) {
        this.confirmedOffset.set(offset);
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    @Override
    public String toString() {
        return "ConsumerLogOffset{" +
                "subject='" + subject + '\'' +
                ", confirmedOffset=" + confirmedOffset +
                '}';
    }
}
