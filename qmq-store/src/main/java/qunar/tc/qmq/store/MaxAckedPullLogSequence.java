package qunar.tc.qmq.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class MaxAckedPullLogSequence {
    private final String subject;
    private final String group;
    private final String consumerId;

    private final AtomicLong maxSequence;

    @JsonCreator
    public MaxAckedPullLogSequence(@JsonProperty("subject") String subject,
                                   @JsonProperty("group") String group,
                                   @JsonProperty("consumerId") String consumerId,
                                   @JsonProperty("maxSequence") long maxSequence) {
        this.subject = subject;
        this.group = group;
        this.consumerId = consumerId;
        this.maxSequence = new AtomicLong(maxSequence);
    }

    public String getSubject() {
        return subject;
    }

    public String getGroup() {
        return group;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public long getMaxSequence() {
        return maxSequence.get();
    }

    public void setMaxSequence(final long maxSequence) {
        this.maxSequence.set(maxSequence);
    }
}
