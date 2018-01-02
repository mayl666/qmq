package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public class MessageLogMeta {
    private final String subject;
    private final long sequence;
    private final long wroteOffset;
    private final int wroteBytes;
    private final long payloadOffset;

    public MessageLogMeta(String subject, long sequence, long wroteOffset, int wroteBytes, long payloadOffset) {
        this.subject = subject;
        this.sequence = sequence;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.payloadOffset = payloadOffset;
    }

    public String getSubject() {
        return subject;
    }

    public long getSequence() {
        return sequence;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public long getPayloadOffset() {
        return payloadOffset;
    }
}
