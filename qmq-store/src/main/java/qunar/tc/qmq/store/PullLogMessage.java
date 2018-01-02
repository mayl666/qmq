package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/8/1
 */
public class PullLogMessage {
    private final long sequence;
    private final long messageSequence;

    public PullLogMessage(final long sequence, final long messageSequence) {
        this.sequence = sequence;
        this.messageSequence = messageSequence;
    }

    public long getSequence() {
        return sequence;
    }

    public long getMessageSequence() {
        return messageSequence;
    }

    @Override
    public String toString() {
        return "PullLogMessage{" +
                "sequence=" + sequence +
                ", messageSequence=" + messageSequence +
                '}';
    }
}
