package qunar.tc.qmq.base;

/**
 * @author yiqun.fan create on 17-7-27.
 */
public class Ack {
    private long offset;
    private long consumerLogOffset;
    private AckStatus status;
    private long elapsed;
    private int times;
    private long next;
    private BaseMessage message;
    private RawMessage rawMessage;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public AckStatus getStatus() {
        return status;
    }

    public void setStatus(AckStatus status) {
        this.status = status;
    }

    public long getElapsed() {
        return elapsed;
    }

    public void setElapsed(long elapsed) {
        this.elapsed = elapsed;
    }

    public int getTimes() {
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
    }

    public long getNext() {
        return next;
    }

    public void setNext(long next) {
        this.next = next;
    }

    public BaseMessage getMessage() {
        return message;
    }

    public void setMessage(BaseMessage message) {
        this.message = message;
    }

    public boolean isFailed() {
        return status != AckStatus.OK;
    }

    public long getConsumerLogOffset() {
        return consumerLogOffset;
    }

    public void setConsumerLogOffset(long consumerLogOffset) {
        this.consumerLogOffset = consumerLogOffset;
    }

    public RawMessage getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(RawMessage rawMessage) {
        this.rawMessage = rawMessage;
    }

    @Override
    public String toString() {
        return "Ack{" +
                "offset=" + offset +
                ", consumerLogOffset=" + consumerLogOffset +
                ", status=" + status +
                ", elapsed=" + elapsed +
                ", times=" + times +
                ", next=" + next +
                ", message=" + message +
                '}';
    }
}
