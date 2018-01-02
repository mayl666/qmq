package qunar.tc.qmq.consumer.pull.model;

/**
 * @author yiqun.fan create on 17-8-25.
 */
public class AckSendEntry {
    private final long pullOffsetBegin;
    private final long pullOffsetLast;

    public AckSendEntry() {
        this.pullOffsetBegin = -1;
        this.pullOffsetLast = -1;
    }

    public AckSendEntry(AckEntry first, AckEntry last) {
        this.pullOffsetBegin = first.pullOffset();
        this.pullOffsetLast = last.pullOffset();
    }

    public AckSendEntry(long pullOffsetBegin, long pullOffsetLast) {
        this.pullOffsetBegin = pullOffsetBegin;
        this.pullOffsetLast = pullOffsetLast;
    }

    public AckSendEntry merge(AckSendEntry entry) {
        if (entry == null) {
            return this;
        }
        return new AckSendEntry(Math.min(pullOffsetBegin, entry.pullOffsetBegin), Math.max(pullOffsetLast, entry.pullOffsetLast));
    }

    public long getPullOffsetBegin() {
        return pullOffsetBegin;
    }

    public long getPullOffsetLast() {
        return pullOffsetLast;
    }

    @Override
    public String toString() {
        return "(" + pullOffsetBegin + ", " + (pullOffsetLast - pullOffsetBegin) + ")";
    }
}
