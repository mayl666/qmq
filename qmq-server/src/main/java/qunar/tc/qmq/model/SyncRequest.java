package qunar.tc.qmq.model;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
public class SyncRequest {
    private final long messageLogOffset;
    private final long actionLogOffset;
    private final int syncType;

    public SyncRequest(int syncType, long messageLogOffset, long actionLogOffset) {
        this.syncType = syncType;
        this.messageLogOffset = messageLogOffset;
        this.actionLogOffset = actionLogOffset;
    }

    public int getSyncType() {
        return syncType;
    }

    public long getMessageLogOffset() {
        return messageLogOffset;
    }

    public long getActionLogOffset() {
        return actionLogOffset;
    }

    @Override
    public String toString() {
        return "SyncRequest{" +
                "messageLogOffset=" + messageLogOffset +
                ", actionLogOffset=" + actionLogOffset +
                ", syncType=" + syncType +
                '}';
    }
}
