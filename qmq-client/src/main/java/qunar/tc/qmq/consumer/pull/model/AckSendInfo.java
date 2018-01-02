package qunar.tc.qmq.consumer.pull.model;

/**
 * @author yiqun.fan create on 17-8-20.
 */
public class AckSendInfo {
    private int toSendNum = 0;
    private long minPullOffset = -1;
    private long maxPullOffset = -1;

    public int getToSendNum() {
        return toSendNum;
    }

    public void setToSendNum(int toSendNum) {
        this.toSendNum = toSendNum;
    }

    public long getMinPullOffset() {
        return minPullOffset;
    }

    public void setMinPullOffset(long minPullOffset) {
        this.minPullOffset = minPullOffset;
    }

    public long getMaxPullOffset() {
        return maxPullOffset;
    }

    public void setMaxPullOffset(long maxPullOffset) {
        this.maxPullOffset = maxPullOffset;
    }

    @Override
    public String toString() {
        return "(" + minPullOffset + ", " + maxPullOffset + ", " + toSendNum + ")";
    }
}
