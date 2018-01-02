package qunar.tc.qmq.consumer.pull.model;

import qunar.tc.qmq.broker.BrokerGroupInfo;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class PullParam {
    private BrokerGroupInfo brokerGroup;
    private int pullBatchSize;
    private long timeoutMillis;
    private long consumeOffset;
    private long minPullOffset;
    private long maxPullOffset;
    private String consumerId;
    private boolean isBroadcast;

    public BrokerGroupInfo getBrokerGroup() {
        return brokerGroup;
    }

    public void setBrokerGroup(BrokerGroupInfo brokerGroup) {
        this.brokerGroup = brokerGroup;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public long getConsumeOffset() {
        return consumeOffset;
    }

    public void setConsumeOffset(long consumeOffset) {
        this.consumeOffset = consumeOffset;
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

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public void setBroadcast(boolean broadcast) {
        isBroadcast = broadcast;
    }
}
