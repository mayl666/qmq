package qunar.tc.qmq.model;

/**
 * @author yunfeng.yang
 * @since 2017/8/2
 */
public class ConsumerPoint {
    private final ConsumerGroup consumerGroup;
    private final String consumerId;

    public ConsumerPoint(final String consumerId, final ConsumerGroup consumerGroup) {
        this.consumerGroup = consumerGroup;
        this.consumerId = consumerId;
    }

    public ConsumerGroup getConsumerGroup() {
        return consumerGroup;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getSubject() {
        return consumerGroup.getSubject();
    }

    public String getGroup() {
        return consumerGroup.getGroup();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerPoint that = (ConsumerPoint) o;

        if (consumerGroup != null ? !consumerGroup.equals(that.consumerGroup) : that.consumerGroup != null)
            return false;
        return consumerId != null ? consumerId.equals(that.consumerId) : that.consumerId == null;
    }

    @Override
    public int hashCode() {
        int result = consumerGroup != null ? consumerGroup.hashCode() : 0;
        result = 31 * result + (consumerId != null ? consumerId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerPoint{" +
                "consumerGroup=" + consumerGroup +
                ", consumerId='" + consumerId + '\'' +
                '}';
    }
}
