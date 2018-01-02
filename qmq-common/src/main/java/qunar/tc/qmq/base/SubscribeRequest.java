package qunar.tc.qmq.base;

import java.io.Serializable;

/**
 * @author keli.wang
 * @since 2017/1/16
 */
public class SubscribeRequest implements Serializable {
    private String consumer;
    private String prefix;
    private String group;

    public SubscribeRequest() {
    }

    public SubscribeRequest(final String consumer, final String prefix, final String group) {
        this.consumer = consumer;
        this.prefix = prefix;
        this.group = group;
    }

    public String getConsumer() {
        return consumer;
    }

    public void setConsumer(String consumer) {
        this.consumer = consumer;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubscribeRequest that = (SubscribeRequest) o;

        if (consumer != null ? !consumer.equals(that.consumer) : that.consumer != null) return false;
        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) return false;
        return group != null ? group.equals(that.group) : that.group == null;
    }

    @Override
    public int hashCode() {
        int result = consumer != null ? consumer.hashCode() : 0;
        result = 31 * result + (prefix != null ? prefix.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        return result;
    }
}
