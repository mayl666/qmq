package qunar.tc.qmq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author yunfeng.yang
 * @since 2017/7/31
 */
public class ConsumerPrefixGroup {
    private final String prefix;
    private final String group;

    @JsonCreator
    public ConsumerPrefixGroup(@JsonProperty("prefix") String prefix,
                               @JsonProperty("group") String group) {
        this.prefix = prefix;
        this.group = group;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerPrefixGroup that = (ConsumerPrefixGroup) o;

        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) return false;
        return group != null ? group.equals(that.group) : that.group == null;
    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + (group != null ? group.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerPrefixGroup{" +
                "prefix='" + prefix + '\'' +
                ", group='" + group + '\'' +
                '}';
    }
}
