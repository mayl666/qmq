package qunar.tc.qmq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * @author yunfeng.yang
 * @since 2017/7/6
 */
@JsonDeserialize
public class ConsumerGroup {
    private final String subject;
    private final String group;

    @JsonCreator
    public ConsumerGroup(@JsonProperty("subject") String subject,
                         @JsonProperty("group") String group) {
        this.subject = subject;
        this.group = group;
    }

    public String getSubject() {
        return subject;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerGroup that = (ConsumerGroup) o;

        if (subject != null ? !subject.equals(that.subject) : that.subject != null) return false;
        return group != null ? group.equals(that.group) : that.group == null;
    }

    @Override
    public int hashCode() {
        int result = subject != null ? subject.hashCode() : 0;
        result = 31 * result + (group != null ? group.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerGroup{" +
                "subject='" + subject + '\'' +
                ", group='" + group + '\'' +
                '}';
    }
}
