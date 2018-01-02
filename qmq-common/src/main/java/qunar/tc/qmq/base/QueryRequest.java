package qunar.tc.qmq.base;

import java.io.Serializable;

/**
 * User: zhaohuiyu
 * Date: 4/8/13
 * Time: 3:09 PM
 */
public class QueryRequest implements Serializable {
    private static final long serialVersionUID = 8251740494297410958L;

    private String prefix;
    private String group;
    private String messageId;

    public QueryRequest() {
    }

    public QueryRequest(String prefix, String group, String messageId) {
        this.prefix = prefix;
        this.group = group;
        this.messageId = messageId;
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

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryRequest that = (QueryRequest) o;

        if (group != null ? !group.equals(that.group) : that.group != null) return false;
        if (messageId != null ? !messageId.equals(that.messageId) : that.messageId != null) return false;
        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + (group != null ? group.hashCode() : 0);
        result = 31 * result + (messageId != null ? messageId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "QueryRequest{" +
                "prefix='" + prefix + '\'' +
                ", group='" + group + '\'' +
                ", messageId='" + messageId + '\'' +
                '}';
    }
}
