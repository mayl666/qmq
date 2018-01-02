package qunar.tc.qmq.meta.model;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/8/31
 */
public class SubjectRoute {
    private String subject;
    private List<String> brokerGroups;
    private long updateTime;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public List<String> getBrokerGroups() {
        return brokerGroups;
    }

    public void setBrokerGroups(List<String> brokerGroups) {
        this.brokerGroups = brokerGroups;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }
}
