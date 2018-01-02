package qunar.tc.qmq.base;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/8/28
 */
public class BrokerGroup {
    private String groupName;
    private String master;
    private List<String> slaves;
    private long updateTime;
    private BrokerState brokerState;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public BrokerState getBrokerState() {
        return brokerState;
    }

    public void setBrokerState(BrokerState brokerState) {
        this.brokerState = brokerState;
    }

    public List<String> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<String> slaves) {
        this.slaves = slaves;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "BrokerGroup{" +
                "groupName='" + groupName + '\'' +
                ", master='" + master + '\'' +
                ", slaves=" + slaves +
                ", updateTime=" + updateTime +
                ", brokerState=" + brokerState +
                '}';
    }
}
