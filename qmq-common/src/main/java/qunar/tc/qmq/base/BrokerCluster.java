package qunar.tc.qmq.base;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/8/28
 */
public class BrokerCluster {
    private List<BrokerGroup> brokerGroups;

    public BrokerCluster() {
    }

    public BrokerCluster(List<BrokerGroup> brokerGroups) {
        this.brokerGroups = brokerGroups;
    }

    public List<BrokerGroup> getBrokerGroups() {
        return brokerGroups;
    }

    public void setBrokerGroups(List<BrokerGroup> brokerGroups) {
        this.brokerGroups = brokerGroups;
    }

}
