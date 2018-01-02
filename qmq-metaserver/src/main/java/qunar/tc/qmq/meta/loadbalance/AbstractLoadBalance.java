package qunar.tc.qmq.meta.loadbalance;

import java.util.List;

/**
 * User: zhaohuiyu Date: 1/9/13 Time: 10:35 AM
 */
public abstract class AbstractLoadBalance implements LoadBalance {
    @Override
    public List<String> select(String subject, List<String> brokerGroups, int minNum) {
        if (brokerGroups == null || brokerGroups.size() == 0) {
            return null;
        }
        if (brokerGroups.size() <= minNum) {
            return brokerGroups;
        }
        return doSelect(subject, brokerGroups, minNum);
    }

    abstract List<String> doSelect(String subject, List<String> brokerGroups, int minNum);
}
