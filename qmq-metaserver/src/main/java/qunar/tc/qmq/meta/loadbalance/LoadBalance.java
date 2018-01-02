package qunar.tc.qmq.meta.loadbalance;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public interface LoadBalance {
    List<String> select(String subject, List<String> brokerGroups, int minNum);
}
