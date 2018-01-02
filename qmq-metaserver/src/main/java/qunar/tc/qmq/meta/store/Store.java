package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.base.BrokerGroup;
import qunar.tc.qmq.base.BrokerState;
import qunar.tc.qmq.meta.model.SubjectRoute;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public interface Store {

    int insertSubjectRoute(String subject, List<String> groupNames);

    SubjectRoute selectSubjectRoute(String subject);

    void insertOrUpdateBrokerGroup(String groupName, String masterInfo, BrokerState brokerState);

    void updateBrokerGroup(String groupName, BrokerState brokerState);

    List<BrokerGroup> getAllBrokerGroups();

    List<SubjectRoute> getAllSubjectRoutes();

    BrokerGroup getBrokerGroup(String groupName);
}
