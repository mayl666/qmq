package qunar.tc.qmq.broker;

import qunar.tc.qmq.common.ClientType;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public interface BrokerService {

    BrokerClusterInfo getClusterBySubject(ClientType clientType, String subject);

    void refresh(ClientType clientType, String subject);
}
