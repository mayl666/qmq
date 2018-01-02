package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.common.ClientType;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfo {
    private final String subject;
    private final ClientType clientType;
    private final BrokerClusterInfo clusterInfo;

    MetaInfo(String subject, ClientType clientType, BrokerClusterInfo clusterInfo) {
        this.subject = subject;
        this.clientType = clientType;
        this.clusterInfo = clusterInfo;
    }

    public String getSubject() {
        return subject;
    }

    public ClientType getClientType() {
        return clientType;
    }

    public BrokerClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    @Override
    public String toString() {
        return "MetaInfo{" +
                "subject='" + subject + '\'' +
                ", clientType=" + clientType +
                ", groups=" + clusterInfo.getGroups() +
                '}';
    }
}
