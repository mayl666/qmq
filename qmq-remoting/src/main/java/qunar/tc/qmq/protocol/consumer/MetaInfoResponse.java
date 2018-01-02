package qunar.tc.qmq.protocol.consumer;

import qunar.tc.qmq.base.BrokerCluster;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfoResponse {
    private String subject;
    private int clientTypeCode;
    private BrokerCluster brokerCluster;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getClientTypeCode() {
        return clientTypeCode;
    }

    public void setClientTypeCode(int clientTypeCode) {
        this.clientTypeCode = clientTypeCode;
    }

    public BrokerCluster getBrokerCluster() {
        return brokerCluster;
    }

    public void setBrokerCluster(BrokerCluster brokerCluster) {
        this.brokerCluster = brokerCluster;
    }
}
