package qunar.tc.qmq.consumer.pull.model;

import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.List;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class PullResult {
    private final short responseCode;
    private final List<BaseMessage> messages;
    private final BrokerGroupInfo brokerGroup;

    public PullResult(short responseCode, List<BaseMessage> messages, BrokerGroupInfo brokerGroup) {
        this.responseCode = responseCode;
        this.messages = messages;
        this.brokerGroup = brokerGroup;
    }

    public short getResponseCode() {
        return responseCode;
    }

    public List<BaseMessage> getMessages() {
        return messages;
    }

    public BrokerGroupInfo getBrokerGroup() {
        return brokerGroup;
    }
}
