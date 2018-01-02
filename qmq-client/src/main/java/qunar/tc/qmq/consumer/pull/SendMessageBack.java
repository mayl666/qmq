package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.List;

/**
 * @author yiqun.fan create on 17-8-23.
 */
public interface SendMessageBack {
    void sendBack(BrokerGroupInfo brokerGroup, BaseMessage messages, Callback callback);

    interface Callback {
        void success();

        void fail(Throwable e);
    }
}
