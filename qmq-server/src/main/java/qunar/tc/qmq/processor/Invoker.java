package qunar.tc.qmq.processor;

import qunar.tc.qmq.model.ReceivingMessage;

/**
 * @author yunfeng.yang
 * @since 2017/8/7
 */
public interface Invoker {
    void invoke(ReceivingMessage message);
}
