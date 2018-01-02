package qunar.tc.qmq.processor.filters;

import qunar.tc.qmq.model.ReceivingMessage;
import qunar.tc.qmq.processor.Invoker;

/**
 * User: zhaohuiyu Date: 4/1/13 Time: 3:10 PM
 */
public interface ReceiveFilter {
    void invoke(Invoker invoker, ReceivingMessage message);
}