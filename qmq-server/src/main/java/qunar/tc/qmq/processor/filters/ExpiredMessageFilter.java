package qunar.tc.qmq.processor.filters;

import qunar.tc.qmq.model.ReceivingMessage;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.processor.Invoker;

/**
 * User: zhaohuiyu Date: 4/1/13 Time: 6:59 PM
 */
class ExpiredMessageFilter implements ReceiveFilter {
    @Override
    public void invoke(Invoker invoker, ReceivingMessage message) {
        if (message.isExpired()) {
            QMon.expiredMessagesCountInc(message.getSubject());
        }
        invoker.invoke(message);
    }
}
