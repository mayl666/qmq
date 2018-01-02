package qunar.tc.qmq.consumer;

import qunar.tc.qmq.model.ActionLogOffset;
import qunar.tc.qmq.model.ConsumerPoint;
import qunar.tc.qmq.model.PullExtraParam;
import qunar.tc.qmq.processor.AckMessageProcessor;
import qunar.tc.qmq.store.GetMessageResult;

/**
 * @author yunfeng.yang
 * @since 2017/8/2
 */
public interface ActionLogManager {
    PullExtraParam putPullActions(boolean isBroadcast, ConsumerPoint consumerPoint, GetMessageResult messages);

    void putAckActions(AckMessageProcessor.AckEntry ackEntry);

    ActionLogOffset getActionLogOffset(ConsumerPoint consumerPoint);
}
