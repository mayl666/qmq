package qunar.tc.qmq.store;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.model.*;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yunfeng.yang
 * @since 2017/7/10
 */
public interface MessageStoreWrapper {
    List<ReceiveResult> putMessages(List<ReceivingMessage> messages);

    PutMessageResult putRawMessage(RawMessage rawMessage);

    GetMessageResult findMessages(ConsumerGroup consumerGroup, boolean isBroadcast, int expectedNum);

    void appendActionLogs(long startOffset, ByteBuf actionLog);

    void appendMessageLogs(long startOffset, ByteBuf messageLogs);

    long getActionLogMaxOffset();

    long getMessageLogMaxOffset();

    SelectSegmentBufferResult getActionLogs(long currentOffset);

    SelectSegmentBufferResult getMessageLogs(long currentOffset);

    ConcurrentMap<String, ConcurrentMap<ConsumerGroup, ActionLogOffset>> loadPullAndAckLogs();

    PutMessageResult putAction(Action action);

    void putErrorMessageForPullLogOffsetRange(ConsumerPoint consumerPoint, long firstPullLogOffset, long lastPullLogOffset);
}
