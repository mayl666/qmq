package qunar.tc.qmq.store;

import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.common.Disposable;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/7/4
 */
public interface MessageStore extends Disposable {
    void start();

    MessageStoreConfig getStoreConfig();

    PutMessageResult putMessage(final RawMessage message);

    GetMessageResult getMessage(final String subject, final long offset, final int maxMessages);

    GetMessageResult getMessageWithTimeFilter(final String subject, final long offset, final long untilTimestamp, final int maxMessages);

    long getMaxMessageOffset();

    long getMaxActionLogOffset();

    long getMaxMessageSequence(final String subject);

    PutMessageResult putAction(final Action action);

    List<PutMessageResult> putPullLogs(final String subject, final String group, final String consumerId, final List<PullLogMessage> messages);

    long getMaxPullLogSequence(final String subject, final String group, final String consumerId);

    List<MaxAckedPullLogSequence> allMaxAckedPullLogSequences();

    List<MaxPulledMessageSequence> allMaxPulledMessageSequences();

    long getMessageSequenceByPullLog(final String subject, final String group, final String consumerId, final long pullLogSequence);

    ConsumeQueue locateConsumeQueue(final String subject, final String group);

    void registerEventListener(Object listener);

    SelectSegmentBufferResult getMessageLogData(final long offset);

    SelectSegmentBufferResult getActionLogData(final long offset);

    boolean appendMessageLogData(final long startOffset, final ByteBuffer data);

    boolean appendActionLogData(final long startOffset, final ByteBuffer data);
}
