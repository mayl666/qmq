package qunar.tc.qmq.store;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.model.*;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.store.action.RangeAckAction;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yunfeng.yang
 * @since 2017/7/10
 */
public class MessageStoreWrapperImpl implements MessageStoreWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(MessageStoreWrapperImpl.class);

    private final MessageStore messageStore;

    public MessageStoreWrapperImpl(final MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    @Override
    public List<ReceiveResult> putMessages(List<ReceivingMessage> messages) {
        final List<ReceiveResult> results = new ArrayList<>(messages.size());
        for (ReceivingMessage message : messages) {
            final MessageHeader header = message.getMessage().getHeader();
            final String msgId = header.getMessageId();
            final PutMessageResult putMessageResult = putRawMessage(message.getMessage());
            final PutMessageStatus status = putMessageResult.getStatus();
            if (status != PutMessageStatus.SUCCESS) {
                LOG.error("put message error, message:[{}]-[{}], status:[{}]", header.getSubject(), msgId, status.name());
                QMon.storeMessageErrorCountInc(header.getSubject());
                results.add(new ReceiveResult(msgId, MessageProducerCode.STORE_ERROR, status.name(), -1));
            } else {
                final long messageLogOffset = putMessageResult.getResult().getAdditional().getPhysicalOffset();
                results.add(new ReceiveResult(msgId, MessageProducerCode.SUCCESS, "", messageLogOffset));
            }
        }
        return results;
    }

    @Override
    public PutMessageResult putRawMessage(RawMessage rawMessage) {
        final long start = System.currentTimeMillis();

        final MessageHeader header = rawMessage.getHeader();
        try {
            return messageStore.putMessage(rawMessage);
        } catch (Throwable e) {
            LOG.error("put message error, message:[{}]-[{}]", header.getSubject(), header.getMessageId(), e);
            QMon.storeMessageErrorCountInc(header.getSubject());
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        } finally {
            QMon.putMessageTime(header.getSubject(), System.currentTimeMillis() - start);
        }
    }

    @Override
    public GetMessageResult findMessages(ConsumerGroup consumerGroup, boolean isBroadcast, int expectedNum) {
        try {
            return messageStore.locateConsumeQueue(consumerGroup.getSubject(), consumerGroup.getGroup()).pollMessages(expectedNum);
        } catch (Exception e) {
            QMon.getMessageErrorCountInc(consumerGroup.getSubject(), consumerGroup.getGroup());
            LOG.error("poll messages failed. subject {}, group: {}", consumerGroup.getSubject(), consumerGroup.getGroup(), e);
        }
        return new GetMessageResult();
    }

    @Override
    public void appendActionLogs(long startOffset, ByteBuf actionLog) {
        if (!messageStore.appendActionLogData(startOffset, actionLog.nioBuffer())) {
            LOG.error("append action log data error {}", startOffset);
        }
    }

    @Override
    public void appendMessageLogs(long startOffset, ByteBuf messageLogs) {
        if (!messageStore.appendMessageLogData(startOffset, messageLogs.nioBuffer())) {
            LOG.error("append message log data error {}", startOffset);
        }
    }

    @Override
    public SelectSegmentBufferResult getActionLogs(long currentOffset) {
        return messageStore.getActionLogData(currentOffset);
    }

    @Override
    public SelectSegmentBufferResult getMessageLogs(long currentOffset) {
        return messageStore.getMessageLogData(currentOffset);
    }

    @Override
    public long getActionLogMaxOffset() {
        return messageStore.getMaxActionLogOffset();
    }

    @Override
    public long getMessageLogMaxOffset() {
        return messageStore.getMaxMessageOffset();
    }

    @Override
    public ConcurrentMap<String, ConcurrentMap<ConsumerGroup, ActionLogOffset>> loadPullAndAckLogs() {
        final ConcurrentMap<String, ConcurrentMap<ConsumerGroup, ActionLogOffset>> result = new ConcurrentHashMap<>();
        final List<MaxAckedPullLogSequence> ackOffsets = messageStore.allMaxAckedPullLogSequences();
        for (final MaxAckedPullLogSequence ackOffset : ackOffsets) {
            final String subject = ackOffset.getSubject();
            final String group = ackOffset.getGroup();
            final String consumerId = ackOffset.getConsumerId();
            final ConsumerGroup consumerGroup = new ConsumerGroup(subject, group);
            final long maxAckLogSequence = ackOffset.getMaxSequence();
            final long maxPullLogSequence = messageStore.getMaxPullLogSequence(subject, group, consumerId) - 1;

            ConcurrentMap<ConsumerGroup, ActionLogOffset> actionLogs = result.get(consumerId);
            if (actionLogs == null) {
                actionLogs = new ConcurrentHashMap<>();
                result.putIfAbsent(consumerId, actionLogs);
            }

            final ActionLogOffset actionLogOffset = new ActionLogOffset(maxPullLogSequence, maxAckLogSequence);
            actionLogs.putIfAbsent(consumerGroup, actionLogOffset);
        }
        return result;
    }

    @Override
    public PutMessageResult putAction(Action action) {
        return messageStore.putAction(action);
    }

    @Override
    public void putErrorMessageForPullLogOffsetRange(ConsumerPoint consumerPoint, long firstPullLogOffset, long lastPullLogOffset) {
        final String subject = consumerPoint.getSubject();
        final String group = consumerPoint.getGroup();
        final String consumerId = consumerPoint.getConsumerId();
        final int needRetryNum = (int) (lastPullLogOffset - firstPullLogOffset + 1);

        // get error msg
        final List<SelectSegmentBufferResult> needRetryMessages = new ArrayList<>(needRetryNum);
        for (long pullLogOffset = firstPullLogOffset; pullLogOffset <= lastPullLogOffset; pullLogOffset++) {
            final long consumerLogOffset = messageStore.getMessageSequenceByPullLog(subject, group, consumerId, pullLogOffset);
            // 每次取出一条
            final GetMessageResult getMessageResult = messageStore.getMessage(subject, consumerLogOffset, 1);
            if (getMessageResult.getStatus() != GetMessageStatus.SUCCESS) {
                LOG.error("get Message Result error when put error message");
                continue;
            }
            final List<SelectSegmentBufferResult> segmentBuffers = getMessageResult.getSegmentBuffers();
            needRetryMessages.addAll(segmentBuffers);
        }

        // put error msg
        for (SelectSegmentBufferResult buffer : needRetryMessages) {
            final ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.getBuffer());
            final RawMessage rawMessage = QMQSerializer.deserializeRawMessage(byteBuf);
            final String messageSubject = rawMessage.getHeader().getSubject();
            if (RetrySubjectUtils.isRetrySubject(messageSubject)) {
                final PutMessageResult putMessageResult = messageStore.putMessage(rawMessage);
                if (putMessageResult.getStatus() != PutMessageStatus.SUCCESS) {
                    LOG.error("put message error, status:{}", putMessageResult.getStatus());
                }
            } else {
                final String retrySubject = RetrySubjectUtils.buildRetrySubject(messageSubject, group);
                rawMessage.setSubject(retrySubject);
                final PutMessageResult putMessageResult = messageStore.putMessage(rawMessage);
                if (putMessageResult.getStatus() != PutMessageStatus.SUCCESS) {
                    LOG.error("put message error, status:{}", putMessageResult.getStatus());
                }
            }
        }

        // put ack action
        final Action action = new RangeAckAction(subject, group, consumerId,
                firstPullLogOffset, lastPullLogOffset);
        final PutMessageResult putMessageResult = messageStore.putAction(action);
        if (putMessageResult.getStatus() != PutMessageStatus.SUCCESS) {
            LOG.error("put action error, status:{}", putMessageResult.getStatus());
        }
    }
}
