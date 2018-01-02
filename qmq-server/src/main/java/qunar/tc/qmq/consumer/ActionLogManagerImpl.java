package qunar.tc.qmq.consumer;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.model.ActionLogOffset;
import qunar.tc.qmq.model.ConsumerGroup;
import qunar.tc.qmq.model.ConsumerPoint;
import qunar.tc.qmq.model.PullExtraParam;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.processor.AckMessageProcessor;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yunfeng.yang
 * @since 2017/8/1
 */
public class ActionLogManagerImpl implements ActionLogManager {
    private static final Logger logger = LoggerFactory.getLogger(ActionLogManagerImpl.class);
    private static final long ACTION_LOG_ORIGIN_OFFSET = -1L;

    private final MessageStoreWrapper messageStoreWrapper;
    private final ConcurrentMap<String, ConcurrentMap<ConsumerGroup, ActionLogOffset>> actionLogOffsets;

    public ActionLogManagerImpl(final MessageStoreWrapper messageStoreWrapper) {
        this.messageStoreWrapper = messageStoreWrapper;
        this.actionLogOffsets = new ConcurrentHashMap<>();
        loadData();
    }

    private void loadData() {
        ConcurrentMap<String, ConcurrentMap<ConsumerGroup, ActionLogOffset>> snapshot = messageStoreWrapper.loadPullAndAckLogs();
        if (snapshot == null || snapshot.size() == 0) {
            return;
        }
        actionLogOffsets.putAll(snapshot);
    }

    @Override
    public PullExtraParam putPullActions(final boolean isBroadcast, final ConsumerPoint consumerPoint, final GetMessageResult getMessageResult) {
        final String subject = consumerPoint.getSubject();
        final String group = consumerPoint.getGroup();
        final String consumerId = consumerPoint.getConsumerId();
        final OffsetRange consumerLogRange = getMessageResult.getConsumerLogRange();
        final ActionLogOffset actionLogOffset = getOrCreateActionLogOffset(consumerPoint);

        actionLogOffset.pullLock();
        final long firstConsumerLogOffset = consumerLogRange.getBegin();
        final long lastConsumerLogOffset = consumerLogRange.getEnd();
        final long firstPullLogOffset = actionLogOffset.getPullLogOffset() + 1;
        final long lastPullLogOffset = actionLogOffset.getPullLogOffset() + getMessageResult.getMessageNum();
        try {
            final Action action = new PullAction(subject, group, consumerId, isBroadcast, firstPullLogOffset, lastPullLogOffset,
                    firstConsumerLogOffset, lastConsumerLogOffset);
            final PutMessageResult putMessageResult = messageStoreWrapper.putAction(action);
            if (putMessageResult.getStatus() != PutMessageStatus.SUCCESS) {
                logger.error("put action fail, consumerPoint:{}", consumerPoint);
                throw new RuntimeException("put pull action fail");
            }
            actionLogOffset.setPullLogOffset(lastPullLogOffset);
        } finally {
            actionLogOffset.pullUnlock();
        }
        return new PullExtraParam(firstPullLogOffset, firstConsumerLogOffset);
    }

    @Override
    public void putAckActions(AckMessageProcessor.AckEntry ackEntry) {
        final String consumerId = ackEntry.getConsumerId();
        final String subject = ackEntry.getSubject();
        final String group = ackEntry.getGroup();
        final long lastPullOffset = ackEntry.getLastPullLogOffset();
        long firstPullOffset = ackEntry.getFirstPullLogOffset();

        final ConsumerGroup consumerGroup = new ConsumerGroup(subject, group);
        final ConsumerPoint consumerPoint = new ConsumerPoint(consumerId, consumerGroup);
        final ActionLogOffset actionLogOffset = getOrCreateActionLogOffset(consumerPoint);

        actionLogOffset.ackSemaphoreAcquire();
        final long confirmedAckLogOffset = actionLogOffset.getAckLogOffset();
        try {
            if (lastPullOffset <= confirmedAckLogOffset) {
                logger.warn("receive duplicate ack, consumer: [{}]-[{}]-[{}]", subject, group, consumerId);
                return;
            }
            final long lostAckCount = firstPullOffset - confirmedAckLogOffset;
            if (lostAckCount <= 0) {
                logger.warn("receive duplicate ack, consumer: [{}]-[{}]-[{}]", subject, group, consumerId);
                firstPullOffset = confirmedAckLogOffset + 1;
            } else if (lostAckCount > 1) {
                logger.error("lost ack count, consumer: [{}]-[{}]-[{}], beginOffset: {}, count: {}",
                        subject, group, consumerId, confirmedAckLogOffset, lostAckCount);
                final long firstLostPullOffset = confirmedAckLogOffset + 1;
                final long lastLostPullLOffset = firstPullOffset - 1;
                messageStoreWrapper.putErrorMessageForPullLogOffsetRange(consumerPoint, firstLostPullOffset, lastLostPullLOffset);
                QMon.consumerLostAckCountInc(subject, group, (int) lostAckCount);
            }
            final Action rangeAckAction = new RangeAckAction(subject, group, consumerId, firstPullOffset, lastPullOffset);
            final PutMessageResult putMessageResult = messageStoreWrapper.putAction(rangeAckAction);
            if (putMessageResult.getStatus() != PutMessageStatus.SUCCESS) {
                logger.error("put ack action fail, consumerPoint:{}", consumerPoint);
                throw new RuntimeException("put ack action fail");
            }
            actionLogOffset.setAckLogOffset(lastPullOffset);
        } finally {
            actionLogOffset.ackSemaphoreRelease();
        }
    }

    @Override
    public ActionLogOffset getActionLogOffset(ConsumerPoint consumerPoint) {
        if (consumerPoint == null) {
            return null;
        }
        final ConcurrentMap<ConsumerGroup, ActionLogOffset> actionLogs = this.actionLogOffsets.get(consumerPoint.getConsumerId());
        if (actionLogs == null) {
            return null;
        }
        return actionLogs.get(consumerPoint.getConsumerGroup());
    }

    private ActionLogOffset getOrCreateActionLogOffset(ConsumerPoint consumerPoint) {
        ConcurrentMap<ConsumerGroup, ActionLogOffset> actionLogs = this.actionLogOffsets.get(consumerPoint.getConsumerId());
        if (actionLogs == null) {
            final ConcurrentMap<ConsumerGroup, ActionLogOffset> newPullLogs = new ConcurrentHashMap<>();
            actionLogs = ObjectUtils.defaultIfNull(actionLogOffsets.putIfAbsent(consumerPoint.getConsumerId(), newPullLogs), newPullLogs);
        }

        ActionLogOffset actionLogOffset = actionLogs.get(consumerPoint.getConsumerGroup());
        if (actionLogOffset == null) {
            final ActionLogOffset newActionLogOffset = new ActionLogOffset(ACTION_LOG_ORIGIN_OFFSET, ACTION_LOG_ORIGIN_OFFSET);
            actionLogOffset = ObjectUtils.defaultIfNull(actionLogs.putIfAbsent(consumerPoint.getConsumerGroup(), newActionLogOffset), newActionLogOffset);
        }
        return actionLogOffset;
    }
}
