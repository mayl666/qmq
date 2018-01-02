package qunar.tc.qmq.store;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.RawMessage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author keli.wang
 * @since 2017/7/4
 */
public class DefaultMessageStore implements MessageStore {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultMessageStore.class);

    private static final int DEFAULT_JOIN_TIME = 1000 * 60; // ms
    private static final int DEFAULT_FLUSH_INTERVAL = 500; // ms

    private final MessageStoreConfig config;

    private final MessageLog messageLog;
    private final ConsumerLogManager consumerLogManager;
    private final PullLogManager pullLogManager;
    private final ActionLog actionLog;

    private final ConsumeQueueManager consumeQueueManager;

    private final ActionLogDispatchService actionLogDispatchService;
    private final ActionLogIterateService actionLogIterateService;
    private final MessageLogIterateService messageLogIterateService;

    private final EventBus messageEventBus;
    private final ScheduledExecutorService logCleanerExecutor;

    private final PeriodicFlushService messageLogFlushService;
    private final PeriodicFlushService consumerLogFlushService;
    private final PeriodicFlushService pullLogFlushService;
    private final PeriodicFlushService actionLogFlushService;

    public DefaultMessageStore(final MessageStoreConfig config) {
        this.config = config;
        this.consumerLogManager = new ConsumerLogManager(config);
        this.messageLog = new MessageLog(config, consumerLogManager);
        this.pullLogManager = new PullLogManager(config);
        this.actionLog = new ActionLog(config);

        this.actionLogDispatchService = new ActionLogDispatchService(config, this);
        this.actionLogIterateService = new ActionLogIterateService(actionLog, actionLog.getMaxOffset(), actionLogDispatchService);

        this.consumeQueueManager = new ConsumeQueueManager(this);

        this.messageEventBus = new EventBus("message-event-bus");
        this.messageEventBus.register(new BuildConsumerLogEventListener());

        this.messageLogIterateService = new MessageLogIterateService(messageLog, messageLog.getMaxOffset(), messageEventBus);

        this.logCleanerExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("log-cleaner-%d").build());

        this.messageLogFlushService = new PeriodicFlushService(new MessageLogFlushProvider());
        this.consumerLogFlushService = new PeriodicFlushService(new ConsumerLogFlushProvider());
        this.pullLogFlushService = new PeriodicFlushService(new PullLogFlushProvider());
        this.actionLogFlushService = new PeriodicFlushService(new ActionLogFlushProvider());
    }

    @Override
    public void start() {
        messageLogFlushService.start();
        consumerLogFlushService.start();
        pullLogFlushService.start();
        actionLogFlushService.start();
        actionLogIterateService.start();
        messageLogIterateService.start();

        logCleanerExecutor.scheduleAtFixedRate(
                new LogCleaner(), 0, config.getLogRetentionCheckIntervalSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public MessageStoreConfig getStoreConfig() {
        return config;
    }

    @Override
    public void destroy() {
        messageLogFlushService.shutdown();
        consumerLogFlushService.shutdown();
        pullLogFlushService.shutdown();
        actionLogFlushService.shutdown();
        actionLogIterateService.shutdown();
        messageLogIterateService.shutdown();

        messageLog.close();
        consumerLogManager.shutdown();
        pullLogManager.shutdown();
    }

    public MessageLogVisitor newMessageLogVisitor() {
        return messageLog.newLogVisitor();
    }

    @Override
    public synchronized PutMessageResult putMessage(RawMessage message) {
        return messageLog.putMessage(message);
    }

    @Override
    public GetMessageResult getMessage(String subject, long offset, int maxMessages) {
        final GetMessageResult getMessageResult = new GetMessageResult();

        if (maxMessages <= 0) {
            getMessageResult.setNextBeginOffset(offset);
            getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE);
            return getMessageResult;
        }

        final ConsumerLog consumerLog = consumerLogManager.getConsumerLog(subject);
        if (consumerLog == null) {
            getMessageResult.setNextBeginOffset(0);
            getMessageResult.setStatus(GetMessageStatus.SUBJECT_NOT_FOUND);
            return getMessageResult;
        }

        final long minOffset = consumerLog.getMinOffset();
        final long maxOffset = consumerLog.getMaxOffset();
        getMessageResult.setMinOffset(minOffset);
        getMessageResult.setMaxOffset(maxOffset);

        if (maxOffset == 0) {
            getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE);
            getMessageResult.setNextBeginOffset(maxOffset);
            return getMessageResult;
        }
        if (offset < 0) {
            getMessageResult.setStatus(GetMessageStatus.SUCCESS);
            getMessageResult.setConsumerLogRange(new OffsetRange(maxOffset, maxOffset));
            getMessageResult.setNextBeginOffset(maxOffset);
            return getMessageResult;
        }
        if (offset > maxOffset) {
            getMessageResult.setStatus(GetMessageStatus.OFFSET_OVERFLOW);
            getMessageResult.setNextBeginOffset(maxOffset);
            return getMessageResult;
        }

        final long startOffset = offset < minOffset ? minOffset : offset;
        final SelectSegmentBufferResult consumerLogBuffer = consumerLog.selectIndexBuffer(startOffset);
        if (consumerLogBuffer == null) {
            getMessageResult.setNextBeginOffset(consumerLog.getMaxOffset());
            getMessageResult.setStatus(GetMessageStatus.EMPTY_CONSUMER_LOG);
            return getMessageResult;
        } else {
            // TODO(keli.wang): move read message into consumer log
            long nextBeginOffset = startOffset;
            final int maxMessagesSize = maxMessages * ConsumerLog.CONSUMER_LOG_UNIT_BYTES;
            for (int i = 0; i < maxMessagesSize; i += ConsumerLog.CONSUMER_LOG_UNIT_BYTES) {
                if (i >= consumerLogBuffer.getSize()) {
                    break;
                }

                final int magic = consumerLogBuffer.getBuffer().getInt();
                final long timestamp = consumerLogBuffer.getBuffer().getLong();
                final long wroteOffset = consumerLogBuffer.getBuffer().getLong();
                final int wroteBytes = consumerLogBuffer.getBuffer().getInt();
                final long messageOffset = consumerLogBuffer.getBuffer().getLong();

                // TODO(keli.wang): do some check

                // TODO(keli.wang): result == null need error log and maybe other operation
                final SelectSegmentBufferResult result = messageLog.getMessage(wroteOffset, wroteBytes, messageOffset);
                if (result != null) {
                    getMessageResult.addSegmentBuffer(result);
                }
                nextBeginOffset += 1;
            }
            getMessageResult.setStatus(GetMessageStatus.SUCCESS);
            getMessageResult.setNextBeginOffset(nextBeginOffset);
            getMessageResult.setConsumerLogRange(new OffsetRange(startOffset, nextBeginOffset - 1));
            return getMessageResult;
        }
    }

    @Override
    public GetMessageResult getMessageWithTimeFilter(String subject, long offset, long untilTimestamp, int maxMessages) {
        final GetMessageResult getMessageResult = new GetMessageResult();

        if (maxMessages <= 0) {
            getMessageResult.setNextBeginOffset(offset);
            getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE);
            return getMessageResult;
        }

        final ConsumerLog consumerLog = consumerLogManager.getConsumerLog(subject);
        if (consumerLog == null) {
            getMessageResult.setNextBeginOffset(0);
            getMessageResult.setStatus(GetMessageStatus.SUBJECT_NOT_FOUND);
            return getMessageResult;
        }

        final long minOffset = consumerLog.getMinOffset();
        final long maxOffset = consumerLog.getMaxOffset();
        getMessageResult.setMinOffset(minOffset);
        getMessageResult.setMaxOffset(maxOffset);

        if (maxOffset == 0) {
            getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE);
            getMessageResult.setNextBeginOffset(maxOffset);
            return getMessageResult;
        }
        if (offset < 0) {
            getMessageResult.setStatus(GetMessageStatus.SUCCESS);
            getMessageResult.setConsumerLogRange(new OffsetRange(maxOffset, maxOffset));
            getMessageResult.setNextBeginOffset(maxOffset);
            return getMessageResult;
        }
        if (offset > maxOffset) {
            getMessageResult.setStatus(GetMessageStatus.OFFSET_OVERFLOW);
            getMessageResult.setNextBeginOffset(maxOffset);
            return getMessageResult;
        }

        final long startOffset = offset < minOffset ? minOffset : offset;
        final SelectSegmentBufferResult consumerLogBuffer = consumerLog.selectIndexBuffer(startOffset);
        if (consumerLogBuffer == null) {
            getMessageResult.setNextBeginOffset(consumerLog.getMaxOffset());
            getMessageResult.setStatus(GetMessageStatus.EMPTY_CONSUMER_LOG);
            return getMessageResult;
        } else {
            // TODO(keli.wang): move read message into consumer log
            long nextBeginOffset = startOffset;
            final int maxMessagesSize = maxMessages * ConsumerLog.CONSUMER_LOG_UNIT_BYTES;
            for (int i = 0; i < maxMessagesSize; i += ConsumerLog.CONSUMER_LOG_UNIT_BYTES) {
                if (i >= consumerLogBuffer.getSize()) {
                    break;
                }

                final int magic = consumerLogBuffer.getBuffer().getInt();
                final long timestamp = consumerLogBuffer.getBuffer().getLong();
                if (timestamp >= untilTimestamp) {
                    break;
                }

                final long wroteOffset = consumerLogBuffer.getBuffer().getLong();
                final int wroteBytes = consumerLogBuffer.getBuffer().getInt();
                final long messageOffset = consumerLogBuffer.getBuffer().getLong();

                // TODO(keli.wang): do some check

                // TODO(keli.wang): result == null need error log and maybe other operation
                final SelectSegmentBufferResult result = messageLog.getMessage(wroteOffset, wroteBytes, messageOffset);
                if (result != null) {
                    getMessageResult.addSegmentBuffer(result);
                }
                nextBeginOffset += 1;
            }

            if (startOffset == nextBeginOffset) {
                getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE);
            } else {
                getMessageResult.setStatus(GetMessageStatus.SUCCESS);
                getMessageResult.setConsumerLogRange(new OffsetRange(startOffset, nextBeginOffset - 1));
            }
            getMessageResult.setNextBeginOffset(nextBeginOffset);
            return getMessageResult;
        }
    }

    @Override
    public long getMaxMessageOffset() {
        return messageLog.getMaxOffset();
    }

    @Override
    public long getMaxActionLogOffset() {
        return actionLog.getMaxOffset();
    }

    @Override
    public long getMaxMessageSequence(String subject) {
        final ConsumerLog consumerLog = consumerLogManager.getConsumerLog(subject);
        if (consumerLog == null) {
            return 0;
        } else {
            return consumerLog.getMaxOffset();
        }
    }

    @Override
    public PutMessageResult putAction(Action action) {
        return actionLog.addAction(action);
    }

    @Override
    public List<PutMessageResult> putPullLogs(String subject, String group, String consumerId, List<PullLogMessage> messages) {
        final PullLog pullLog = pullLogManager.getOrCreate(subject, group, consumerId);
        return pullLog.putPullLogMessages(messages);
    }

    @Override
    public long getMaxPullLogSequence(final String subject, final String group, final String consumerId) {
        final PullLog pullLog = pullLogManager.getOrCreate(subject, group, consumerId);
        return pullLog.getMaxOffset();
    }

    @Override
    public List<MaxAckedPullLogSequence> allMaxAckedPullLogSequences() {
        return actionLogDispatchService.getOffsetManager().getMaxAckedPullLogSequences();
    }

    @Override
    public List<MaxPulledMessageSequence> allMaxPulledMessageSequences() {
        return actionLogDispatchService.getOffsetManager().getMaxPulledMessageSequences();
    }

    @Override
    public long getMessageSequenceByPullLog(String subject, String group, String consumerId, long pullLogSequence) {
        final PullLog log = pullLogManager.get(subject, group, consumerId);
        if (log == null) {
            return -1;
        }

        return log.getMessageSequence(pullLogSequence);
    }

    @Override
    public ConsumeQueue locateConsumeQueue(String subject, String group) {
        return consumeQueueManager.getOrCreate(subject, group);
    }

    @Override
    public void registerEventListener(Object listener) {
        messageEventBus.register(listener);
    }

    @Override
    public SelectSegmentBufferResult getMessageLogData(long offset) {
        return messageLog.getMessageData(offset);
    }

    @Override
    public SelectSegmentBufferResult getActionLogData(long offset) {
        return actionLog.getMessageData(offset);
    }

    @Override
    public boolean appendMessageLogData(long startOffset, ByteBuffer data) {
        return messageLog.appendData(startOffset, data);
    }

    @Override
    public boolean appendActionLogData(long startOffset, ByteBuffer data) {
        return actionLog.appendData(startOffset, data);
    }

    private class BuildConsumerLogEventListener {
        @Subscribe
        public void buildConsumerLog(final MessageLogMeta event) {
            final ConsumerLog consumerLog = consumerLogManager.getOrCreateConsumerLog(event.getSubject());
            if (consumerLog.getMaxOffset() != event.getSequence()) {
                LOG.error("next sequence not equals to max sequence. diff: {}", event.getSequence() - consumerLog.getMaxOffset());
            }
            final boolean success = consumerLog.putMessageLogOffset(event.getSequence(), event.getWroteOffset(), event.getWroteBytes(), event.getPayloadOffset());

            messageEventBus.post(new ConsumerLogWroteEvent(event.getSubject(), success));
        }

        @Subscribe
        public void buildConsumerLog(final MessageLogWroteEvent event) {
            final ConsumerLog consumerLog = consumerLogManager.getOrCreateConsumerLog(event.getSubject());
            final AppendMessageResult<MessageSequence> result = event.getResult();
            final MessageSequence additional = result.getAdditional();
            final long sequence = additional.getSequence();
            if (consumerLog.getMaxOffset() != sequence) {
                LOG.error("WRONG!!!!!");
            }
            final boolean success = consumerLog.putMessageLogOffset(additional.getSequence(), result.getWroteOffset(), result.getWroteBytes(), additional.getPhysicalOffset());

            messageEventBus.post(new ConsumerLogWroteEvent(event.getSubject(), success));
        }
    }

    private class MessageLogFlushProvider implements PeriodicFlushService.FlushProvider {

        @Override
        public int getJoinTime() {
            return DEFAULT_JOIN_TIME;
        }

        @Override
        public int getInterval() {
            return DEFAULT_FLUSH_INTERVAL;
        }

        @Override
        public void flush() {
            messageLog.flush();
        }
    }

    private class ConsumerLogFlushProvider implements PeriodicFlushService.FlushProvider {

        @Override
        public int getJoinTime() {
            return DEFAULT_JOIN_TIME;
        }

        @Override
        public int getInterval() {
            return DEFAULT_FLUSH_INTERVAL;
        }

        @Override
        public void flush() {
            consumerLogManager.flush();
        }
    }

    private class PullLogFlushProvider implements PeriodicFlushService.FlushProvider {

        @Override
        public int getJoinTime() {
            return DEFAULT_JOIN_TIME;
        }

        @Override
        public int getInterval() {
            return DEFAULT_FLUSH_INTERVAL;
        }

        @Override
        public void flush() {
            pullLogManager.flush();
        }
    }

    private class LogCleaner implements Runnable {

        @Override
        public void run() {
            try {
                messageLog.clean();
                consumerLogManager.clean();
                pullLogManager.clean();
                actionLog.clean();
            } catch (Throwable e) {
                LOG.error("log cleaner caught exception.", e);
            }
        }
    }

    private class ActionLogFlushProvider implements PeriodicFlushService.FlushProvider {
        @Override
        public int getJoinTime() {
            return DEFAULT_JOIN_TIME;
        }

        @Override
        public int getInterval() {
            return DEFAULT_FLUSH_INTERVAL;
        }

        @Override
        public void flush() {
            actionLog.flush();
        }
    }
}
