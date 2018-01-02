package qunar.tc.qmq.store;

import qunar.metrics.Gauge;
import qunar.metrics.Metrics;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 整合 error log 和 consumer log ，提供统一的读取消息类
 * 通过 (subject, consumer group) 唯一定位一个 consume queue
 * 这里需要一个 filter 链处理 error message topic 之类的
 *
 * @author keli.wang
 * @since 2017/7/31
 */
public class ConsumeQueue {
    private final MessageStore store;
    private final String subject;
    private final AtomicLong nextSequence;

    public ConsumeQueue(final MessageStore store, final String subject, final String group, final long lastMaxSequence) {
        this.store = store;
        this.subject = subject;
        this.nextSequence = new AtomicLong(lastMaxSequence + 1);

        Metrics.gauge("messageSequenceLag")
                .tag("subject", subject)
                .tag("group", group)
                .call(() -> store.getMaxMessageSequence(subject) - nextSequence.get());
    }

    // TODO(keli.wang): 暂时获取过程中直接锁住。后面看是否能通过预先分配的方式避免大范围的锁
    public synchronized GetMessageResult pollMessages(final int maxMessages) {
        if (RetrySubjectUtils.isRetrySubject(subject)) {
            final long untilTimestamp = System.currentTimeMillis() - store.getStoreConfig().getRetryDelaySeconds();
            final GetMessageResult result = store.getMessageWithTimeFilter(subject, nextSequence.get(), untilTimestamp, maxMessages);
            nextSequence.set(result.getNextBeginOffset());
            return result;
        } else {
            final GetMessageResult result = store.getMessage(subject, nextSequence.get(), maxMessages);
            nextSequence.set(result.getNextBeginOffset());
            return result;
        }
    }
}
