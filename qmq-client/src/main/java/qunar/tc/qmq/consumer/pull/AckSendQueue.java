package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Gauge;
import qunar.metrics.Metrics;
import qunar.metrics.Timer;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerLoadBalanceImpl;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.QmqLogger;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.consumer.pull.exception.SendMessageBackException;
import qunar.tc.qmq.consumer.pull.impl.SendMessageBackImpl;
import qunar.tc.qmq.consumer.pull.model.AckEntry;
import qunar.tc.qmq.consumer.pull.model.AckSendEntry;
import qunar.tc.qmq.consumer.pull.model.AckSendInfo;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yiqun.fan create on 17-8-23.
 */
public class AckSendQueue implements TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(AckSendQueue.class);
    private static final long DEFAULT_PULL_OFFSET = 0;
    private static final int ACK_INTERVAL_SECONDS = 30;
    private static final long ACK_TRY_SEND_TIMEOUT_MILLIS = 1000;
    private static final int SEND_BACK_DELAY_SECONDS = 5;

    private final String brokerGroupName;
    private final String subject;
    private final String group;
    private final AckService ackService = AckService.getInstance();
    private final HashedWheelTimer timer;
    private final String retrySubject;
    private final AtomicReference<Integer> pullBatchSize;
    private final LinkedBlockingQueue<AckEntry> ackEntryQueue = new LinkedBlockingQueue<>();
    private final ReentrantLock updateLock = new ReentrantLock();
    private volatile AckEntry tailAckEntry = null;
    private final AtomicLong toSendPullOffsetEnd = new AtomicLong(0);
    private final AtomicLong minPullOffset = new AtomicLong(DEFAULT_PULL_OFFSET);
    private final AtomicLong maxPullOffset = new AtomicLong(DEFAULT_PULL_OFFSET);
    private final AtomicInteger toSendNum = new AtomicInteger(0);
    private final LinkedBlockingQueue<AckSendEntry> sendEntryQueue = new LinkedBlockingQueue<>();
    private final ReentrantLock sendLock = new ReentrantLock();
    private final AtomicBoolean inSending = new AtomicBoolean(false);
    private final BrokerService brokerService = BrokerServiceImpl.getInstance();
    private final BrokerLoadBalance brokerLoadBalance = BrokerLoadBalanceImpl.getInstance();
    private final SendMessageBack sendMessageBack = SendMessageBackImpl.getInstance();
    private final Timer sendNumMonitor;
    private final RateLimiter ackSendFailLogLimit = RateLimiter.create(0.5);

    AckSendQueue(String brokerGroupName, String subject, String group, HashedWheelTimer timer) {
        this.brokerGroupName = brokerGroupName;
        this.subject = subject;
        this.group = group;
        this.timer = timer;
        this.retrySubject = RetrySubjectUtils.isRetrySubject(subject) ? subject : RetrySubjectUtils.buildRetrySubject(subject, group);
        this.pullBatchSize = PullSubjectsConfig.get().getPullBatchSize(subject);
        timer.newTimeout(this, ACK_INTERVAL_SECONDS, TimeUnit.SECONDS);
        sendNumMonitor = Metrics.timer("qmq_pull_ack_sendnum").tag("subject", subject).tag("group", group).get();

        Metrics.gauge("qmq_pull_ack_min_offset").call(new Gauge() {
            @Override
            public double getValue() {
                return minPullOffset.get();
            }
        });
        Metrics.gauge("qmq_pull_ack_diff_offset").call(new Gauge() {
            @Override
            public double getValue() {
                return maxPullOffset.get() - minPullOffset.get();
            }
        });
        Metrics.gauge("qmq_pull_ack_tosendnum").call(new Gauge() {
            @Override
            public double getValue() {
                return toSendNum.get();
            }
        });
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (!trySendAck(ACK_TRY_SEND_TIMEOUT_MILLIS)) {
            final BrokerGroupInfo brokerGroup = getBrokerGroup();
            if (brokerGroup == null) {
                LOGGER.info("lost broker group: {}. subject={}, consumeGroup={}", brokerGroupName, subject, group);
                return;
            }
            ackService.sendAck(brokerGroup, subject, group, new AckSendEntry(), new AckService.SendAckCallback() {
                @Override
                public void success() {
                    QmqLogger.log("send empty Ack ok");
                }

                @Override
                public void fail(Exception ex) {
                    QmqLogger.log("send empty Ack fail: " + Strings.nullToEmpty(ex.getMessage()));
                }
            });
        }
        timer.newTimeout(this, ACK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    void appendAckEntries(final List<AckEntry> ackEntries) {
        if (ackEntries == null || ackEntries.isEmpty()) {
            return;
        }
        updateLock.lock();
        try {
            if (tailAckEntry == null) {
                toSendPullOffsetEnd.set(ackEntries.get(0).pullOffset());
                minPullOffset.set(ackEntries.get(0).pullOffset());
            } else {
                tailAckEntry.setNext(ackEntries.get(0));
            }
            tailAckEntry = ackEntries.get(ackEntries.size() - 1);
            maxPullOffset.set(tailAckEntry.pullOffset());
            toSendNum.getAndAdd(ackEntries.size());
            for (AckEntry ackEntry : ackEntries) {
                if (ackEntry != null)
                    ackEntryQueue.offer(ackEntry);
            }
        } finally {
            updateLock.unlock();
        }
    }

    public void sendbackAndCompleteNack(final int nextRetryCount, final BaseMessage message, final AckEntry ackEntry) {
        message.setSubject(retrySubject);
        message.setProperty(BaseMessage.keys.qmq_times, nextRetryCount);
        final BrokerClusterInfo borkerCluster = brokerService.getClusterBySubject(ClientType.PRODUCER, retrySubject);
        final SendMessageBack.Callback callback = new SendMessageBack.Callback() {
            private final int retryTooMuch = borkerCluster.getGroups().size() * 2;
            private final AtomicInteger retryNumOnFail = new AtomicInteger(0);

            @Override
            public void success() {
                ackEntry.completed();
            }

            @Override
            public void fail(Throwable e) {
                if (retryNumOnFail.incrementAndGet() > retryTooMuch) {
                    if (e instanceof SendMessageBackException) {
                        LOGGER.error("send message back fail, and retry {} times after {} seconds. exception: {}", retryNumOnFail.get(), SEND_BACK_DELAY_SECONDS, e.getMessage());
                    } else {
                        LOGGER.error("send message back fail, and retry {} times after {} seconds", retryNumOnFail.get(), SEND_BACK_DELAY_SECONDS, e);
                    }
                    final SendMessageBack.Callback callback1 = this;
                    timer.newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) throws Exception {
                            AckSendQueue.this.sendbackAndCompleteNack(message, callback1);
                        }
                    }, SEND_BACK_DELAY_SECONDS, TimeUnit.SECONDS);
                } else {
                    if (e instanceof SendMessageBackException) {
                        LOGGER.error("send message back fail, and retry {} times. exception: {}", retryNumOnFail.get(), SEND_BACK_DELAY_SECONDS, e.getMessage());
                    } else {
                        LOGGER.error("send message back fail, and retry {} times", retryNumOnFail.get(), SEND_BACK_DELAY_SECONDS, e);
                    }
                    AckSendQueue.this.sendbackAndCompleteNack(message, this);
                }
            }
        };
        final BrokerGroupInfo brokerGroup = brokerLoadBalance.loadBalance(borkerCluster, null);
        sendMessageBack.sendBack(brokerGroup, message, callback);
    }

    private void sendbackAndCompleteNack(final BaseMessage message, final SendMessageBack.Callback callback) {
        final BrokerClusterInfo brokerCluster = brokerService.getClusterBySubject(ClientType.PRODUCER, message.getSubject());
        final BrokerGroupInfo brokerGroup = brokerLoadBalance.loadBalance(brokerCluster, null);
        sendMessageBack.sendBack(brokerGroup, message, callback);
    }

    public void ackCompleted(AckEntry ackEntry) {
        AckSendEntry sendEntry = null;
        updateLock.lock();
        try {
            final long ackOffset = ackEntry.pullOffset();
            if (ackOffset == toSendPullOffsetEnd.get()) {
                AckEntry last = ackEntry;
                while (last.next() != null && last.next().isDone()) {
                    last = last.next();
                }
                if (last.next() == null) {
                    tailAckEntry = null;
                    toSendPullOffsetEnd.set(last.pullOffset() + 1);
                } else {
                    toSendPullOffsetEnd.set(last.next().pullOffset());
                }
                if (last.next() == null || last.pullOffset() - minPullOffset.get() >= pullBatchSize.get() - 1) {
                    sendEntry = new AckSendEntry(ackEntryQueue.peek(), last);
                    while (!ackEntryQueue.isEmpty()) {
                        if (ackEntryQueue.poll() == last) {
                            break;
                        }
                    }
                }
            }
        } finally {
            updateLock.unlock();
        }
        if (sendEntry != null) {
            sendEntryQueue.offer(sendEntry);
            sendAck();
        }
    }

    private boolean trySendAck(long timeout) {
        AckSendEntry sendEntry = null;
        try {
            if (!updateLock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                return false;
            }
        } catch (InterruptedException e) {
            return false;
        }
        try {
            long pullOffsetB = -1, pullOffsetL = -1;
            while (!ackEntryQueue.isEmpty() && ackEntryQueue.peek().isDone()) {
                AckEntry ackEntry = ackEntryQueue.poll();
                if (pullOffsetB < 0)
                    pullOffsetB = ackEntry.pullOffset();
                pullOffsetL = ackEntry.pullOffset();
            }
            if (pullOffsetB >= 0) {
                sendEntry = new AckSendEntry(pullOffsetB, pullOffsetL);
            }
        } finally {
            updateLock.unlock();
        }
        if (sendEntry != null) {
            sendEntryQueue.offer(sendEntry);
            sendAck();
            return true;
        }
        return false;
    }

    private void sendAck() {
        AckSendEntry sendEntry = null;
        sendLock.lock();
        try {
            if (inSending.get()) {
                return;
            }
            while (!sendEntryQueue.isEmpty()) {
                if (sendEntry == null) {
                    sendEntry = sendEntryQueue.poll();
                } else {
                    sendEntry = sendEntry.merge(sendEntryQueue.poll());
                }
            }
            if (sendEntry != null) {
                inSending.set(true);
            } else {
                return;
            }
        } finally {
            sendLock.unlock();
        }
        doSendAck(sendEntry);
    }

    private void doSendAck(final AckSendEntry sendEntry) {
        BrokerGroupInfo brokerGroup = getBrokerGroup();
        if (brokerGroup == null) {
            LOGGER.info("lost broker group: {}. subject={}, consumeGroup={}", brokerGroupName, subject, group);
            sendEntryQueue.offer(sendEntry);
            return;
        }
        ackService.sendAck(getBrokerGroup(), subject, group, sendEntry, new AckService.SendAckCallback() {
            @Override
            public void success() {
                minPullOffset.set(sendEntry.getPullOffsetLast() + 1);
                inSending.set(false);
                final int sendNum = (int) (sendEntry.getPullOffsetLast() - sendEntry.getPullOffsetBegin()) + 1;
                toSendNum.getAndAdd(-sendNum);
                sendNumMonitor.update(sendNum, TimeUnit.MILLISECONDS);
                AckSendQueue.this.sendAck();
            }

            @Override
            public void fail(Exception ex) {
                if (ackSendFailLogLimit.tryAcquire()) {
                    LOGGER.error("send ack fail", ex);
                }
                sendEntryQueue.offer(sendEntry);
                inSending.set(false);
                AckSendQueue.this.sendAck();
            }
        });
    }

    AckSendInfo getAckSendInfo() {
        AckSendInfo info = new AckSendInfo();
        info.setMinPullOffset(minPullOffset.get());
        info.setMaxPullOffset(maxPullOffset.get());
        info.setToSendNum(toSendNum.get());
        return info;
    }

    private BrokerGroupInfo getBrokerGroup() {
        return brokerService.getClusterBySubject(ClientType.CONSUMER, subject).getGroupByName(brokerGroupName);
    }
}
