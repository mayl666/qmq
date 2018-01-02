package qunar.tc.qmq.consumer.pull;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerLoadBalanceImpl;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.SwitchWaiter;
import qunar.tc.qmq.config.PullState;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.consumer.pull.model.AckSendInfo;
import qunar.tc.qmq.consumer.pull.model.PullParam;
import qunar.tc.qmq.consumer.pull.model.PullResult;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class PullEntry implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PullEntry.class);
    private static final long PAUSE_TIME_MILLIS = 100;
    private static final long PAUSE_PULL_TIME_MILLIS = 1000;

    private final BrokerService brokerService;
    private final BrokerLoadBalance brokerLoadBalance = BrokerLoadBalanceImpl.getInstance();
    private final PullService pullService = PullService.getInstance();
    private final AckService ackService = AckService.getInstance();
    private final PushConsumer pushConsumer;
    private final String subject;
    private final String group;
    private final String originSubject;
    private final boolean isBroadcast;
    private final AtomicReference<Integer> pullBatchSize;
    private final AtomicReference<Integer> pullTimeout;
    private final AtomicReference<Integer> ackNosendLimit;
    private final AtomicReference<Integer> pullState;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final SwitchWaiter onlineSwitcher = new SwitchWaiter(false);

    PullEntry(BrokerService brokerService, PushConsumer pushConsumer, boolean isBroadcast) {
        this.brokerService = brokerService;
        this.pushConsumer = pushConsumer;
        this.subject = pushConsumer.getSubject();
        this.group = pushConsumer.getGroup();
        this.originSubject = RetrySubjectUtils.isRetrySubject(subject) ? RetrySubjectUtils.getRealSubject(subject) : subject;
        this.isBroadcast = isBroadcast;
        this.pullBatchSize = PullSubjectsConfig.get().getPullBatchSize(subject);
        this.pullTimeout = PullSubjectsConfig.get().getPullTimeout(subject);
        this.ackNosendLimit = PullSubjectsConfig.get().getAckNosendLimit(subject);
        this.pullState = PullSubjectsConfig.get().getPullState(subject);
    }

    void online() {
        onlineSwitcher.on();
        LOGGER.info("consumer online. subject={}, group={}", subject, group);
    }

    void offline() {
        onlineSwitcher.off();
        LOGGER.info("consumer offline. subject={}, group={}", subject, group);
    }

    void destory() {
        isRunning.set(false);
    }

    @Override
    public void run() {
        BrokerGroupInfo pullBrokerGroup = null;
        int pullFailNum = 0;
        boolean needPause = false;
        final RateLimiter waitAckLogLimit = RateLimiter.create(0.5);

        while (isRunning.get()) {
            if (!pushConsumer.cleanLocalBuffer() || needPause) {
                needPause = false;
                pause(PAUSE_TIME_MILLIS);
                continue;
            }
            if (pullState.get() == PullState.PAUSE_PULL.ordinal()) {
                pause(PAUSE_PULL_TIME_MILLIS);
                continue;
            }
            onlineSwitcher.waitOn();

            BrokerClusterInfo brokerCluster = brokerService.getClusterBySubject(ClientType.CONSUMER, subject);
            pullBrokerGroup = brokerLoadBalance.loadBalance(brokerCluster, pullBrokerGroup);
            if (pullBrokerGroup == null || !pullBrokerGroup.isAvailable()) {
                pause(PAUSE_TIME_MILLIS);
                continue;
            }

            AckSendInfo ackSendInfo = ackService.getAckSendInfo(pullBrokerGroup, subject, group);
            if (ackSendInfo.getToSendNum() > ackNosendLimit.get()) {
                if (waitAckLogLimit.tryAcquire()) {
                    LOGGER.info("wait ack and pause. ackSend={}", ackSendInfo);
                }
                pause(PAUSE_TIME_MILLIS);
                continue;
            }

            final PullParam pullParam = pullService.buildPullParam(pullBrokerGroup, ackSendInfo, pullBatchSize.get(), pullTimeout.get(), isBroadcast);
            Future<PullResult> resultFuture = pullService.pull(subject, group, pullParam);
            try {
                PullResult pullResult = resultFuture.get();
                pullFailNum = 0;
                needPause = false;
                if (pullResult.getResponseCode() == CommandCode.BROKER_REJECT) {
                    pullBrokerGroup.setAvailable(false);
                    brokerService.refresh(ClientType.CONSUMER, subject);
                    continue;
                }
                if (pullResult.getMessages() != null && !pullResult.getMessages().isEmpty()) {
                    List<PulledMessage> messages = ackService.convertToPulledMessage(subject, group, pullResult, pushConsumer);
                    if (!originSubject.equals(subject)) {
                        for (PulledMessage message : messages) {
                            message.setSubject(originSubject);
                            message.setProperty(BaseMessage.keys.qmq_prefix, originSubject);
                        }
                    }
                    pushConsumer.push(messages);
                }
            } catch (Exception e) {
                Throwable t = (e instanceof ExecutionException) ? e.getCause() : e;
                LOGGER.error("pull message exception. subject={}, group={}", subject, group, t);
                int pullFailLimit = brokerCluster.getGroups().size() * 2;
                if (++pullFailNum > pullFailLimit) {
                    LOGGER.warn("pull fail num {} > {}, pause {} ms", pullFailNum, pullFailLimit, PAUSE_TIME_MILLIS);
                    needPause = true;
                }
            }
        }
    }

    private void pause(long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (Exception e) {
            // ignore
        }
    }
}
