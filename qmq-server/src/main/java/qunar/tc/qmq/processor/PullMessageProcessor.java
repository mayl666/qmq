package qunar.tc.qmq.processor;

import com.google.common.base.Strings;
import com.google.common.eventbus.Subscribe;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.commons.lang3.ObjectUtils;
import qunar.tc.qmq.base.BrokerRole;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.consumer.ActionLogManager;
import qunar.tc.qmq.consumer.ConnectionManager;
import qunar.tc.qmq.model.ConsumerGroup;
import qunar.tc.qmq.model.ConsumerPoint;
import qunar.tc.qmq.model.PullExtraParam;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.store.ConsumerLogWroteEvent;
import qunar.tc.qmq.store.GetMessageResult;
import qunar.tc.qmq.store.ManyMessageTransfer;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.ActorUtils;
import qunar.tc.qmq.utils.PullRemoteHeaderSerializer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yunfeng.yang
 * @since 2017/7/4
 */
public class PullMessageProcessor implements NettyRequestProcessor {
    private static final HashedWheelTimer TIMER = new HashedWheelTimer();
    private static final AtomicLong UNIQUE_SUBSCRIBE_INDEX = new AtomicLong(0);

    private final ActorSystem actorSystem;
    private final Pull pull;
    private final ConnectionManager connectionManager;
    private final ActionLogManager actionLogManager;

    private final ConcurrentMap<String, ConcurrentMap<String, String>> subscribers;

    static {
        TIMER.start();
    }

    public PullMessageProcessor(final ActorSystem actorSystem,
                                final ActionLogManager actionLogManager,
                                final MessageStoreWrapper messageStoreWrapper,
                                final ConnectionManager connectionManager) {
        this.actorSystem = actorSystem;
        this.connectionManager = connectionManager;
        this.subscribers = new ConcurrentHashMap<>();
        this.actionLogManager = actionLogManager;

        this.pull = new Pull(messageStoreWrapper, actorSystem);
    }

    @Override
    public void processRequest(final ChannelHandlerContext ctx, final RemotingCommand command) throws Exception {
        final PullRequest pullRequest = QMQSerializer.deserializePullRequest(command.getBody());
        final String subject = pullRequest.getSubject();
        final String group = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();
        final int requestId = command.getHeader().getOpaque();

        connectionManager.putConnection(ctx.channel(), new ConsumerPoint(consumerId, new ConsumerGroup(subject, group)));

        final PullEntry entry = new PullEntry(pullRequest, requestId, this, ctx);
        TIMER.newTimeout(entry, pullRequest.getTimeoutMillis(), TimeUnit.MILLISECONDS);

        final String actorPath = ActorUtils.pullActorPath(subject, group);
        actorSystem.dispatch(actorPath, entry, pull);
    }

    @Override
    public boolean rejectRequest() {
        return BrokerConfig.getBrokerRole() == BrokerRole.SLAVE;
    }

    private void subscribe(String subject, String group) {
        ConcurrentMap<String, String> map = subscribers.get(subject);
        if (map == null) {
            map = new ConcurrentHashMap<>();
            map = ObjectUtils.defaultIfNull(subscribers.putIfAbsent(subject, map), map);
        }
        map.putIfAbsent(group, String.valueOf(UNIQUE_SUBSCRIBE_INDEX.getAndIncrement()));
    }

    private class PullEntry implements TimerTask {
        final String subject;
        final String group;
        final String consumerId;
        final boolean isBroadcast;
        final int requestId;
        final int expectedNum;
        final long pullBegin;

        private final PullMessageProcessor processor;
        private final ChannelHandlerContext ctx;
        private final AtomicInteger state = new AtomicInteger(0);

        PullEntry(PullRequest pullRequest, int requestId, PullMessageProcessor processor, ChannelHandlerContext ctx) {
            this.subject = pullRequest.getSubject();
            this.group = pullRequest.getGroup();
            this.consumerId = pullRequest.getConsumerId();
            this.expectedNum = pullRequest.getRequestNum();
            this.isBroadcast = pullRequest.isBroadcast();
            this.requestId = requestId;
            this.processor = processor;
            this.ctx = ctx;
            this.pullBegin = System.currentTimeMillis();
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (!timeout()) return;
            QMon.pullTimeOutCountInc(subject, group);

            final ConsumerGroup consumerGroup = new ConsumerGroup(subject, group);
            final GetMessageResult getMessageResult = pull.messageStoreWrapper.findMessages(consumerGroup, isBroadcast, expectedNum);
            final ConsumerPoint consumerPoint = new ConsumerPoint(consumerId, consumerGroup);

            if (getMessageResult.getMessageNum() > 0) {
                processMessageResult(consumerPoint, getMessageResult);
                actorSystem.resume(ActorUtils.pullActorPath(subject, group));
            } else {
                processor.processNoMessageResult(ctx, requestId, subject, group, pullBegin);
            }
        }

        boolean processing() {
            return state.compareAndSet(0, 1);
        }

        boolean timeout() {
            return state.compareAndSet(0, -1);
        }

        void initial() {
            state.set(0);
        }

        void subscribe() {
            processor.subscribe(subject, group);
        }

        void processMessageResult(ConsumerPoint consumerPoint, GetMessageResult getMessageResult) {
            processor.processMessageResult(ctx, requestId, isBroadcast, consumerPoint, getMessageResult, pullBegin);
        }
    }

    private class Pull implements ActorSystem.Processor<PullEntry> {
        private final MessageStoreWrapper messageStoreWrapper;
        private final ActorSystem actorSystem;

        Pull(MessageStoreWrapper messageStoreWrapper, ActorSystem actorSystem) {
            this.messageStoreWrapper = messageStoreWrapper;
            this.actorSystem = actorSystem;
        }

        @Override
        public void process(final PullEntry entry) {
            if (!entry.processing()) return;

            final ConsumerGroup consumerGroup = new ConsumerGroup(entry.subject, entry.group);
            final GetMessageResult getMessageResult = messageStoreWrapper.findMessages(consumerGroup, entry.isBroadcast, entry.expectedNum);
            final ConsumerPoint consumerPoint = new ConsumerPoint(entry.consumerId, consumerGroup);

            if (getMessageResult.getMessageNum() > 0) {
                entry.processMessageResult(consumerPoint, getMessageResult);
            } else {
                final String actorPath = ActorUtils.pullActorPath(entry.subject, entry.group);
                actorSystem.suspend(actorPath);

                entry.subscribe();
                QMon.suspendRequestCountInc(entry.subject, entry.group);
                entry.initial();
                actorSystem.dispatch(actorPath, entry, this);
            }
        }
    }

    @Subscribe
    @SuppressWarnings("unused")
    public void onConsumerLogWrote(final ConsumerLogWroteEvent e) {
        if (!e.isSuccess() || Strings.isNullOrEmpty(e.getSubject())) {
            return;
        }
        final String subject = e.getSubject();
        final ConcurrentMap<String, String> map = this.subscribers.get(subject);
        if (map == null) return;

        for (String group : map.keySet()) {
            this.actorSystem.resume(ActorUtils.pullActorPath(subject, group));
            QMon.resumeActorCountInc(subject, group);
        }
//        final ConcurrentMap<String, String> removeMap = this.subscribers.remove(subject);
//        if (!removeMap.equals(map)) {
//            this.subscribers.putIfAbsent(subject, removeMap);
//        }
    }

    private void processNoMessageResult(ChannelHandlerContext ctx, int opaque, String subject, String group, long pullBegin) {
        QMon.pulledNoMessagesCountInc(subject, group);
        QMon.pullProcessTime(subject, group, System.currentTimeMillis() - pullBegin);

        final RemotingHeader header = RemotingBuilder.buildResponseHeader(CommandCode.NO_MESSAGE, opaque);
        final Datagram datagram = new Datagram();
        datagram.setHeader(header);
        ctx.writeAndFlush(datagram);
    }

    private void processMessageResult(ChannelHandlerContext ctx, int opaque, boolean isBroadcast, ConsumerPoint consumerPoint, GetMessageResult getMessageResult, long pullBegin) {
        QMon.pulledMessagesCountInc(consumerPoint.getSubject(), consumerPoint.getGroup(), getMessageResult.getMessageNum());
        QMon.pullProcessTime(consumerPoint.getSubject(), consumerPoint.getGroup(), System.currentTimeMillis() - pullBegin);

        // TODO(keli.wang): pull log 的写入也改为批量，这里使用future
        final PullExtraParam pullExtraParam = actionLogManager.putPullActions(isBroadcast, consumerPoint, getMessageResult);

        final RemotingHeader remotingHeader = RemotingBuilder.buildResponseHeader(CommandCode.SUCCESS, opaque);
        final ByteBuffer buffer = PullRemoteHeaderSerializer.serializePullHeader(remotingHeader, pullExtraParam, getMessageResult.getBufferTotalSize());
        ctx.writeAndFlush(new ManyMessageTransfer(buffer, getMessageResult.getSegmentBuffers(), getMessageResult.getBufferTotalSize()));
    }
}