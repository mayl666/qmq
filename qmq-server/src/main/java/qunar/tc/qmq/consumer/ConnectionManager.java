package qunar.tc.qmq.consumer;

import com.google.common.eventbus.Subscribe;
import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.model.ActionLogOffset;
import qunar.tc.qmq.model.ConsumerPoint;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.netty.ConnectionAddedEvent;
import qunar.tc.qmq.netty.ConnectionRemovedEvent;
import qunar.tc.qmq.store.MessageStoreWrapper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/8/20
 */
public class ConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
    private static final HashedWheelTimer timer = new HashedWheelTimer();

    static {
        timer.start();
    }

    private final ConcurrentMap<Channel, ConsumerPoint> connections;
    private final MessageStoreWrapper messageStoreWrapper;
    private final ActionLogManager actionLogManager;
    private final ConcurrentMap<Channel, Timeout> tasks;
    private final long timeoutSeconds;

    public ConnectionManager(MessageStoreWrapper messageStoreWrapper, Config config, ActionLogManager actionLogManager) {
        this.actionLogManager = actionLogManager;
        this.connections = new ConcurrentHashMap<>();
        this.tasks = new ConcurrentHashMap<>();
        this.messageStoreWrapper = messageStoreWrapper;
        this.timeoutSeconds = config.getLong("connection.timeout.seconds", 5L);
    }

    @Subscribe
    @SuppressWarnings("unused")
    public void channelConnected(ConnectionAddedEvent event) {
        final Timeout timeout = tasks.get(event.getChannel());
        if (timeout != null && timeout.isCancelled() && timeout.isExpired()) {
            timeout.cancel();
            tasks.remove(event.getChannel());
        }
    }

    @Subscribe
    @SuppressWarnings("unused")
    public void channelDisconnected(ConnectionRemovedEvent event) {
        final Channel channel = event.getChannel();
        final ConsumerPoint consumerPoint = connections.get(channel);
        if (consumerPoint == null) {
            logger.info("channel disconnected : {}", channel);
            return;
        }
        final ConnectionInactiveTimerTask task = new ConnectionInactiveTimerTask(consumerPoint, channel);
        final Timeout timeout = timer.newTimeout(task, this.timeoutSeconds, TimeUnit.SECONDS);
        tasks.putIfAbsent(channel, timeout);
    }

    public void putConnection(Channel channel, ConsumerPoint consumerPoint) {
        connections.putIfAbsent(channel, consumerPoint);
    }

    private class ConnectionInactiveTimerTask implements TimerTask {
        private final ConsumerPoint consumerPoint;
        private final Channel channel;

        ConnectionInactiveTimerTask(ConsumerPoint consumerPoint, Channel channel) {
            this.consumerPoint = consumerPoint;
            this.channel = channel;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            final ActionLogOffset actionLogOffset = actionLogManager.getActionLogOffset(consumerPoint);
            if (actionLogOffset == null) {
                logger.warn("find no actionLogOffset for consumerPoint-[{}]", consumerPoint);
                return;
            }
            try {
                actionLogOffset.ackSemaphoreAcquire();
                final long firstPullLogOffset = actionLogOffset.getAckLogOffset() + 1;
                final long lastPullLogOffset = actionLogOffset.getPullLogOffset();
                if (lastPullLogOffset < firstPullLogOffset) {
                    return;
                }
                messageStoreWrapper.putErrorMessageForPullLogOffsetRange(consumerPoint, firstPullLogOffset, lastPullLogOffset);
                actionLogOffset.setAckLogOffset(lastPullLogOffset);
                connections.remove(channel);

                QMon.connectionInActiveErrorCountInc(consumerPoint.getConsumerId());
            } finally {
                actionLogOffset.ackSemaphoreRelease();
            }
        }
    }
}
