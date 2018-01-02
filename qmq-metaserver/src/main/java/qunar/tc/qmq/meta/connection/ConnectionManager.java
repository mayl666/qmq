package qunar.tc.qmq.meta.connection;

import com.google.common.base.Strings;
import com.google.common.eventbus.Subscribe;
import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BrokerState;
import qunar.tc.qmq.meta.monitor.QMon;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.netty.ConnectionAddedEvent;
import qunar.tc.qmq.netty.ConnectionRemovedEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class ConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
    private static final HashedWheelTimer timer = new HashedWheelTimer();
    private static final long DEFAULT_TIME_OUT_SECONDS = 10;

    static {
        timer.start();
    }

    private final ConcurrentMap<Channel, String> brokerConnections = new ConcurrentHashMap<>();
    private final Store store;
    private final ConcurrentMap<Channel, Timeout> tasks;

    public ConnectionManager(Store store) {
        this.store = store;
        this.tasks = new ConcurrentHashMap<>();
    }

    public void putBrokerConnection(Channel channel, String groupName) {
        brokerConnections.put(channel, groupName);
    }

    @Subscribe
    @SuppressWarnings("unused")
    public void channelConnected(ConnectionAddedEvent event) {
        final Channel channel = event.getChannel();
        final Timeout timeout = tasks.get(channel);
        if (timeout != null && timeout.isCancelled() && timeout.isExpired()) {
            timeout.cancel();
            tasks.remove(event.getChannel());
        }
    }

    @Subscribe
    @SuppressWarnings("unused")
    public void channelDisconnected(ConnectionRemovedEvent event) {
        final Channel channel = event.getChannel();
        logger.warn("broker connection lost, channel:{}", channel);
        final ConnectionInactiveTimerTask task = new ConnectionInactiveTimerTask(channel);
        final Timeout timeout = timer.newTimeout(task, DEFAULT_TIME_OUT_SECONDS, TimeUnit.SECONDS);
        tasks.putIfAbsent(channel, timeout);
    }

    private class ConnectionInactiveTimerTask implements TimerTask {
        private final Channel channel;

        ConnectionInactiveTimerTask(Channel channel) {
            this.channel = channel;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            final String groupName = brokerConnections.remove(channel);
            if (Strings.isNullOrEmpty(groupName)) {
                return;
            }
            QMon.brokerDisconnectedCountInc(groupName);
            logger.warn("broker group lost connection, groupName:{}", groupName);
            store.updateBrokerGroup(groupName, BrokerState.NRW);
        }
    }
}
