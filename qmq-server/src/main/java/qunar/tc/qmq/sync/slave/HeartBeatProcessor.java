package qunar.tc.qmq.sync.slave;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Conf;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.store.MessageStoreWrapper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static qunar.tc.qmq.configuration.BrokerConstants.DEFAULT_HEARTBEAT_SLEEP_TIMEOUT_MS;
import static qunar.tc.qmq.configuration.BrokerConstants.HEARTBEAT_SLEEP_TIMEOUT_MS;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class HeartBeatProcessor implements SyncLogProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HeartBeatProcessor.class);

    private final MessageStoreWrapper messageStoreWrapper;
    private final long sleepTimeoutMs;
    private final AtomicBoolean needHeartBeat;

    HeartBeatProcessor(MessageStoreWrapper messageStoreWrapper) {
        this.messageStoreWrapper = messageStoreWrapper;
        final MapConfig mapConfig = MapConfig.get("slave.properties");
        final Conf slaveConfig = Conf.fromMap(mapConfig.asMap());
        this.sleepTimeoutMs = slaveConfig.getLong(HEARTBEAT_SLEEP_TIMEOUT_MS, DEFAULT_HEARTBEAT_SLEEP_TIMEOUT_MS);
        final boolean isRunning = slaveConfig.getBoolean("heartbeat.enable", true);
        this.needHeartBeat = new AtomicBoolean(isRunning);
        mapConfig.addListener(conf -> needHeartBeat.set(Boolean.valueOf(conf.get("heartbeat.enable"))));
    }

    @Override
    public void process(Datagram syncData) {
        try {
            TimeUnit.MILLISECONDS.sleep(sleepTimeoutMs);
        } catch (InterruptedException e) {
            logger.error("heart beat sleep error", e);
        }
    }

    @Override
    public SyncRequest getRequest() {
        long messageLogMaxOffset = messageStoreWrapper.getMessageLogMaxOffset();
        long actionLogMaxOffset = messageStoreWrapper.getActionLogMaxOffset();
        return new SyncRequest(SyncType.heartbeat.getCode(), messageLogMaxOffset, actionLogMaxOffset);
    }

    @Override
    public boolean isRunning() {
        return needHeartBeat.get();
    }

    @Override
    public void destroy() {
        needHeartBeat.set(false);
    }
}
