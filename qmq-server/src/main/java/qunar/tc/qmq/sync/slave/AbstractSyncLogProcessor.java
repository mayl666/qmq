package qunar.tc.qmq.sync.slave;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Conf;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.store.MessageStoreWrapper;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
abstract class AbstractSyncLogProcessor implements SyncLogProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSyncLogProcessor.class);
    private final AtomicBoolean needSync;
    final MessageStoreWrapper messageStoreWrapper;

    AbstractSyncLogProcessor(MessageStoreWrapper messageStoreWrapper) {
        this.messageStoreWrapper = messageStoreWrapper;
        MapConfig mapConfig = MapConfig.get("slave.properties");
        final boolean isSlave = Conf.fromMap(mapConfig.asMap()).getBoolean("sync.enable", false);
        this.needSync = new AtomicBoolean(isSlave);
        mapConfig.addListener(conf -> needSync.set(Boolean.valueOf(conf.get("sync.enable"))));
    }

    @Override
    public void process(Datagram datagram) {
        final ByteBuf body = datagram.getBody();
        final int size = body.readInt();
        if (size == 0) {
            logger.warn("sync data empty");
            return;
        }
        final long startOffset = body.readLong();
        appendLogs(startOffset, body);
    }

    abstract void appendLogs(long startOffset, ByteBuf body);

    @Override
    public boolean isRunning() {
        return needSync.get();
    }

    @Override
    public void destroy() {
        needSync.set(false);
    }
}
