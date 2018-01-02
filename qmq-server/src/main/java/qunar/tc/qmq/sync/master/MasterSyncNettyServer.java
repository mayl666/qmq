package qunar.tc.qmq.sync.master;

import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.netty.NettyServer;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.store.MessageStoreWrapper;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class MasterSyncNettyServer implements Disposable {
    private final NettyServer nettyServer;
    private final SyncRequestProcessor syncRequestProcessor;

    public MasterSyncNettyServer(final Config config,
                                 final MessageStoreWrapper messageStoreWrapper) {
        this.nettyServer = new NettyServer(config.getInt("sync.port", 20882));
        this.syncRequestProcessor = new SyncRequestProcessor(messageStoreWrapper, config);
    }

    public void registerSyncEvent(Object listener) {
        syncRequestProcessor.registerSyncEvent(listener);
    }

    public void start() {
        nettyServer.registerProcessor(CommandCode.SYNC_REQUEST, syncRequestProcessor);
        nettyServer.start();
    }

    @Override
    public void destroy() {
        nettyServer.destroy();
    }
}
