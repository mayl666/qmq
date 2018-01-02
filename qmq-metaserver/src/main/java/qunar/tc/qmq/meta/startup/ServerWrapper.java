package qunar.tc.qmq.meta.startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Conf;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.connection.ConnectionManager;
import qunar.tc.qmq.meta.processor.BrokerRegisterProcessor;
import qunar.tc.qmq.meta.processor.ClientRegisterProcessor;
import qunar.tc.qmq.meta.store.DatabaseStore;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.netty.NettyServer;
import qunar.tc.qmq.protocol.CommandCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class ServerWrapper implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(ServerWrapper.class);
    public static final int DEFAULT_META_SERVER_PORT = 20883;

    private final List<Disposable> resources;
    private final Conf conf;

    public ServerWrapper() {
        this.resources = new ArrayList<>();
        final Map<String, String> configMap = MapConfig.get("config.properties").asMap();
        this.conf = Conf.fromMap(configMap);
    }

    public void start() {
        final int port = conf.getInt("meta.server.port", DEFAULT_META_SERVER_PORT);

        final Store store = new DatabaseStore();
        final CachedMetaInfoManager cachedMetaInfoManager = new CachedMetaInfoManager(store, conf);
        final ConnectionManager connectionManager = new ConnectionManager(store);
        final BrokerRegisterProcessor brokerRegisterProcessor = new BrokerRegisterProcessor(cachedMetaInfoManager, connectionManager, store);
        final ClientRegisterProcessor clientRegisterProcessor = new ClientRegisterProcessor(cachedMetaInfoManager, store);

        final NettyServer metaNettyServer = new NettyServer(port);
        metaNettyServer.registerProcessor(CommandCode.CLIENT_REGISTER, clientRegisterProcessor);
        metaNettyServer.registerProcessor(CommandCode.BROKER_REGISTER, brokerRegisterProcessor);
        metaNettyServer.registerConnectionEventListener(connectionManager);
        metaNettyServer.start();

        resources.add(cachedMetaInfoManager);
        resources.add(metaNettyServer);
    }

    @Override
    public void destroy() {
        if (resources.isEmpty()) return;

        for (int i = resources.size() - 1; i >= 0; --i) {
            try {
                resources.get(i).destroy();
            } catch (Throwable e) {
                logger.error("destroy resource failed", e);
            }
        }
    }
}
