package qunar.tc.qmq.startup;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BrokerRole;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.consumer.ActionLogManager;
import qunar.tc.qmq.consumer.ActionLogManagerImpl;
import qunar.tc.qmq.consumer.ConnectionManager;
import qunar.tc.qmq.netty.NettyServer;
import qunar.tc.qmq.processor.*;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.register.BrokerRegisterService;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.sync.master.MasterSyncNettyServer;
import qunar.tc.qmq.sync.slave.MasterSlaveSyncManager;

import java.util.List;

import static qunar.tc.qmq.configuration.BrokerConstants.*;

/**
 * @author yunfeng.yang
 * @since 2017/6/30
 */
public class ServerWrapper implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(ServerWrapper.class);
    private final List<Disposable> resources;
    private final Config config;

    public ServerWrapper(final Config config) {
        this.config = config;
        this.resources = Lists.newArrayList();
    }

    public void start() {
        final Integer listenPort = config.getInt(PORT_CONFIG, DEFAULT_PORT);
        final String metaServerEndpoint = config.getString(META_SERVER_ENDPOINT);

        final NettyServer nettyServer = new NettyServer(listenPort);
        final MessageStore messageStore = new DefaultMessageStore(new MessageStoreConfigImpl(config));
        messageStore.start();
        final MessageStoreWrapper messageStoreWrapper = new MessageStoreWrapperImpl(messageStore);

        final ActorSystem actorSystem = new ActorSystem(ACTOR_SYSTEM_NAME);
        final ActionLogManager actionLogManager = new ActionLogManagerImpl(messageStoreWrapper);
        final ConnectionManager connectionManager = new ConnectionManager(messageStoreWrapper, config, actionLogManager);

        final BrokerRegisterService brokerRegisterService = new BrokerRegisterService(listenPort, metaServerEndpoint);

        final SubjectWritableService subjectWritableService = new SubjectWritableService();
        if (BrokerConfig.getBrokerRole() == BrokerRole.MASTER) {
            brokerRegisterService.register(subjectWritableService);
            brokerRegisterService.start();
            resources.add(brokerRegisterService);
        }

        final Receiver receiver = new Receiver(config, messageStoreWrapper, subjectWritableService);

        final PullMessageProcessor pullMessageProcessor = new PullMessageProcessor(actorSystem, actionLogManager, messageStoreWrapper, connectionManager);
        messageStore.registerEventListener(pullMessageProcessor);
        final SendMessageProcessor sendMessageProcessor = new SendMessageProcessor(receiver, config);
        final AckMessageProcessor ackMessageProcessor = new AckMessageProcessor(actorSystem, actionLogManager);

        nettyServer.registerProcessor(CommandCode.SEND_MESSAGE, sendMessageProcessor);
        nettyServer.registerProcessor(CommandCode.PULL_MESSAGE, pullMessageProcessor);
        nettyServer.registerProcessor(CommandCode.ACK_REQUEST, ackMessageProcessor);
        nettyServer.registerConnectionEventListener(connectionManager);
        nettyServer.start();

        if (BrokerConfig.getBrokerRole() == BrokerRole.MASTER) {
            final MasterSyncNettyServer masterSyncNettyServer = new MasterSyncNettyServer(config, messageStoreWrapper);
            masterSyncNettyServer.registerSyncEvent(receiver);
            masterSyncNettyServer.start();

            resources.add(masterSyncNettyServer);
        }

        resources.add(nettyServer);
        resources.add(messageStore);

        if (BrokerConfig.getBrokerRole() == BrokerRole.SLAVE) {
            final MasterSlaveSyncManager masterSlaveSyncManager = new MasterSlaveSyncManager(BrokerConfig.getMasterAddress(), messageStoreWrapper);
            resources.add(masterSlaveSyncManager);
        }
    }

    @Override
    public void destroy() {
        if (resources == null || resources.isEmpty()) return;

        for (int i = resources.size() - 1; i >= 0; --i) {
            try {
                resources.get(i).destroy();
            } catch (Throwable e) {
                logger.error("destroy resource failed", e);
            }
        }
    }
}
