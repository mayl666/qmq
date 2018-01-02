package qunar.tc.qmq.register;

import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.management.ServerManager;
import qunar.management.Switchable;
import qunar.tc.qmq.base.BrokerRegisterRequest;
import qunar.tc.qmq.base.BrokerRegisterResponse;
import qunar.tc.qmq.base.BrokerRequestType;
import qunar.tc.qmq.base.BrokerState;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2017/9/1
 */
public class BrokerRegisterService implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerRegisterService.class);

    private static final Serializer SERIALIZER = SerializerFactory.create();
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);
    private static final int HEARTBEAT_DELAY_SECONDS = 10;

    private final ScheduledExecutorService heartbeatScheduler;
    private final EventBus notifier;
    private final Random rand;
    private final MetaServerLocator locator;
    private final NettyClient client;
    private final String brokerAddress;

    private volatile int brokerState;
    private volatile String endpoint;

    public BrokerRegisterService(final int port, final String metaServerEndpoint) {
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("broker-register-heartbeat-%d").build());
        this.notifier = new EventBus("broker-register-notifier");
        this.rand = new Random();
        this.locator = new MetaServerLocator(metaServerEndpoint);
        this.client = NettyClient.getClient();
        this.client.start(new NettyClientConfig());
        this.brokerAddress = BrokerConfig.getBrokerAddress() + ":" + port;

        repickEndpoint();
    }

    public void register(final Object handler) {
        notifier.register(handler);
    }

    public void start() {
        ServerManager.getInstance().addSwitcher("NewQMQ-BrokerRegister", new Switchable() {
            @Override
            public boolean offline() {
                return brokerOffline();
            }

            @Override
            public boolean online() {
                return brokerOnline();
            }
        });

        heartbeatScheduler.scheduleWithFixedDelay(this::heartbeat, 0, HEARTBEAT_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    private void heartbeat() {
        try {
            final Datagram datagram = client.sendSync(endpoint, buildDatagram(BrokerRequestType.HEARTBEAT), TIMEOUT_MS);
            notifier.post(deserialize(datagram.getBody()));
        } catch (Exception e) {
            LOG.error("Send HEARTBEAT message to meta server failed", e);
            repickEndpoint();
        }
    }

    private BrokerRegisterResponse deserialize(ByteBuf buf) {
        final byte[] bytes = new byte[buf.readableBytes()];
        final int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return SERIALIZER.deSerialize(bytes, BrokerRegisterResponse.class);
    }

    private boolean brokerOnline() {
        try {
            brokerState = BrokerState.RW.getCode();
            client.sendSync(endpoint, buildDatagram(BrokerRequestType.ONLINE), TIMEOUT_MS);
            notifier.post(true);
            return true;
        } catch (Exception e) {
            LOG.error("Send ONLINE message to meta server failed", e);
            repickEndpoint();
        }

        return false;
    }

    private boolean brokerOffline() {
        try {
            brokerState = BrokerState.NRW.getCode();
            client.sendSync(endpoint, buildDatagram(BrokerRequestType.OFFLINE), TIMEOUT_MS);
            notifier.post(false);
            return true;
        } catch (Exception e) {
            LOG.error("Send OFFLINE message to meta server failed", e);
            repickEndpoint();
        }

        return false;
    }

    private void repickEndpoint() {
        final ImmutableList<String> endpoints = locator.getMetaServerEndpoints();
        if (endpoints.isEmpty()) {
            LOG.error("meta server address list is empty!");
        } else {
            endpoint = endpoints.get(rand.nextInt(endpoints.size()));
        }
    }

    private Datagram buildDatagram(final BrokerRequestType checkType) {
        final Datagram datagram = new Datagram();
        final RemotingHeader header = new RemotingHeader();
        header.setCode(CommandCode.BROKER_REGISTER);
        datagram.setHeader(header);
        datagram.setPayloadHolder(out -> {
            final BrokerRegisterRequest request = buildRegisterRequest(checkType);
            out.writeBytes(SERIALIZER.serializeToBytes(request));
        });
        return datagram;
    }

    private BrokerRegisterRequest buildRegisterRequest(final BrokerRequestType checkType) {
        final BrokerRegisterRequest request = new BrokerRegisterRequest();
        request.setGroupName(BrokerConfig.getBrokerName());
        request.setBrokerRole(BrokerConfig.getBrokerRole().getCode());
        request.setBrokerState(brokerState);
        request.setRequestType(checkType.getCode());
        request.setBrokerAddress(brokerAddress);
        return request;
    }

    @Override
    public void destroy() {
        heartbeatScheduler.shutdown();
        try {
            heartbeatScheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Shutdown heartbeat scheduler interrupted.");
        }
    }
}
