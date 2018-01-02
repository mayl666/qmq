package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.config.NettyClientConfigManager;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.protocol.Datagram;

/**
 * @author zhenyu.nie created on 2017 2017/7/4 17:49
 */
class NettyProducerClient {

    private static final long timeout = 3 * 1000;

    private volatile boolean start = false;

    private NettyClient client;

    NettyProducerClient() {
        client = NettyClient.getClient();
    }

    public synchronized void start() {
        if (start) {
            return;
        }

        client.start(NettyClientConfigManager.get().getDefaultClientConfig());
        start = true;
    }

    Datagram sendMessage(String address, Datagram datagram) throws InterruptedException, RemoteTimeoutException, ClientSendException {
        // todo: address failover
        return client.sendSync(address, datagram, timeout);
    }
}
