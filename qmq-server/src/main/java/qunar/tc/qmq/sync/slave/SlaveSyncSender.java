package qunar.tc.qmq.sync.slave;

import qunar.agile.Conf;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.protocol.Datagram;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
class SlaveSyncSender {
    private final NettyClient nettyClient;
    private final long timeout;

    SlaveSyncSender(NettyClient nettyClient) {
        final Conf salveConfig = Conf.fromMap(MapConfig.get("slave.properties").asMap());
        this.timeout = salveConfig.getLong("slave.sync.timeout", 3000L);
        this.nettyClient = nettyClient;
    }

    Datagram send(String address, Datagram datagram) throws InterruptedException, RemoteTimeoutException, ClientSendException {
        return nettyClient.sendSync(address, datagram, timeout);
    }
}
