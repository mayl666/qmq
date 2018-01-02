package qunar.tc.qmq.netty;

import io.netty.channel.Channel;

/**
 * @author yunfeng.yang
 * @since 2017/8/3
 */
public class ConnectionEvent {
//    private final String address;
//    private final int port;
    private final Channel channel;

    ConnectionEvent(Channel channel) {
        this.channel = channel;
    }

    public Channel getChannel() {
        return channel;
    }
}
