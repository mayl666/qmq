package qunar.tc.qmq.netty;


import io.netty.channel.Channel;

/**
 * @author yunfeng.yang
 * @since 2017/8/3
 */
public class ConnectionAddedEvent extends ConnectionEvent{
    ConnectionAddedEvent(Channel channel) {
        super(channel);
    }
}
