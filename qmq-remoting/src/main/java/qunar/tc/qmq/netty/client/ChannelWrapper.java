package qunar.tc.qmq.netty.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author yiqun.fan create on 17-8-29.
 */
class ChannelWrapper {
    private final ChannelFuture channelFuture;

    ChannelWrapper(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    ChannelFuture getChannelFuture() {
        return channelFuture;
    }

    Channel getChannel() {
        return this.channelFuture.channel();
    }

    boolean isOK() {
        return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
    }
}
