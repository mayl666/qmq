package qunar.tc.qmq.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yiqun.fan create on 17-7-5.
 */
public class RemotingUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemotingUtil.class);

    public static void closeChannel(Channel channel) {
        final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.info("close channel result: {}. remoteAddr={}", remoteAddr, future.isSuccess());
            }
        });
    }
}
