package qunar.tc.qmq.netty;

import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.protocol.RemotingCommand;

/**
 * @author yunfeng.yang
 * @since 2017/7/3
 */
public interface NettyRequestProcessor {
    void processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws Exception;

    boolean rejectRequest();
}
