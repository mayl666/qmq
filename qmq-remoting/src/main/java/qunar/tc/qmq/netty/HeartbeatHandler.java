package qunar.tc.qmq.netty;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * Created by zhaohui.yu
 */
@ChannelHandler.Sharable
public class HeartbeatHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RemotingCommand) {
            final RemotingCommand command = (RemotingCommand) msg;
            if (command.getHeader().getCode() == CommandCode.HEARTBEAT) {
                Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.HEARTBEAT, command.getHeader().getOpaque(), null);
                ctx.writeAndFlush(response);
                return;
            }
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("heartbeat handler error: {}", ctx.channel(), cause);
        ctx.close();
    }
}
