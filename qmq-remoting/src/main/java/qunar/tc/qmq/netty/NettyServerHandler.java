package qunar.tc.qmq.netty;

import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.Map;

/**
 * @author yunfeng.yang
 * @since 2017/7/3
 */
@ChannelHandler.Sharable
public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
    private static final Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);

    private Map<Short, NettyRequestExecutor> commandTables = Maps.newHashMap();

    void registerProcessors(short requestCode, NettyRequestProcessor processor) {
        this.commandTables.put(requestCode, new NettyRequestExecutor(processor));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        processMessageReceived(ctx, msg);
    }

    private void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand cmd) throws Exception {
        if (cmd != null) {
            switch (cmd.getCommandType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    private void processResponseCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
    }

    private void processRequestCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final NettyRequestExecutor executor = commandTables.get(cmd.getHeader().getCode());
        if (executor == null) {
            cmd.release();
            logger.error("unknown command code, code: {}", cmd.getHeader().getCode());
            Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.UNKNOWN_CODE, cmd.getHeader().getOpaque(), null);
            ctx.writeAndFlush(response);
            return;
        }
        executor.execute(ctx, cmd);
    }
}
