package qunar.tc.qmq.netty;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Metrics;
import qunar.metrics.Timer;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yunfeng.yang
 * @since 2017/7/3
 */
class NettyRequestExecutor {
    private static final Logger logger = LoggerFactory.getLogger(NettyRequestExecutor.class);

    private final NettyRequestProcessor nettyRequestProcessor;

    NettyRequestExecutor(final NettyRequestProcessor nettyRequestProcessor) {
        this.nettyRequestProcessor = nettyRequestProcessor;
    }

    void execute(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        final Timer.Context timer = Metrics.timer("NettyRequestExecutor.execute").get().time();
        final int opaque = cmd.getHeader().getOpaque();

        if (nettyRequestProcessor.rejectRequest()) {
            final Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.BROKER_REJECT, opaque, null);
            ctx.writeAndFlush(response);
            return;
        }

        try {
            nettyRequestProcessor.processRequest(ctx, cmd);
            if (cmd.isOneWay()) {
                return;
            }

            final ListenableFuture<?> future = AsyncRequestContext.getContext().getFuture();
            if (future != null) {
                Futures.addCallback(future, new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result) {
                        ((Datagram) result).getHeader().setOpaque(opaque);
                        ctx.writeAndFlush(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        final Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.BROKER_ERROR, opaque, null);
                        ctx.writeAndFlush(response);
                    }
                });
            }
        } catch (Throwable e) {
            logger.error("process request exception, cmd:{}", cmd, e);
            final Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.BROKER_ERROR, opaque, null);
            ctx.writeAndFlush(response);
        } finally {
            AsyncRequestContext.removeContext();
            cmd.release();
            timer.stop();
        }
    }
}
