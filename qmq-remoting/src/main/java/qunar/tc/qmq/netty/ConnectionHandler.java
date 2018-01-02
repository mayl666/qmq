package qunar.tc.qmq.netty;

import com.google.common.eventbus.EventBus;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Gauge;
import qunar.metrics.Metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author keli.wang
 * @since 2017/2/28
 */
@ChannelHandler.Sharable
class ConnectionHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionHandler.class);

    private final AtomicLong connectionCounter = new AtomicLong(0);
    private final EventBus connectionEventBus;

    ConnectionHandler() {
        connectionEventBus = new EventBus("connection_changed");
        Metrics.gauge("NettyProvider.Connection.ActiveCount").call(new Gauge() {
            @Override
            public double getValue() {
                return connectionCounter.doubleValue();
            }
        });
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("client {} connected", ctx.channel().remoteAddress());
        connectionCounter.incrementAndGet();

        super.channelActive(ctx);

        final ConnectionAddedEvent event = new ConnectionAddedEvent(ctx.channel());
        connectionEventBus.post(event);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("client {} disconnected", ctx.channel().remoteAddress());
        connectionCounter.decrementAndGet();

        super.channelInactive(ctx);

        final ConnectionRemovedEvent event = new ConnectionRemovedEvent(ctx.channel());
        connectionEventBus.post(event);
    }

    void registerConnectionEventListener(Object listener) {
        connectionEventBus.register(listener);
    }
}
