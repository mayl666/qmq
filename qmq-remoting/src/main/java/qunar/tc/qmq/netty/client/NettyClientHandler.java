package qunar.tc.qmq.netty.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.concurrent.NamedThreadFactory;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.util.RemotingHelper;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yiqun.fan create on 17-8-29.
 */
@ChannelHandler.Sharable
class NettyClientHandler extends SimpleChannelInboundHandler<Datagram> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClientHandler.class);
    private static final long CLEAN_RESPONSE_TABLE_PERIOD_MILLIS = 1000;

    private final AtomicInteger opaque = new AtomicInteger(0);
    private final ConcurrentMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);
    private ScheduledExecutorService cleanResponseTableExecutor;

    NettyClientHandler() {
        cleanResponseTableExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-client-clean"));
        cleanResponseTableExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                NettyClientHandler.this.cleanResponseTable();
            }
        }, 3 * CLEAN_RESPONSE_TABLE_PERIOD_MILLIS, CLEAN_RESPONSE_TABLE_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void cleanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<Map.Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture responseFuture = next.getValue();

            if ((responseFuture.getBeginTime() + responseFuture.getTimeout() + 1000) <= System.currentTimeMillis()) {
                responseFuture.setResponse(null);
                it.remove();
                rfList.add(responseFuture);
                LOGGER.warn("remove timeout request, " + responseFuture);
            }
        }
        for (ResponseFuture responseFuture : rfList) {
            try {
                responseFuture.executeCallbackOnlyOnce();
            } catch (Throwable e) {
                LOGGER.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    void shutdown() {
        cleanResponseTableExecutor.shutdown();
    }

    ResponseFuture newResponse(long timeout, ResponseFuture.Callback callback) throws ClientSendException {
        final int op = opaque.getAndIncrement();
        ResponseFuture future = new ResponseFuture(op, timeout, callback);
        if (responseTable.putIfAbsent(op, future) != null) {
            throw new ClientSendException(ClientSendException.SendErrorCode.ILLEGAL_OPAQUE);
        }
        return future;
    }

    void removeResponse(ResponseFuture responseFuture) {
        if (responseFuture != null) {
            responseTable.remove(responseFuture.getOpaque(), responseFuture);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Datagram datagram) throws Exception {
        if (datagram == null) return;
        processResponse(ctx, datagram);
    }

    private void processResponse(ChannelHandlerContext ctx, Datagram response) {
        int opaque = response.getHeader().getOpaque();
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseTable.remove(opaque);
            responseFuture.setResponse(response);
            responseFuture.executeCallbackOnlyOnce();
        } else {
            LOGGER.warn("receive response, but not matched any request, maybe response timeout or channel had been closed, {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }
    }
}
