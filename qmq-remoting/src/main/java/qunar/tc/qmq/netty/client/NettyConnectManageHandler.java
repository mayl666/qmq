package qunar.tc.qmq.netty.client;

import com.google.common.base.Strings;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.util.RemotingHelper;
import qunar.tc.qmq.util.RemotingUtil;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yiqun.fan create on 17-8-29.
 */
@ChannelHandler.Sharable
public class NettyConnectManageHandler extends ChannelDuplexHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyConnectManageHandler.class);
    private static final long LOCK_TIMEOUT_MILLIS = 5000;

    private final Bootstrap bootstrap;
    private final long connectTimeout;
    private final ConcurrentMap<String, ChannelWrapper> channelTables = new ConcurrentHashMap<>();
    private final Lock channelLock = new ReentrantLock();

    NettyConnectManageHandler(Bootstrap bootstrap, long connectTimeout) {
        this.bootstrap = bootstrap;
        this.connectTimeout = connectTimeout;
    }

    void shutdown() {
        for (ChannelWrapper cw : channelTables.values()) {
            this.closeChannel(cw.getChannel());
        }
        channelTables.clear();
    }

    private boolean tryLockChannelTable() {
        try {
            if (channelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                return true;
            } else {
                LOGGER.warn("try to lock channel table, but timeout in {}ms.", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.warn("try to lock channel table, but be interrupted.");
        }
        return false;
    }

    Channel getOrCreateChannel(final String remoteAddr) throws ClientSendException {
        if (Strings.isNullOrEmpty(remoteAddr)) {
            throw new ClientSendException(ClientSendException.SendErrorCode.EMPTY_ADDRESS);
        }
        ChannelWrapper cw = channelTables.get(remoteAddr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        if (!tryLockChannelTable()) {
            throw new ClientSendException(ClientSendException.SendErrorCode.CREATE_CHANNEL_FAIL, remoteAddr);
        }
        try {
            boolean needCreateChannel = true;
            cw = channelTables.get(remoteAddr);
            if (cw != null) {
                if (cw.isOK()) {
                    return cw.getChannel();
                } else if (!cw.getChannelFuture().isDone()) {
                    needCreateChannel = false;
                } else {
                    channelTables.remove(remoteAddr);
                }
            }

            if (needCreateChannel) {
                ChannelFuture cf = bootstrap.connect(RemotingHelper.string2SocketAddress(remoteAddr));
                LOGGER.debug("begin to connect remote host: {}", remoteAddr);
                cw = new ChannelWrapper(cf);
                channelTables.put(remoteAddr, cw);
            }
        } catch (Exception e) {
            LOGGER.error("create channel exception. remoteAddr={}", remoteAddr, e);
        } finally {
            channelLock.unlock();
        }

        if (cw != null) {
            ChannelFuture cf = cw.getChannelFuture();
            if (cf.awaitUninterruptibly(connectTimeout)) {
                if (cw.isOK()) {
                    LOGGER.debug("connect remote host success: {}", remoteAddr);
                    return cw.getChannel();
                } else {
                    LOGGER.warn("connect remote host fail: {}. {}", remoteAddr, cf.toString(), cf.cause());
                }
            } else {
                LOGGER.warn("connect remote host timeout: {}. {}", remoteAddr, cf.toString());
            }
        }
        throw new ClientSendException(ClientSendException.SendErrorCode.CREATE_CHANNEL_FAIL, remoteAddr);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
        final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
        LOGGER.debug("NETTY CLIENT PIPELINE: CONNECT {} => {}", local, remote);
        super.connect(ctx, remoteAddress, localAddress, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        LOGGER.debug("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
        closeChannel(ctx.channel());
        super.disconnect(ctx, promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        LOGGER.debug("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
        closeChannel(ctx.channel());
        super.close(ctx, promise);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                LOGGER.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                closeChannel(ctx.channel());
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
        LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
        closeChannel(ctx.channel());
    }

    private void closeChannel(final Channel channel) {
        if (channel == null)
            return;
        if (!tryLockChannelTable()) {
            return;
        }
        try {
            ChannelWrapper oldCw = null;
            String remoteAddr = null;
            for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                ChannelWrapper cw = entry.getValue();
                if (cw != null && cw.getChannel() == channel) {
                    remoteAddr = entry.getKey();
                    oldCw = cw;
                    break;
                }
            }
            if (oldCw == null) {
                LOGGER.info("close channel but not found in channelTable");
                return;
            }

            channelTables.remove(remoteAddr);
            LOGGER.info("close channel and remove from channelTable. remoteAddr={}", remoteAddr);
            RemotingUtil.closeChannel(channel);
        } catch (Exception e) {
            LOGGER.error("closeChannel: close the channel exception", e);
        } finally {
            this.channelLock.unlock();
        }
    }
}
