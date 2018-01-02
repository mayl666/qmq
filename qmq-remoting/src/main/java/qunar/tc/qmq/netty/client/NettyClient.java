package qunar.tc.qmq.netty.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.netty.DecodeHandler;
import qunar.tc.qmq.netty.EncodeHandler;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.util.RemotingHelper;

import static qunar.tc.qmq.netty.exception.ClientSendException.SendErrorCode;

/**
 * @author yiqun.fan create on 17-7-3.
 */
public class NettyClient extends AbstractNettyClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);
    private static final NettyClient INSTANCE = new NettyClient();

    public static NettyClient getClient() {
        return INSTANCE;
    }

    private NettyClientHandler clientHandler;

    private NettyClient() {
        super("qmq-client");
    }

    @Override
    protected void initHandler() {
        clientHandler = new NettyClientHandler();
    }

    @Override
    protected void destoryHandler() {
        clientHandler.shutdown();
    }

    @Override
    protected ChannelInitializer<SocketChannel> newChannelInitializer(final NettyClientConfig config, final DefaultEventExecutorGroup eventExecutors, final NettyConnectManageHandler connectManager) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(eventExecutors,
                        new EncodeHandler(),
                        new DecodeHandler(false),
                        new IdleStateHandler(0, 0, config.getClientChannelMaxIdleTimeSeconds()),
                        connectManager,
                        clientHandler);
            }
        };
    }

    public Datagram sendSync(String brokerAddr, final Datagram request, long timeoutMs) throws InterruptedException, ClientSendException, RemoteTimeoutException {
        ResponseFuture responseFuture = sendAsync(brokerAddr, request, 2 * timeoutMs, null);
        Datagram response = responseFuture.waitResponse(timeoutMs);
        if (response != null) {
            return response;
        }
        if (!responseFuture.isSendOk()) {
            LOGGER.warn("send request fail. brokerAddr={}", brokerAddr);
            throw new ClientSendException(SendErrorCode.WRITE_CHANNEL_FAIL, brokerAddr, responseFuture.getCause());
        }
        LOGGER.warn("request timeout in {}ms. brokerAddr={}", timeoutMs, brokerAddr);
        throw new RemoteTimeoutException(brokerAddr, responseFuture.getTimeout(), responseFuture.getCause());
    }

    public ResponseFuture sendAsync(String brokerAddr, Datagram request, long responseTimeout, ResponseFuture.Callback callback) throws ClientSendException {
        final Channel channel = getOrCreateChannel(brokerAddr);
        final ResponseFuture responseFuture = clientHandler.newResponse(responseTimeout, callback);
        request.getHeader().setOpaque(responseFuture.getOpaque());

        try {
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    responseFuture.setSendOk(future.isSuccess());
                    if (future.isSuccess()) {
                        responseFuture.setSendOk(true);
                        return;
                    }
                    clientHandler.removeResponse(responseFuture);
                    responseFuture.setSendOk(false);
                    responseFuture.setResponse(null);
                    responseFuture.setCause(future.cause());
                    LOGGER.error("send request to broker failed.", future.cause());
                    try {
                        responseFuture.executeCallbackOnlyOnce();
                    } catch (Throwable e) {
                        LOGGER.error("execute callback when send error exception", e);
                    }
                }
            });
            return responseFuture;
        } catch (Exception e) {
            clientHandler.removeResponse(responseFuture);
            responseFuture.setSendOk(false);
            LOGGER.warn("send request fail. brokerAddr={}", brokerAddr);
            throw new ClientSendException(SendErrorCode.WRITE_CHANNEL_FAIL, RemotingHelper.parseChannelRemoteAddr(channel), e);
        }
    }
}
