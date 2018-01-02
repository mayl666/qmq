package qunar.tc.qmq.metainfoclient.impl;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.metainfoclient.MetaServerAddressHelper;
import qunar.tc.qmq.netty.DecodeHandler;
import qunar.tc.qmq.netty.EncodeHandler;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.AbstractNettyClient;
import qunar.tc.qmq.netty.client.NettyConnectManageHandler;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequestPayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfoClientNettyImpl extends AbstractNettyClient implements MetaInfoClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaInfoClientNettyImpl.class);
    private static final int DELAY_RETRY_NUM = 5;
    private static final long DELAY_TIME_MS = 5000;
    private static final Supplier<MetaInfoClientNettyImpl> SUPPLIER = Suppliers.memoize(new Supplier<MetaInfoClientNettyImpl>() {
        @Override
        public MetaInfoClientNettyImpl get() {
            return new MetaInfoClientNettyImpl();
        }
    });

    public static MetaInfoClientNettyImpl getClient() {
        MetaInfoClientNettyImpl client = SUPPLIER.get();
        if (!client.isStarted()) {
            NettyClientConfig config = new NettyClientConfig();
            config.setClientWorkerThreads(1);
            client.start(config);
        }
        return client;
    }

    private MetaInfoClientHandler clientHandler;

    private MetaInfoClientNettyImpl() {
        super("qmq-metaclient");
    }

    @Override
    protected void initHandler() {
        clientHandler = new MetaInfoClientHandler();
    }

    @Override
    protected ChannelInitializer<SocketChannel> newChannelInitializer(final NettyClientConfig config, final DefaultEventExecutorGroup eventExecutors, final NettyConnectManageHandler connectManager) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(eventExecutors,
                        new EncodeHandler(),
                        new DecodeHandler(false),
                        connectManager,
                        clientHandler);
            }
        };
    }

    @Override
    public void sendRequest(final MetaInfoRequest request) {
        int retryNum = 0;
        while (!doSendRequest(request)) {
            long sleep = 100;
            if (retryNum++ >= DELAY_RETRY_NUM) {
                sleep = DELAY_TIME_MS;
            }
            LOGGER.info("retry to request meta server after {}ms", sleep);
            try {
                Thread.sleep(sleep);
            } catch (Exception e) {
                //
            }
        }
    }

    private boolean doSendRequest(final MetaInfoRequest request) {
        try {
            final Channel channel = getOrCreateChannel(MetaServerAddressHelper.getAddress());
            final Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.CLIENT_REGISTER, new MetaInfoRequestPayloadHolder(request));
            channel.writeAndFlush(datagram).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        LOGGER.error("request meta info fail. {}", request);
                    }
                }
            });
            return true;
        } catch (Exception e) {
            LOGGER.error("request meta info exception. {}", request, e);
            return false;
        }
    }

    @Override
    public void registerResponseSubscriber(ResponseSubscriber subscriber) {
        clientHandler.registerResponseSubscriber(subscriber);
    }
}
