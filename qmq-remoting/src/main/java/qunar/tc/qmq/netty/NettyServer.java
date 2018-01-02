package qunar.tc.qmq.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;

/**
 * @author yunfeng.yang
 * @since 2017/6/30
 */
public class NettyServer implements Disposable{
    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

    private static final NioEventLoopGroup BOSS_GROUP = new NioEventLoopGroup(1,
            new DefaultThreadFactory("netty-server-boss", true));
    private static final NioEventLoopGroup WORKER_GROUP = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
            new DefaultThreadFactory("netty-server-worker", true));

    private final ServerBootstrap serverBootstrap;
    private final NettyServerHandler nettyServerHandler;
    private final ConnectionHandler connectionHandler;

    private Channel channel;
    private int port;

    public NettyServer(final int port) {
        this.port = port;
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerHandler = new NettyServerHandler();
        this.connectionHandler = new ConnectionHandler();
    }

    public void registerProcessor(short requestCode, NettyRequestProcessor processor) {
        nettyServerHandler.registerProcessors(requestCode, processor);
    }

    public void registerConnectionEventListener(Object listener) {
        connectionHandler.registerConnectionEventListener(listener);
    }

    public void start() {
        serverBootstrap.option(ChannelOption.SO_REUSEADDR, true);
        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        serverBootstrap.group(BOSS_GROUP, WORKER_GROUP)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("connectionHandler", connectionHandler);
                        ch.pipeline().addLast("encoder", new EncodeHandler());
                        ch.pipeline().addLast("decoder", new DecodeHandler(true));
                        ch.pipeline().addLast("heartbeat", new HeartbeatHandler());
                        ch.pipeline().addLast("dispatcher", nettyServerHandler);
                    }
                });
        try {
            channel = serverBootstrap.bind(port).await().channel();
        } catch (InterruptedException e) {
            LOG.error("server start fail", e);
        }
        LOG.info("listen on port {}", port);
    }

    @Override
    public void destroy() {
        if (channel != null && channel.isActive()) {
            channel.close().awaitUninterruptibly();
        }
    }
}
