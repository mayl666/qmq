package qunar.tc.qmq.netty.client;

import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import qunar.api.pojo.node.JacksonSupport;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.util.ChannelUtil;
import qunar.tc.qmq.utils.CharsetUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * @author yiqun.fan create on 17-8-29.
 */
public class HttpClient extends AbstractNettyClient {
    private static final HttpVersion HTTP_VERSION = HttpVersion.HTTP_1_1;
    private static final String HTTP_SCHEME = "http://";
    private static final Joiner.MapJoiner PARAM_JOINER = Joiner.on("&").withKeyValueSeparator("=");

    public static HttpClient newClient() {
        HttpClient client = new HttpClient();
        NettyClientConfig config = new NettyClientConfig();
        config.setClientWorkerThreads(1);
        client.start(config);
        return client;
    }

    private HttpClient() {
        super("qmq-httpclient");
    }

    @Override
    protected ChannelInitializer<SocketChannel> newChannelInitializer(final NettyClientConfig config, final DefaultEventExecutorGroup eventExecutors, final NettyConnectManageHandler connectManager) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(eventExecutors,
                        new HttpClientCodec(),
                        new HttpContentDecompressor(),
                        connectManager,
                        new HttpClientHandler());
            }
        };
    }

    public <V> Future<V> get(String link, Map<String, String> param, HttpResponseCallback<V> callback) {
        List<String> hostAndUrl = parseHostAndUrl(link);
        if (param != null && !param.isEmpty()) {
            hostAndUrl.set(1, hostAndUrl.get(1) + "?" + PARAM_JOINER.join(param));
        }
        final HttpRequest request = buildRequest(hostAndUrl.get(0), hostAndUrl.get(1), HttpMethod.GET, null);
        return request(hostAndUrl.get(0), request, callback);
    }

    public <V> Future<V> post(String link, Object param, HttpResponseCallback<V> callback) {
        List<String> hostAndUrl = parseHostAndUrl(link);
        final HttpRequest request = buildRequest(hostAndUrl.get(0), hostAndUrl.get(1), HttpMethod.POST, param);
        return request(hostAndUrl.get(0), request, callback);
    }

    private static List<String> parseHostAndUrl(String link) {
        List<String> hostAndUrl = new ArrayList<>(2);
        link = link.substring(HttpClient.HTTP_SCHEME.length(), link.length());
        int index = link.indexOf('/');
        String host = link.substring(0, index);
        if (!host.contains(":")) {
            host += ":80";
        }
        hostAndUrl.add(host);
        hostAndUrl.add(link.substring(index, link.length()));
        return hostAndUrl;
    }

    private HttpRequest buildRequest(String remoteAddr, String url, HttpMethod method, Object param) {
        if (!url.startsWith("/")) {
            url = "/" + url;
        }
        HttpRequest request;
        if (param != null) {
            byte[] body = CharsetUtils.toUTF8Bytes(JacksonSupport.toJson(param));
            ByteBuf bodyBuf = Unpooled.buffer(body.length, body.length);
            bodyBuf.writeBytes(body);
            request = new DefaultFullHttpRequest(HTTP_VERSION, method, HTTP_SCHEME + remoteAddr + url, bodyBuf);
            request.headers().set("Content-Type", "application/json");
        } else {
            request = new DefaultFullHttpRequest(HTTP_VERSION, method, HTTP_SCHEME + remoteAddr + url);
        }
        request.headers().set("Host", remoteAddr);
        return request;
    }

    private <V> Future<V> request(String remoteAddr, HttpRequest request, HttpResponseCallback<V> callback) {
        final HttpResponseFuture<V> responseFuture = new HttpResponseFuture<>(callback);

        try {
            final Channel channel = getOrCreateChannel(remoteAddr);
            if (!ChannelUtil.setAttributeIfAbsent(channel, responseFuture)) {
                throw new IllegalStateException("this channel has requested. remoteAddr=" + remoteAddr);
            }
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        responseFuture.onThrowable(new ClientSendException(ClientSendException.SendErrorCode.WRITE_CHANNEL_FAIL));
                    }
                }
            });
        } catch (Exception e) {
            responseFuture.onThrowable(e);
        }
        return responseFuture;
    }
}
