package qunar.tc.qmq.netty.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.util.ChannelUtil;
import qunar.tc.qmq.utils.CharsetUtils;

/**
 * @author yiqun.fan create on 17-8-29.
 */
public class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        Object attr = ChannelUtil.getAttribute(ctx.channel());
        if (attr == null || !(attr instanceof HttpResponseFuture)) {
            LOGGER.error("lost attr.");
            return;
        }
        HttpResponseFuture future = (HttpResponseFuture) attr;
        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            future.onStatusAndHeader(response.getStatus(), response.headers());
        } else if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            future.onContent(content.content().toString(CharsetUtils.UTF8));
            if (msg instanceof LastHttpContent) {
                ChannelUtil.removeAttribute(ctx.channel());
                future.onCompleted();
            }
        }
    }
}
