package qunar.tc.qmq.metainfoclient.impl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.consumer.MetaInfoResponse;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;

/**
 * @author yiqun.fan create on 17-8-31.
 */
@ChannelHandler.Sharable
class MetaInfoClientHandler extends SimpleChannelInboundHandler<Datagram> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaInfoClientHandler.class);
    private static final Serializer SERIALIZER = SerializerFactory.create();

    private final ConcurrentSet<MetaInfoClient.ResponseSubscriber> responseSubscribers = new ConcurrentSet<>();

    void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber) {
        responseSubscribers.add(subscriber);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Datagram msg) throws Exception {
        MetaInfoResponse response = null;
        if (msg.getHeader().getCode() == CommandCode.SUCCESS) {
            response = deserialize(msg.getBody());
        }
        for (MetaInfoClient.ResponseSubscriber subscriber : responseSubscribers) {
            try {
                subscriber.onResponse(response);
            } catch (Exception e) {
                LOGGER.error("", e);
            }
        }
    }

    private static MetaInfoResponse deserialize(ByteBuf buf) {
        try {
            final int size = buf.readableBytes();
            if (size > 0) {
                byte[] bs = new byte[size];
                buf.readBytes(bs);
                return SERIALIZER.deSerialize(bs, MetaInfoResponse.class);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return null;
    }
}
