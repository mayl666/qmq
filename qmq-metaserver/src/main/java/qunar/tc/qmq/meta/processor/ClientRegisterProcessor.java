package qunar.tc.qmq.meta.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.monitor.QMon;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class ClientRegisterProcessor implements NettyRequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ClientRegisterProcessor.class);
    private static final Serializer SERIALIZER = SerializerFactory.create();
    private final ClientRegisterWorker clientRegisterWorker;

    public ClientRegisterProcessor(CachedMetaInfoManager cachedMetaInfoManager, Store store) {
        final SubjectRouteManager subjectRouteManager = new SubjectRouteManager(cachedMetaInfoManager, store);
        this.clientRegisterWorker = new ClientRegisterWorker(subjectRouteManager);
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        final ByteBuf body = request.getBody();
        final MetaInfoRequest metaInfoRequest = deserialize(body);
        QMon.clientRegisterCountInc(metaInfoRequest.getSubject(), metaInfoRequest.getClientTypeCode());
        logger.info("client register request received. request: {}", metaInfoRequest);

        clientRegisterWorker.register(new ClientRegisterRequestEntry(metaInfoRequest, ctx, request.getOpaque()));
    }

    static class ClientRegisterRequestEntry {
        private final MetaInfoRequest metaInfoRequest;
        private final ChannelHandlerContext ctx;
        private final int opaque;

        ClientRegisterRequestEntry(MetaInfoRequest metaInfoRequest, ChannelHandlerContext ctx, int opaque) {
            this.metaInfoRequest = metaInfoRequest;
            this.ctx = ctx;
            this.opaque = opaque;
        }

        MetaInfoRequest getMetaInfoRequest() {
            return metaInfoRequest;
        }

        ChannelHandlerContext getCtx() {
            return ctx;
        }

        int getOpaque() {
            return opaque;
        }
    }

    private MetaInfoRequest deserialize(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return SERIALIZER.deSerialize(bytes, MetaInfoRequest.class);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
