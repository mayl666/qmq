package qunar.tc.qmq.meta.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BrokerCluster;
import qunar.tc.qmq.base.BrokerGroup;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoResponse;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/9/1
 */
class ClientRegisterWorker implements ActorSystem.Processor<ClientRegisterProcessor.ClientRegisterRequestEntry> {
    private static final Logger logger = LoggerFactory.getLogger(ClientRegisterProcessor.class);
    private static final Serializer SERIALIZER = SerializerFactory.create();

    private final SubjectRouteManager subjectRouteManager;
    private final ActorSystem actorSystem;

    ClientRegisterWorker(SubjectRouteManager subjectRouteManager) {
        this.subjectRouteManager = subjectRouteManager;
        this.actorSystem = new ActorSystem("qmq-meta");
    }

    void register(ClientRegisterProcessor.ClientRegisterRequestEntry message) {
        actorSystem.dispatch("client-register-" + message.getMetaInfoRequest().getSubject(), message, this);
    }

    @Override
    public void process(ClientRegisterProcessor.ClientRegisterRequestEntry message) {
        final MetaInfoRequest metaInfoRequest = message.getMetaInfoRequest();
        final ChannelHandlerContext ctx = message.getCtx();
        final int opaque = message.getOpaque();
        final String realSubject = RetrySubjectUtils.getRealSubject(metaInfoRequest.getSubject());

        final List<BrokerGroup> brokerGroups = subjectRouteManager.route(realSubject, metaInfoRequest.getClientTypeCode());
        logger.info("client register response, metaInfoRequest:{}, realSubject:{}, brokerGroups:{}", metaInfoRequest, realSubject, brokerGroups);

        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, opaque,
                new MetaInfoResponsePayloadHolder(new BrokerCluster(brokerGroups), metaInfoRequest));
        ctx.writeAndFlush(datagram);
    }

    private static class MetaInfoResponsePayloadHolder implements PayloadHolder {
        private final MetaInfoResponse metaInfoResponse;

        MetaInfoResponsePayloadHolder(BrokerCluster brokerCluster, MetaInfoRequest clientRequest) {
            this.metaInfoResponse = new MetaInfoResponse();
            this.metaInfoResponse.setBrokerCluster(brokerCluster);
            this.metaInfoResponse.setClientTypeCode(clientRequest.getClientTypeCode());
            this.metaInfoResponse.setSubject(clientRequest.getSubject());
        }

        @Override
        public void writeBody(ByteBuf out) {
            byte[] bytes = SERIALIZER.serializeToBytes(metaInfoResponse);
            out.writeBytes(bytes);
        }
    }
}
