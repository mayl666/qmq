package qunar.tc.qmq.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.base.BrokerRole;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.consumer.ActionLogManager;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.consumer.AckRequest;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yunfeng.yang
 * @since 2017/7/27
 */
public class AckMessageProcessor implements NettyRequestProcessor {
    private final AckWorker ackWorker;

    public AckMessageProcessor(final ActorSystem actorSystem,
                               final ActionLogManager actionLogManager) {
        this.ackWorker = new AckWorker(actorSystem, actionLogManager);
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, RemotingCommand command) throws Exception {
        final ByteBuf body = command.getBody();
        final AckRequest ackRequest = QMQSerializer.deserializeAckRequest(body);
        final long pullOffsetBegin = ackRequest.getPullOffsetBegin();
        if (pullOffsetBegin < 0) {
            final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, command.getHeader().getOpaque(), null);
            ctx.writeAndFlush(datagram);
            return;
        }

        final int ackSize = (int) (ackRequest.getPullOffsetLast() - pullOffsetBegin + 1);
        QMon.consumerAckCountInc(ackRequest.getSubject(), ackRequest.getGroup(), ackSize);
        ackWorker.ack(new AckEntry(ackRequest, ctx, command.getHeader().getOpaque()));
    }

    public static class AckEntry {
        private final String subject;
        private final String group;
        private final String consumerId;
        private final long firstPullLogOffset;
        private final long lastPullLogOffset;
        private final ChannelHandlerContext ctx;
        private final int opaque;

        AckEntry(AckRequest ackRequest, ChannelHandlerContext ctx, int opaque) {
            this.subject = ackRequest.getSubject();
            this.group = ackRequest.getGroup();
            this.consumerId = ackRequest.getConsumerId();
            this.firstPullLogOffset = ackRequest.getPullOffsetBegin();
            this.lastPullLogOffset = ackRequest.getPullOffsetLast();

            this.ctx = ctx;
            this.opaque = opaque;
        }

        public long getFirstPullLogOffset() {
            return firstPullLogOffset;
        }

        public long getLastPullLogOffset() {
            return lastPullLogOffset;
        }

        public String getSubject() {
            return subject;
        }

        public String getGroup() {
            return group;
        }

        public String getConsumerId() {
            return consumerId;
        }

        ChannelHandlerContext getCtx() {
            return ctx;
        }

        int getOpaque() {
            return opaque;
        }
    }

    @Override
    public boolean rejectRequest() {
        return BrokerConfig.getBrokerRole() == BrokerRole.SLAVE;
    }
}
