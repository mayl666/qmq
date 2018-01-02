package qunar.tc.qmq.processor;

import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.consumer.ActionLogManager;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yunfeng.yang
 * @since 2017/7/27
 */
class AckWorker implements ActorSystem.Processor<AckMessageProcessor.AckEntry> {

    private final ActorSystem actorSystem;
    private final ActionLogManager actionLogManager;

    AckWorker(final ActorSystem actorSystem,
              final ActionLogManager actionLogManager) {
        this.actorSystem = actorSystem;
        this.actionLogManager = actionLogManager;
    }

    void ack(AckMessageProcessor.AckEntry entry) {
        actorSystem.dispatch("ack-" + entry.getGroup(), entry, this);
    }

    @Override
    public void process(final AckMessageProcessor.AckEntry ackEntry) {
        actionLogManager.putAckActions(ackEntry);
        final Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, ackEntry.getOpaque(), null);
        ackEntry.getCtx().writeAndFlush(response);
    }
}
