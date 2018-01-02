package qunar.tc.qmq.sync.master;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.sync.slave.SyncType;
import qunar.tc.qmq.util.RemotingBuilder;

/**
 * @author yunfeng.yang
 * @since 2017/8/20
 */
public class HeartbeatSyncWorker implements SyncRequestProcessor.SyncProcessor {
    private final MessageStoreWrapper messageStoreWrapper;

    HeartbeatSyncWorker(MessageStoreWrapper messageStoreWrapper) {
        this.messageStoreWrapper = messageStoreWrapper;
    }

    @Override
    public void process(SyncRequestProcessor.SyncRequestEntry requestEntry) {
        final ChannelHandlerContext ctx = requestEntry.getCtx();
        final long messageLogMaxOffset = messageStoreWrapper.getMessageLogMaxOffset();
        final long actionLogMaxOffset = messageStoreWrapper.getActionLogMaxOffset();

        QMon.slaveSyncLogOffset(SyncType.message.name(), messageLogMaxOffset - requestEntry.getSyncRequest().getMessageLogOffset());
        QMon.slaveSyncLogOffset(SyncType.action.name(), actionLogMaxOffset - requestEntry.getSyncRequest().getActionLogOffset());

        final HeartBeatPayloadHolder heartBeatPayloadHolder = new HeartBeatPayloadHolder(messageLogMaxOffset, actionLogMaxOffset);
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, requestEntry.getOpaque(), heartBeatPayloadHolder);
        ctx.writeAndFlush(datagram);
    }

    private static class HeartBeatPayloadHolder implements PayloadHolder {
        private final long messageLogMaxOffset;
        private final long actionLogOffset;

        HeartBeatPayloadHolder(long messageLogMaxOffset, long actionLogOffset) {
            this.messageLogMaxOffset = messageLogMaxOffset;
            this.actionLogOffset = actionLogOffset;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeLong(messageLogMaxOffset);
            out.writeLong(actionLogOffset);
        }
    }

    @Override
    public void processTimeout(SyncRequestProcessor.SyncRequestEntry entry) {
        // do nothing
    }
}
