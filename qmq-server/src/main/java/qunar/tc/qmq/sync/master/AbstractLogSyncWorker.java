package qunar.tc.qmq.sync.master;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.util.RemotingBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/8/21
 */
abstract class AbstractLogSyncWorker implements SyncRequestProcessor.SyncProcessor {
    final MessageStoreWrapper messageStoreWrapper;

    private final Config config;

    AbstractLogSyncWorker(MessageStoreWrapper messageStoreWrapper, Config config) {
        this.messageStoreWrapper = messageStoreWrapper;
        this.config = config;
    }

    @Override
    public void process(SyncRequestProcessor.SyncRequestEntry entry) {
        final SyncRequest syncRequest = entry.getSyncRequest();
        final SyncLog logs = getSyncLog(syncRequest);
        if (!logs.isHasData()) {
            final long timeout = config.getLong("message.sync.timeout.ms", 10L);
            SyncRequestProcessor.TIMER.newTimeout(new SyncRequestProcessor.SyncRequestTimeoutTask(entry, this), timeout, TimeUnit.MILLISECONDS);
            return;
        }

        processSyncLog(entry, logs);
    }

    @Override
    public void processTimeout(SyncRequestProcessor.SyncRequestEntry entry) {
        final SyncRequest syncRequest = entry.getSyncRequest();
        final SyncLog syncLog = getSyncLog(syncRequest);

        if (!syncLog.isHasData()) {
            final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, entry.getOpaque(), new SyncLogPayloadHolder(0, syncLog.getStartOffset(), null));
            entry.getCtx().writeAndFlush(datagram);
            return;
        }

        processSyncLog(entry, syncLog);
    }

    private void processSyncLog(SyncRequestProcessor.SyncRequestEntry entry, SyncLog log) {
        final int batchSize = config.getInt("sync.batch.size", 100000);
        final long startOffset = log.getStartOffset();
        final ByteBuffer buffer = log.getBuffer();
        int size = log.getSize();

        if (size > batchSize) {
            buffer.limit(batchSize);
            size = batchSize;
        }
        final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, entry.getOpaque(), new SyncLogPayloadHolder(size, startOffset, buffer));
        entry.getCtx().writeAndFlush(datagram);
    }

    protected abstract SyncLog getSyncLog(SyncRequest syncRequest);

    private static class SyncLogPayloadHolder implements PayloadHolder {
        private final int size;
        private final long startOffset;
        private final ByteBuffer buffer;

        SyncLogPayloadHolder(int size, long startOffset, ByteBuffer buffer) {
            this.size = size;
            this.startOffset = startOffset;
            this.buffer = buffer;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeInt(size);
            out.writeLong(startOffset);
            if (buffer != null) {
                out.writeBytes(buffer);
            }
        }
    }

    class SyncLog {
        private final long startOffset;
        private final ByteBuffer buffer;
        private final boolean hasData;
        private final int size;

        SyncLog(long startOffset, ByteBuffer buffer, boolean hasData, int size) {
            this.startOffset = startOffset;
            this.buffer = buffer;
            this.hasData = hasData;
            this.size = size;
        }

        public int getSize() {
            return size;
        }

        long getStartOffset() {
            return startOffset;
        }

        ByteBuffer getBuffer() {
            return buffer;
        }

        boolean isHasData() {
            return hasData;
        }

        @Override
        public String toString() {
            return "SyncLog{" +
                    "startOffset=" + startOffset +
                    ", buffer=" + buffer +
                    ", hasData=" + hasData +
                    ", size=" + size +
                    '}';
        }
    }
}
