package qunar.tc.qmq.store;

import com.google.common.base.Optional;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/7/25
 */
public class MessageLogVisitor {
    private final LogManager logManager;

    private LogSegment currentSegment;
    private ByteBuffer currentBuffer;

    public MessageLogVisitor(final LogManager logManager) {
        this.logManager = logManager;
        this.currentSegment = logManager.firstSegment();
        this.currentBuffer = currentSegment.sliceByteBuffer();
    }

    public Optional<MessageLogRecord> nextRecord() {
        final MessageLogRecord record = readOneRecord(currentBuffer);
        if (record != null) {
            return Optional.of(record);
        }
        if (rollNextBuffer()) {
            return nextRecord();
        } else {
            return Optional.absent();
        }
    }

    private boolean rollNextBuffer() {
        final long nextBaseOffset = currentSegment.getBaseOffset() + currentSegment.getFileSize();
        if (nextBaseOffset > logManager.latestSegment().getBaseOffset()) {
            return false;
        }

        this.currentSegment = logManager.locateSegment(nextBaseOffset);
        this.currentBuffer = currentSegment.sliceByteBuffer();
        return true;
    }

    private MessageLogRecord readOneRecord(final ByteBuffer buffer) {
        final int magic = buffer.getInt();
        if (magic != MagicCode.MESSAGE_LOG_MAGIC_V1) {
            return null;
        }

        final byte attributes = buffer.get();
        final long timestamp = buffer.getLong();
        if (attributes == 1) {
            return null;
        } else {
            final int subjectSize = buffer.getInt();
            final byte[] subjectBytes = new byte[subjectSize];
            buffer.get(subjectBytes);

            final int payloadSize = buffer.getInt();
            final ByteBuffer message = buffer.slice();
            message.limit(payloadSize);
            buffer.position(buffer.position() + payloadSize);
            return new MessageLogRecord(magic, attributes, timestamp, new String(subjectBytes), message);
        }
    }
}
