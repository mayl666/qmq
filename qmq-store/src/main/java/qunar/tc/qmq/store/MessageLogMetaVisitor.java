package qunar.tc.qmq.store;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public class MessageLogMetaVisitor {
    private static final int MIN_RECORD_BYTES = 13;

    private final LogSegment currentSegment;
    private final SelectSegmentBufferResult currentBuffer;
    private final AtomicInteger visitedBufferSize = new AtomicInteger(0);
    private final long startOffset;

    public MessageLogMetaVisitor(final LogManager logManager, final long startOffset) {
        this.currentSegment = logManager.locateSegment(startOffset);
        this.currentBuffer = selectBuffer(startOffset);
        this.startOffset = startOffset;
    }

    public Optional<MessageLogMeta> nextRecord() {
        if (currentBuffer == null) {
            return null;
        }

        return readOneRecord(currentBuffer.getBuffer());
    }

    public int visitedBufferSize() {
        if (currentBuffer == null) {
            return 0;
        }
        return visitedBufferSize.get();
    }

    private SelectSegmentBufferResult selectBuffer(final long startOffset) {
        if (currentSegment == null) {
            return null;
        }
        final int pos = (int) (startOffset % currentSegment.getFileSize());
        return currentSegment.selectSegmentBuffer(pos);
    }

    private Optional<MessageLogMeta> readOneRecord(final ByteBuffer buffer) {
        if (buffer.remaining() < MIN_RECORD_BYTES) {
            return null;
        }

        final int startPos = buffer.position();
        final long wroteOffset = startOffset + buffer.position();
        final int magic = buffer.getInt();
        if (magic != MagicCode.MESSAGE_LOG_MAGIC_V1) {
            visitedBufferSize.set(currentBuffer.getSize());
            return null;
        }

        final byte attributes = buffer.get();
        final long timestamp = buffer.getLong();
        if (attributes == 2) {
            if (buffer.remaining() < Integer.BYTES) {
                return null;
            }
            final int blankSize = buffer.getInt();
            visitedBufferSize.addAndGet(blankSize + (buffer.position() - startPos));
            return Optional.empty();
        } else if (attributes == 1) {
            visitedBufferSize.set(currentBuffer.getSize());
            return null;
        } else {
            if (buffer.remaining() < (Long.BYTES + Integer.BYTES)) {
                return null;
            }
            final long sequence = buffer.getLong();
            final int subjectSize = buffer.getInt();
            if (buffer.remaining() < subjectSize) {
                return null;
            }
            final byte[] subjectBytes = new byte[subjectSize];
            buffer.get(subjectBytes);

            if (buffer.remaining() < Integer.BYTES) {
                return null;
            }
            final int payloadSize = buffer.getInt();
            if (buffer.remaining() < payloadSize) {
                return null;
            }
            final long payloadOffset = startOffset + buffer.position();
            final ByteBuffer message = buffer.slice();
            message.limit(payloadSize);
            buffer.position(buffer.position() + payloadSize);
            final int wroteBytes = buffer.position() - startPos;
            visitedBufferSize.addAndGet(wroteBytes);
            return Optional.of(new MessageLogMeta(new String(subjectBytes, StandardCharsets.UTF_8), sequence, wroteOffset, wroteBytes, payloadOffset));
        }
    }
}
