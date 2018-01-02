package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class ActionLogVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(ActionLogVisitor.class);

    private static final int MIN_RECORD_BYTES = 5; // 4 bytes magic + 1 byte record type

    private final LogSegment currentSegment;
    private final SelectSegmentBufferResult currentBuffer;
    private final AtomicInteger visitedBufferSize = new AtomicInteger(0);

    public ActionLogVisitor(final LogManager logManager, final long startOffset) {
        this.currentSegment = logManager.locateSegment(startOffset);
        this.currentBuffer = selectBuffer(startOffset);
    }

    public Optional<Action> nextAction() {
        if (currentBuffer == null) {
            return null;
        }
        return readAction(currentBuffer.getBuffer());
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

    private Optional<Action> readAction(final ByteBuffer buffer) {
        if (buffer.remaining() < MIN_RECORD_BYTES) {
            return null;
        }

        final int startPos = buffer.position();
        final int magic = buffer.getInt();
        if (magic != MagicCode.ACTION_LOG_MAGIC_V1) {
            visitedBufferSize.set(currentBuffer.getSize());
            return null;
        }

        final byte recordType = buffer.get();
        if (recordType == 2) {
            if (buffer.remaining() < Integer.BYTES) {
                return null;
            }
            final int blankSize = buffer.getInt();
            visitedBufferSize.addAndGet(blankSize + (buffer.position() - startPos));
            return Optional.empty();
        } else if (recordType == 1) {
            visitedBufferSize.set(currentBuffer.getSize());
            return null;
        } else if (recordType == 0) {
            try {
                if (buffer.remaining() < Integer.BYTES + Byte.BYTES) {
                    return null;
                }
                final ActionType payloadType = ActionType.fromCode(buffer.get());
                final int payloadSize = buffer.getInt();
                if (buffer.remaining() < payloadSize) {
                    return null;
                }
                final Action action = payloadType.getReaderWriter().read(buffer);
                visitedBufferSize.addAndGet(buffer.position() - startPos);
                return Optional.of(action);
            } catch (Exception e) {
                LOG.error("fail read action log", e);
                return null;
            }
        } else {
            throw new RuntimeException("Unknown record type");
        }
    }
}
