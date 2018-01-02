package qunar.tc.qmq.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/8/2
 */
public class PullLog {
    private static final int PULL_LOG_UNIT_BYTES = 12; // 4 bytes magic + 8 bytes message sequence
    private static final int PULL_LOG_SIZE = PULL_LOG_UNIT_BYTES * 10000000; // TODO(keli.wang): to config

    private final MessageStoreConfig config;
    private final LogManager logManager;
    private final MessageAppender<PullLogMessage, MessageSequence> messageAppender = new PullLogMessageAppender();

    public PullLog(final MessageStoreConfig config, final String consumerId, final String groupId) {
        this.config = config;
        this.logManager = new LogManager(buildPullLogPath(consumerId, groupId), PULL_LOG_SIZE, config, new PullLogSegmentValidator());
    }

    private File buildPullLogPath(final String subject, final String group) {
        return new File(new File(config.getPullLogStorePath(), subject), group);
    }

    public synchronized PutMessageResult putPullLogMessage(final PullLogMessage message) {
        return directPutMessage(message);
    }

    public synchronized List<PutMessageResult> putPullLogMessages(final List<PullLogMessage> messages) {
        final List<PutMessageResult> results = new ArrayList<>(messages.size());
        for (final PullLogMessage message : messages) {
            results.add(directPutMessage(message));
        }
        return results;
    }

    private PutMessageResult directPutMessage(final PullLogMessage message) {
        final long expectPhysicalOffset = message.getSequence() * PULL_LOG_UNIT_BYTES;

        LogSegment segment = logManager.locateSegment(expectPhysicalOffset);
        if (segment == null) {
            segment = logManager.allocOrResetSegments(expectPhysicalOffset);
            fillPreBlank(segment, expectPhysicalOffset);
        }

        final AppendMessageResult<MessageSequence> result = segment.append(message, messageAppender);
        switch (result.getStatus()) {
            case SUCCESS:
                break;
            case END_OF_FILE:
                logManager.allocNextSegment();
                return putPullLogMessage(message);
            default:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
        }

        return new PutMessageResult(PutMessageStatus.SUCCESS, result);
    }

    private void fillPreBlank(final LogSegment segment, final long untilWhere) {
        final PullLogMessage blankMessage = new PullLogMessage(0, -1);
        for (long i = segment.getBaseOffset(); i < untilWhere; i += PULL_LOG_UNIT_BYTES) {
            segment.append(blankMessage, messageAppender);
        }
    }

    public SelectSegmentBufferResult selectIndexBuffer(final long startIndex) {
        final long startOffset = startIndex * PULL_LOG_UNIT_BYTES;
        final LogSegment segment = logManager.locateSegment(startOffset);
        if (segment == null) {
            return null;
        } else {
            return segment.selectSegmentBuffer((int) (startOffset % PULL_LOG_SIZE));
        }
    }

    public long getMessageSequence(long pullLogSequence) {
        final SelectSegmentBufferResult result = selectIndexBuffer(pullLogSequence);
        if (result == null) {
            return -1;
        }

        final ByteBuffer buffer = result.getBuffer();
        buffer.getInt();
        return buffer.getLong();
    }

    public long getMinOffset() {
        return logManager.getMinOffset() / PULL_LOG_UNIT_BYTES;
    }

    public long getMaxOffset() {
        return logManager.getMaxOffset() / PULL_LOG_UNIT_BYTES;
    }

    public void flush() {
        logManager.flush();
    }

    public void close() {
        logManager.close();
    }

    public void clean() {
        logManager.deleteExpiredSegments(config.getPullLogRetentionMs());
    }


    private static class PullLogMessageAppender implements MessageAppender<PullLogMessage, MessageSequence> {
        private final ByteBuffer workingBuffer = ByteBuffer.allocate(PULL_LOG_UNIT_BYTES);

        @Override
        public AppendMessageResult<MessageSequence> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, PullLogMessage message) {
            workingBuffer.clear();

            final long wroteOffset = baseOffset + targetBuffer.position();
            workingBuffer.flip();
            workingBuffer.limit(PULL_LOG_UNIT_BYTES);
            workingBuffer.putInt(MagicCode.PULL_LOG_MAGIC_V1);
            workingBuffer.putLong(message.getMessageSequence());
            targetBuffer.put(workingBuffer.array(), 0, PULL_LOG_UNIT_BYTES);

            final long messageIndex = wroteOffset / PULL_LOG_UNIT_BYTES;
            return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, PULL_LOG_UNIT_BYTES, new MessageSequence(messageIndex, wroteOffset));
        }
    }

    private static class PullLogSegmentValidator implements LogSegmentValidator {
        @Override
        public ValidateResult validate(LogSegment segment) {
            final int fileSize = segment.getFileSize();
            final ByteBuffer buffer = segment.sliceByteBuffer();

            int position = 0;
            while (true) {
                if (position == fileSize) {
                    return new ValidateResult(ValidateStatus.COMPLETE, fileSize);
                }

                final int result = consumeAndValidateMessage(buffer);
                if (result == -1) {
                    return new ValidateResult(ValidateStatus.PARTIAL, position);
                } else {
                    position += result;
                }
            }
        }

        private int consumeAndValidateMessage(final ByteBuffer buffer) {
            final int magic = buffer.getInt();
            if (magic != MagicCode.PULL_LOG_MAGIC_V1) {
                return -1;
            }

            buffer.getLong();
            return PULL_LOG_UNIT_BYTES;
        }
    }
}
