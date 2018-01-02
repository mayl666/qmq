package qunar.tc.qmq.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author keli.wang
 * @since 2017/7/5
 */
public class ConsumerLog {
    public static final int CONSUMER_LOG_UNIT_BYTES = 32; // 4 bytes magic + 8 bytes timestamp + 8 bytes wrote offset + 4 bytes wrote bytes + 8 bytes wrote offset
    private static final int CONSUMER_LOG_SIZE = CONSUMER_LOG_UNIT_BYTES * 10000000; // TODO(keli.wang): to config

    private final MessageStoreConfig config;
    private final LogManager logManager;
    private final MessageAppender<ConsumerLogMessage, Void> consumerLogAppender = new ConsumerLogMessageAppender();
    private final ReentrantLock putMessageLogOffsetLock = new ReentrantLock();

    public ConsumerLog(final MessageStoreConfig config, final String subject) {
        this.config = config;
        this.logManager = new LogManager(new File(config.getConsumerLogStorePath(), subject), CONSUMER_LOG_SIZE, config, new ConsumerLogSegmentValidator());
    }

    // TODO(keli.wang): handle write fail and retry
    public boolean putMessageLogOffset(final long sequence, final long offset, final int size, final long messageOffset) {
        putMessageLogOffsetLock.lock();
        try {
            if (sequence < getMaxOffset()) {
                return true;
            }

            final long expectedOffset = sequence * CONSUMER_LOG_UNIT_BYTES;
            LogSegment segment = logManager.locateSegment(expectedOffset);
            if (segment == null) {
                segment = logManager.allocOrResetSegments(expectedOffset);
                fillPreBlank(segment, expectedOffset);
            }

            final AppendMessageResult result = segment.append(new ConsumerLogMessage(sequence, offset, size, messageOffset), consumerLogAppender);
            switch (result.getStatus()) {
                case SUCCESS:
                    break;
                case END_OF_FILE:
                    logManager.allocNextSegment();
                    return putMessageLogOffset(sequence, offset, size, messageOffset);
                default:
                    return false;
            }
        } finally {
            putMessageLogOffsetLock.unlock();
        }

        return true;
    }

    private void fillPreBlank(final LogSegment segment, final long untilWhere) {
        final ConsumerLogMessage blankMessage = new ConsumerLogMessage(0, 0, Integer.MAX_VALUE, 0);
        final long startOffset = segment.getBaseOffset() + segment.getWrotePosition();
        for (long i = startOffset; i < untilWhere; i += CONSUMER_LOG_UNIT_BYTES) {
            segment.append(blankMessage, consumerLogAppender);
        }
    }

    public SelectSegmentBufferResult selectIndexBuffer(long startIndex) {
        final long startOffset = startIndex * CONSUMER_LOG_UNIT_BYTES;
        final LogSegment segment = logManager.locateSegment(startOffset);
        if (segment == null) {
            return null;
        } else {
            return segment.selectSegmentBuffer((int) (startOffset % CONSUMER_LOG_SIZE));
        }
    }

    public long getMinOffset() {
        return logManager.getMinOffset() / CONSUMER_LOG_UNIT_BYTES;
    }

    public long getMaxOffset() {
        return logManager.getMaxOffset() / CONSUMER_LOG_UNIT_BYTES;
    }

    public void flush() {
        logManager.flush();
    }

    public void close() {
        logManager.close();
    }

    public void clean() {
        logManager.deleteExpiredSegments(config.getConsumerLogRetentionMs());
    }

    private static class ConsumerLogMessage {
        private final long sequence;
        private final long offset;
        private final int size;
        private final long messageOffset;

        private ConsumerLogMessage(long sequence, long offset, int size, long messageOffset) {
            this.sequence = sequence;
            this.offset = offset;
            this.size = size;
            this.messageOffset = messageOffset;
        }

        public long getSequence() {
            return sequence;
        }

        public long getOffset() {
            return offset;
        }

        public int getSize() {
            return size;
        }

        public long getMessageOffset() {
            return messageOffset;
        }
    }

    private static class ConsumerLogMessageAppender implements MessageAppender<ConsumerLogMessage, Void> {
        private final ByteBuffer workingBuffer = ByteBuffer.allocate(CONSUMER_LOG_UNIT_BYTES);

        @Override
        public AppendMessageResult<Void> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, ConsumerLogMessage message) {
            workingBuffer.clear();

            final long wroteOffset = baseOffset + targetBuffer.position();
            workingBuffer.flip();
            workingBuffer.limit(CONSUMER_LOG_UNIT_BYTES);
            workingBuffer.putInt(MagicCode.CONSUMER_LOG_MAGIC_V1);
            workingBuffer.putLong(System.currentTimeMillis());
            workingBuffer.putLong(message.getOffset());
            workingBuffer.putInt(message.getSize());
            workingBuffer.putLong(message.getMessageOffset());
            targetBuffer.put(workingBuffer.array(), 0, CONSUMER_LOG_UNIT_BYTES);
            return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, CONSUMER_LOG_UNIT_BYTES);
        }
    }

    private static class ConsumerLogSegmentValidator implements LogSegmentValidator {
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
            if (magic != MagicCode.CONSUMER_LOG_MAGIC_V1) {
                return -1;
            }

            final long timestamp = buffer.getLong();
            final long offset = buffer.getLong();
            final long size = buffer.getInt();
            final long messageOffset = buffer.getLong();
            if (timestamp > 0 && offset >= 0 && size > 0 && messageOffset > offset) {
                return CONSUMER_LOG_UNIT_BYTES;
            } else {
                return -1;
            }
        }
    }
}
