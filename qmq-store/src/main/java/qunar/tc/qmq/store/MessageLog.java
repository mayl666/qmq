package qunar.tc.qmq.store;

import qunar.tc.qmq.base.RawMessage;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author keli.wang
 * @since 2017/7/4
 */
public class MessageLog {
    // TODO(keli.wang): move to config
    private static final int PER_SEGMENT_FILE_SIZE = 1024 * 1024 * 1024;

    private final MessageStoreConfig config;
    private final ConsumerLogManager consumerLogManager;
    private final LogManager logManager;
    private final MessageAppender<RawMessage, MessageSequence> messageAppender = new RawMessageAppender();

    public MessageLog(final MessageStoreConfig config, final ConsumerLogManager consumerLogManager) {
        this.config = config;
        this.consumerLogManager = consumerLogManager;
        this.logManager = new LogManager(new File(config.getMessageLogStorePath()), PER_SEGMENT_FILE_SIZE, config, new MessageLogSegmentValidator());
    }

    private static int calRecordSize(final int subjectSize, final int payloadSize) {
        return 4 // magic code
                + 1 // attributes
                + 8 // timestamp
                + 8 // sequence
                + 4 // subject size
                + (subjectSize > 0 ? subjectSize : 0)
                + 4 // payload size
                + (payloadSize > 0 ? payloadSize : 0);
    }

    public long getMaxOffset() {
        return logManager.getMaxOffset();
    }

    public PutMessageResult putMessage(final RawMessage message) {
        final AppendMessageResult<MessageSequence> result;
        LogSegment segment = logManager.latestSegment();
        if (segment == null) {
            segment = logManager.allocNextSegment();
        }

        if (segment == null) {
            return new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null);
        }

        result = segment.append(message, messageAppender);
        switch (result.getStatus()) {
            case SUCCESS:
                break;
            case END_OF_FILE:
                if (logManager.allocNextSegment() == null) {
                    return new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null);
                }
                return putMessage(message);
            case MESSAGE_SIZE_EXCEEDED:
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
            default:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
        }

        return new PutMessageResult(PutMessageStatus.SUCCESS, result);
    }

    public SelectSegmentBufferResult getMessage(final long wroteOffset, final int wroteBytes, final long payloadOffset) {
        final int payloadSize = wroteBytes - (int) (payloadOffset - wroteOffset);
        final LogSegment segment = logManager.locateSegment(payloadOffset);
        if (segment == null) {
            return null;
        }

        final int pos = (int) (payloadOffset % PER_SEGMENT_FILE_SIZE);
        final SelectSegmentBufferResult result = segment.selectSegmentBuffer(pos, payloadSize);
        result.setWroteOffset(wroteOffset);
        result.setWroteBytes(wroteBytes);
        result.setPayloadOffset(payloadOffset);
        return result;
    }

    public SelectSegmentBufferResult getMessageData(final long offset) {
        final LogSegment segment = logManager.locateSegment(offset);
        if (segment == null) {
            return null;
        }

        final int pos = (int) (offset % PER_SEGMENT_FILE_SIZE);
        return segment.selectSegmentBuffer(pos);
    }

    public boolean appendData(final long startOffset, final ByteBuffer data) {
        LogSegment segment = logManager.locateSegment(startOffset);
        if (segment == null) {
            segment = logManager.allocOrResetSegments(startOffset);
            fillPreBlank(segment, startOffset);
        }

        return segment.appendData(data);
    }

    private void fillPreBlank(LogSegment segment, long untilWhere) {
        final ByteBuffer buffer = ByteBuffer.allocate(17);
        buffer.putInt(MagicCode.MESSAGE_LOG_MAGIC_V1);
        buffer.put((byte) 2);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt((int) (untilWhere % PER_SEGMENT_FILE_SIZE));
        segment.fillPreBlank(buffer, untilWhere);
    }

    public MessageLogVisitor newLogVisitor() {
        return new MessageLogVisitor(logManager);
    }

    public void flush() {
        logManager.flush();
    }

    public void close() {
        logManager.close();
    }

    public void clean() {
        logManager.deleteExpiredSegments(config.getMessageLogRetentionMs());
    }

    public MessageLogMetaVisitor newVisitor(long iterateFrom) {
        return new MessageLogMetaVisitor(logManager, iterateFrom);
    }

    private static class MessageLogSegmentValidator implements LogSegmentValidator {
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
                } else if (result == 0) {
                    return new ValidateResult(ValidateStatus.COMPLETE, fileSize);
                } else {
                    position += result;
                }
            }
        }

        private int consumeAndValidateMessage(final ByteBuffer buffer) {
            final int magic = buffer.getInt();
            if (magic != MagicCode.MESSAGE_LOG_MAGIC_V1) {
                return -1;
            }

            final byte attributes = buffer.get();
            final long timestamp = buffer.getLong();
            if (attributes == 2) {
                return buffer.getInt();
            } else if (attributes == 1) {
                return 0;
            } else if (attributes == 0) {
                final long sequence = buffer.getLong();
                final int subjectSize = buffer.getInt();
                buffer.position(buffer.position() + subjectSize);
                final int payloadSize = buffer.getInt();
                buffer.position(buffer.position() + payloadSize);
                return calRecordSize(subjectSize, payloadSize);
            } else {
                return -1;
            }
        }
    }

    private class RawMessageAppender implements MessageAppender<RawMessage, MessageSequence> {
        private static final int MIN_RECORD_BYTES = 13;
        private static final int MAX_BYTES = 1024 * 1024 * 50; // 50M
        private final ByteBuffer workingBuffer = ByteBuffer.allocate(MAX_BYTES);

        @Override
        public AppendMessageResult<MessageSequence> doAppend(long baseOffset, ByteBuffer targetBuffer, int freeSpace, RawMessage message) {
            workingBuffer.clear();

            final String subject = message.getHeader().getSubject();
            final byte[] subjectBytes = subject.getBytes(StandardCharsets.UTF_8);

            final long wroteOffset = baseOffset + targetBuffer.position();
            final int recordSize = calRecordSize(subjectBytes.length, message.getMessageSize());

            workingBuffer.flip();
            if (recordSize != freeSpace && recordSize + MIN_RECORD_BYTES > freeSpace) {
                workingBuffer.limit(freeSpace);
                workingBuffer.putInt(MagicCode.MESSAGE_LOG_MAGIC_V1);
                workingBuffer.put((byte) 1);
                workingBuffer.putLong(System.currentTimeMillis());
                targetBuffer.put(workingBuffer.array(), 0, freeSpace);
                return new AppendMessageResult<>(AppendMessageStatus.END_OF_FILE, wroteOffset, freeSpace, null);
            } else {
                final long sequence = consumerLogManager.getOffsetOrDefault(subject, 0);

                workingBuffer.limit(recordSize);
                workingBuffer.putInt(MagicCode.MESSAGE_LOG_MAGIC_V1);
                workingBuffer.put((byte) 0);
                workingBuffer.putLong(System.currentTimeMillis());
                workingBuffer.putLong(sequence);
                workingBuffer.putInt(subjectBytes.length);
                workingBuffer.put(subjectBytes);
                workingBuffer.putInt(message.getMessageSize());
                workingBuffer.put(message.getMessageBuf().nioBuffer());
                targetBuffer.put(workingBuffer.array(), 0, recordSize);

                consumerLogManager.incOffset(subject);

                final long payloadOffset = wroteOffset + recordSize - message.getMessageSize();
                return new AppendMessageResult<>(AppendMessageStatus.SUCCESS, wroteOffset, recordSize, new MessageSequence(sequence, payloadOffset));
            }
        }
    }
}
