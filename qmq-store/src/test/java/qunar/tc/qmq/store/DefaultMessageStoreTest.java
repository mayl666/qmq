package qunar.tc.qmq.store;

import com.google.common.base.Optional;
import com.google.common.primitives.Longs;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.base.RawMessage;

/**
 * @author keli.wang
 * @since 2017/7/28
 */
public class DefaultMessageStoreTest {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultMessageStoreTest.class);

    @Test
    public void testSaveAndReadMessage() {
        final DefaultMessageStore messageStore = new DefaultMessageStore(new MessageStoreConfig() {
            @Override
            public String getCheckpointStorePath() {
                return null;
            }

            @Override
            public String getMessageLogStorePath() {
                return "/Users/keli/Desktop/store";
            }

            @Override
            public long getMessageLogRetentionMs() {
                return 1000000000;
            }

            @Override
            public String getConsumerLogStorePath() {
                return "/Users/keli/Desktop/consumerlog";
            }

            @Override
            public long getConsumerLogRetentionMs() {
                return 1000000000;
            }

            @Override
            public int getLogRetentionCheckIntervalSeconds() {
                return 60;
            }

            @Override
            public String getPullLogStorePath() {
                return null;
            }

            @Override
            public long getPullLogRetentionMs() {
                return 0;
            }

            @Override
            public String getActionLogStorePath() {
                return null;
            }

            @Override
            public boolean isDeleteExpiredLogsEnable() {
                return false;
            }

            @Override
            public long getLogRetentionMs() {
                return 0;
            }

            @Override
            public int getRetryDelaySeconds() {
                return 0;
            }
        });
        messageStore.start();

        for (long i = 0; i < 20; i++) {
            final byte[] bytes = Longs.toByteArray(i);
            final MessageHeader header = new MessageHeader();
            header.setSubject("demo_subject");
            messageStore.putMessage(new RawMessage(header, bytes.length, Unpooled.copiedBuffer(bytes)));
        }

        final MessageLogVisitor visitor = messageStore.newMessageLogVisitor();
        while (true) {
            final Optional<MessageLogRecord> record = visitor.nextRecord();
            if (!record.isPresent()) {
                break;
            }

            LOG.info("buffer value: {}", record.get().getMessage().getLong());
        }


        long nextBeginOffset = 0;
        while (true) {
            final GetMessageResult result = messageStore.getMessage("demo_subject", nextBeginOffset, 100);
            if (result.getStatus() != GetMessageStatus.SUCCESS) {
                LOG.warn("MESSAGE STATUS: {}", result.getStatus());
                break;
            } else {
                for (final SelectSegmentBufferResult buffer : result.getSegmentBuffers()) {
                    final long value = buffer.getBuffer().getLong();
                    LOG.info("buffer value: {}", value);
                }
                nextBeginOffset = result.getNextBeginOffset();
            }
        }

        LOG.info("DONE with next offset: {}", nextBeginOffset);
        messageStore.destroy();
    }
}