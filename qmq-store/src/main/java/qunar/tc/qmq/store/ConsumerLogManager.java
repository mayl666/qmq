package qunar.tc.qmq.store;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author keli.wang
 * @since 2017/8/19
 */
public class ConsumerLogManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLogManager.class);

    private final MessageStoreConfig config;

    private final ConcurrentMap<String, ConsumerLog> logs;
    private final ConcurrentMap<String, Long> offsets;

    public ConsumerLogManager(final MessageStoreConfig config) {
        this.config = config;
        this.logs = new ConcurrentHashMap<>();
        this.offsets = new ConcurrentHashMap<>();

        loadConsumerLogs();
    }

    private void loadConsumerLogs() {
        LOG.info("Start load consumer logs");

        final File root = new File(config.getConsumerLogStorePath());
        final File[] consumerLogDirs = root.listFiles();
        if (consumerLogDirs != null) {
            for (final File consumerLogDir : consumerLogDirs) {
                if (!consumerLogDir.isDirectory()) {
                    continue;
                }

                final String subject = consumerLogDir.getName();
                final ConsumerLog consumerLog = new ConsumerLog(config, subject);
                logs.put(subject, consumerLog);
                offsets.put(subject, consumerLog.getMaxOffset());
            }
        }

        LOG.info("Load consumer logs done");
    }

    public ConsumerLog getOrCreateConsumerLog(final String subject) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(subject), "message subject cannot be null or empty");
        if (!logs.containsKey(subject)) {
            synchronized (logs) {
                if (!logs.containsKey(subject)) {
                    logs.put(subject, new ConsumerLog(config, subject));
                }
            }
        }

        return logs.get(subject);
    }

    public ConsumerLog getConsumerLog(final String subject) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(subject), "message subject cannot be null or empty");
        return logs.get(subject);
    }

    public long getOffsetOrDefault(final String subject, final long defaultVal) {
        return offsets.getOrDefault(subject, defaultVal);
    }

    public long incOffset(final String subject) {
        return offsets.compute(subject, (key, offset) -> offset == null ? 1 : offset + 1);
    }

    public void flush() {
        for (final ConsumerLog log : logs.values()) {
            log.flush();
        }
    }

    public void clean() {
        for (final ConsumerLog log : logs.values()) {
            log.clean();
        }
    }

    public void shutdown() {
        for (final ConsumerLog log : logs.values()) {
            log.close();
        }
    }
}
