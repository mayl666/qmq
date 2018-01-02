package qunar.tc.qmq.store;

import qunar.tc.qmq.configuration.BrokerConstants;
import qunar.tc.qmq.configuration.Config;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2017/7/13
 */
public class MessageStoreConfigImpl implements MessageStoreConfig {
    private static final String CHECKPOINT = "checkpoint";
    private static final String MESSAGE_LOG = "messagelog";
    private static final String CONSUMER_LOG = "consumerlog";
    private static final String PULL_LOG = "pulllog";
    private static final String ACTION_LOG = "actionlog";

    private static final long MS_PER_HOUR = TimeUnit.HOURS.toMillis(1);

    private final Config config;

    public MessageStoreConfigImpl(final Config config) {
        this.config = config;
    }

    @Override
    public String getCheckpointStorePath() {
        return buildStorePath(CHECKPOINT);
    }

    @Override
    public String getMessageLogStorePath() {
        return buildStorePath(MESSAGE_LOG);
    }

    @Override
    public long getMessageLogRetentionMs() {
        final int retentionHours = config.getInt(BrokerConstants.MESSAGE_LOG_RETENTION_HOURS, BrokerConstants.DEFAULT_MESSAGE_LOG_RETENTION_HOURS);
        return retentionHours * MS_PER_HOUR;
    }

    @Override
    public String getConsumerLogStorePath() {
        return buildStorePath(CONSUMER_LOG);
    }

    @Override
    public long getConsumerLogRetentionMs() {
        final int retentionHours = config.getInt(BrokerConstants.CONSUMER_LOG_RETENTION_HOURS, BrokerConstants.DEFAULT_CONSUMER_LOG_RETENTION_HOURS);
        return retentionHours * MS_PER_HOUR;
    }

    @Override
    public int getLogRetentionCheckIntervalSeconds() {
        return config.getInt(BrokerConstants.LOG_RETENTION_CHECK_INTERVAL_SECONDS, BrokerConstants.DEFAULT_LOG_RETENTION_CHECK_INTERVAL_SECONDS);
    }

    @Override
    public String getPullLogStorePath() {
        return buildStorePath(PULL_LOG);
    }

    @Override
    public long getPullLogRetentionMs() {
        final int retentionHours = config.getInt(BrokerConstants.PULL_LOG_RETENTION_HOURS, BrokerConstants.DEFAULT_PULL_LOG_RETENTION_HOURS);
        return retentionHours * MS_PER_HOUR;
    }

    @Override
    public String getActionLogStorePath() {
        return buildStorePath(ACTION_LOG);
    }

    private String buildStorePath(final String name) {
        final String root = config.getString(BrokerConstants.STORE_ROOT, BrokerConstants.LOG_STORE_ROOT);
        return new File(root, name).getAbsolutePath();
    }

    @Override
    public boolean isDeleteExpiredLogsEnable() {
        return config.getBoolean(BrokerConstants.ENABLE_DELETE_EXPIRED_LOGS, false);
    }

    @Override
    public long getLogRetentionMs() {
        final int retentionHours = config.getInt(BrokerConstants.PULL_LOG_RETENTION_HOURS, BrokerConstants.DEFAULT_PULL_LOG_RETENTION_HOURS);
        return retentionHours * MS_PER_HOUR;
    }

    @Override
    public int getRetryDelaySeconds() {
        return config.getInt(BrokerConstants.RETRY_DELAY_SECONDS, BrokerConstants.DEFAULT_RETRY_DELAY_SECONDS);
    }
}
