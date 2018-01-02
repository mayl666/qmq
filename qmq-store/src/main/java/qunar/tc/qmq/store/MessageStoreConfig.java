package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public interface MessageStoreConfig {
    String getCheckpointStorePath();

    String getMessageLogStorePath();

    long getMessageLogRetentionMs();

    String getConsumerLogStorePath();

    long getConsumerLogRetentionMs();

    int getLogRetentionCheckIntervalSeconds();

    String getPullLogStorePath();

    long getPullLogRetentionMs();

    String getActionLogStorePath();

    boolean isDeleteExpiredLogsEnable();

    long getLogRetentionMs();

    int getRetryDelaySeconds();
}
