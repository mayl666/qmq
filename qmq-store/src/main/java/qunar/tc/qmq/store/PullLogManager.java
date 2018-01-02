package qunar.tc.qmq.store;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.io.File;

/**
 * @author keli.wang
 * @since 2017/8/3
 */
public class PullLogManager {
    private static final String GROUP_INDEX_DELIM = "@";

    private final MessageStoreConfig config;
    private final Table<String, String, PullLog> logs;

    public PullLogManager(final MessageStoreConfig config) {
        this.config = config;
        this.logs = HashBasedTable.create();

        loadPullLogs();
    }

    private void loadPullLogs() {
        final File pullLogsRoot = new File(config.getPullLogStorePath());
        final File[] consumerIdDirs = pullLogsRoot.listFiles();
        if (consumerIdDirs != null) {
            for (final File consumerIdDir : consumerIdDirs) {
                if (!consumerIdDir.isDirectory()) {
                    continue;
                }
                loadPullLogsByConsumerId(consumerIdDir);
            }
        }
    }

    private void loadPullLogsByConsumerId(final File consumerIdDir) {
        final File[] groupIdDirs = consumerIdDir.listFiles();
        if (groupIdDirs != null) {
            for (final File groupIdDir : groupIdDirs) {
                if (!groupIdDir.isDirectory()) {
                    continue;
                }

                final String consumerId = consumerIdDir.getName();
                final String groupId = groupIdDir.getName();
                logs.put(consumerId, groupId, new PullLog(config, consumerId, groupId));
            }
        }
    }

    public PullLog get(final String subject, final String group, final String consumerId) {
        return logs.get(consumerId, buildGroupId(subject, group));
    }

    public PullLog getOrCreate(final String subject, final String group, final String consumerId) {
        final String groupId = buildGroupId(subject, group);
        synchronized (logs) {
            if (!logs.contains(consumerId, groupId)) {
                logs.put(consumerId, groupId, new PullLog(config, consumerId, groupId));
            }

            return logs.get(consumerId, groupId);
        }
    }

    private String buildGroupId(final String subject, final String group) {
        return group + GROUP_INDEX_DELIM + subject;
    }

    public void flush() {
        for (final PullLog log : logs.values()) {
            log.flush();
        }
    }

    public void clean() {
        for (final PullLog log : logs.values()) {
            log.clean();
        }
    }

    public void shutdown() {
        for (final PullLog log : logs.values()) {
            log.close();
        }
    }
}
