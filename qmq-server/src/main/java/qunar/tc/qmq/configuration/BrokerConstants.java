package qunar.tc.qmq.configuration;

/**
 * User: zhaohuiyu Date: 5/13/13 Time: 4:05 PM
 */
public class BrokerConstants {
    public static final String ACTOR_SYSTEM_NAME = "qmq";

    public static final String PORT_CONFIG = "broker.port";
    public static final Integer DEFAULT_PORT = 20881;

    public static final String META_SERVER_ENDPOINT = "meta.server.endpoint";

    public static final String STORE_ROOT = "store.root";
    public static final String LOG_STORE_ROOT = "/home/q/newqmq/";

    public static final String MESSAGE_LOG_RETENTION_HOURS = "messagelog.retention.hours";
    public static final int DEFAULT_MESSAGE_LOG_RETENTION_HOURS = 72; // 3 days

    public static final String CONSUMER_LOG_RETENTION_HOURS = "consumerlog.retention.hours";
    public static final int DEFAULT_CONSUMER_LOG_RETENTION_HOURS = 72; // 3 days

    public static final String RETRY_DELAY_SECONDS = "message.retry.delay.seconds";
    public static final int DEFAULT_RETRY_DELAY_SECONDS = 5;
    public static final String LOG_RETENTION_CHECK_INTERVAL_SECONDS = "log.retention.check.interval.seconds";
    public static final int DEFAULT_LOG_RETENTION_CHECK_INTERVAL_SECONDS = 60;
    public static final String ENABLE_DELETE_EXPIRED_LOGS = "log.expired.delete.enable";

    // slave
    public static final String HEARTBEAT_SLEEP_TIMEOUT_MS = "heartbeat.sleep.timeout.ms";
    public static final long DEFAULT_HEARTBEAT_SLEEP_TIMEOUT_MS = 1000L;

    public static String PULL_LOG_RETENTION_HOURS = "pulllog.retention.hours";
    public static int DEFAULT_PULL_LOG_RETENTION_HOURS = 72; // 3 days

}
