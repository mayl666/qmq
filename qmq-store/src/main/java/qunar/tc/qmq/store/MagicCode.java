package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
final class MagicCode {
    static final int CONSUMER_LOG_MAGIC_V1 = 0xA3B2C100;

    static final int MESSAGE_LOG_MAGIC_V1 = 0xA1B2C300;

    static final int PULL_LOG_MAGIC_V1 = 0xC1B2A300;

    static final int ACTION_LOG_MAGIC_V1 = 0xC3B2A100;
}
