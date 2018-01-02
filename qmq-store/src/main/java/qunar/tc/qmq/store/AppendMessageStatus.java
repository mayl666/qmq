package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/7/4
 */
public enum AppendMessageStatus {
    SUCCESS,
    END_OF_FILE,
    MESSAGE_SIZE_EXCEEDED,
    UNKNOWN_ERROR
}
