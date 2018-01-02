package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public enum GetMessageStatus {
    SUCCESS,

    OFFSET_OVERFLOW,

    NO_MESSAGE,

    SUBJECT_NOT_FOUND,

    EMPTY_CONSUMER_LOG
}
