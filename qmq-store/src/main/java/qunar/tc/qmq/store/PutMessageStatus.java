package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/7/5
 */
public enum PutMessageStatus {
    SUCCESS(0),
    CREATE_MAPPED_FILE_FAILED(1),
    MESSAGE_ILLEGAL(2),
    UNKNOWN_ERROR(-1);

    private int code;
    private String desc;

    PutMessageStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}