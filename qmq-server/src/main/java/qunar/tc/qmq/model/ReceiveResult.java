package qunar.tc.qmq.model;

/**
 * @author yunfeng.yang
 * @since 2017/8/8
 */
public class ReceiveResult {
    private final String messageId;
    private final int code;
    private final String remark;
    private final long messageLogOffset;

    public ReceiveResult(String messageId, int code, String remark, long messageLogOffset) {
        this.messageId = messageId;
        this.code = code;
        this.remark = remark;
        this.messageLogOffset = messageLogOffset;
    }

    public String getMessageId() {
        return messageId;
    }

    public int getCode() {
        return code;
    }

    public String getRemark() {
        return remark;
    }

    public long getMessageLogOffset() {
        return messageLogOffset;
    }
}
