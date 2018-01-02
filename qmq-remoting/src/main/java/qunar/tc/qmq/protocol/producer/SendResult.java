package qunar.tc.qmq.protocol.producer;

/**
 * @author yunfeng.yang
 * @since 2017/7/6
 */
public class SendResult {
    private final int code;
    private final String remark;

    public SendResult(int code, String remark) {
        this.code = code;
        this.remark = remark;
    }

    public int getCode() {
        return code;
    }

    public String getRemark() {
        return remark;
    }
}
