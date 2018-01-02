package qunar.tc.qmq.base;

/**
 * @author yunfeng.yang
 * @since 2017/8/31
 */
public enum BrokerRequestType {
    ONLINE(0), HEARTBEAT(1), OFFLINE(2);

    private int code;

    BrokerRequestType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
