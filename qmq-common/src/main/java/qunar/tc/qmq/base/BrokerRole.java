package qunar.tc.qmq.base;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public enum BrokerRole {
    MASTER(0), SLAVE(1);

    private final int code;

    BrokerRole(final int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
