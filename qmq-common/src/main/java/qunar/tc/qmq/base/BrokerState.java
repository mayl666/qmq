package qunar.tc.qmq.base;

/**
 * @author yunfeng.yang
 * @since 2017/8/28
 */
public enum BrokerState {
    RW(1), R(2), W(3), NRW(4);

    private int code;

    BrokerState(int code) {
        this.code = code;
    }

    public static BrokerState codeOf(int brokerState) {
        for (BrokerState value : BrokerState.values()) {
            if (value.getCode() == brokerState) {
                return value;
            }
        }
        return null;
    }

    public int getCode() {
        return code;
    }
}
